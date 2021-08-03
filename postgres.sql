-- Transformar a tabela de vendas particionada por ano.
-- Lembre-se de verificar todos os anos possíveis para criar as partições de forma correta.

drop table sale_read;

create table public.sale_read
(
    id          integer      not null,
    id_customer integer      not null,
    id_branch   integer      not null,
    id_employee integer      not null,
    date        timestamp(6) not null,
    created_at  timestamp    not null,
    modified_at timestamp    not null,
    active      boolean      not null
) partition by range (date);

do
$$
    declare
        ano     integer;
        comando varchar;
    begin
        for ano in 1970..2021
            loop
                comando := format('create table sale_read_%s partition of sale_read for values from (%s) to (%s);',
                                  ano,
                                  quote_literal(concat(ano::varchar, '-01-01 00:00:00.000000')),
                                  quote_literal(concat(ano::varchar, '-12-31 23:59:59.999999'))
                    );
                execute comando;
            end loop;
    end;
$$;

create or replace function fn_popular_sale_read() returns trigger as
$$
begin
    insert into sale_read(id, id_customer, id_branch, id_employee, date, created_at, modified_at, active)
    values (new.id, new.id_customer, new.id_branch, new.id_employee, new.date, new.created_at, new.modified_at,
            new.active);
    return new;
end;
$$
    language plpgsql;

create trigger tg_popular_sale_read_update
    after update
    on sale
    for each row
execute function fn_popular_sale_read();

do
$$
    declare
        consulta record;
    begin
        for consulta in select * from sale
            loop
                update sale set id_customer = id_customer where id = consulta.id;
            end loop;
    end;
$$;


-- -------------------------------------------------------------------------------------------------------
-- Crie um PIVOT TABLE para saber o total vendido por grupo de produto
-- por mês referente a um determinado ano.

select *
from crosstab(
             'select pg.name, date_part(''month'', s.date) as date, sum(si.quantity * p.sale_price) as total from sale s
                  inner join sale_item si on s.id = si.id_sale
                  inner join product p on p.id = si.id_product
                  inner join product_group pg on pg.id = p.id_product_group
              where date_part(''year'', s.date) = 2020
              group by 1, 2',
             'select m from generate_series(1,12) m'
         ) as (
               ano varchar, Janeiro numeric, Fevereiro numeric, Marco numeric, Abril numeric, Maio numeric,
               Junho numeric,
               Julho numeric, Agosto numeric, Setembro numeric, Outubro numeric, Novembro numeric, Dezembro numeric
    );

-- ------------------------------------------------------------------------------------------------------
-- Crie um PIVOT TABLE para saber o total de clientes por bairro e zona.

select bairro, coalesce(sul, 0), coalesce(oeste, 0), coalesce(norte, 0), coalesce(leste, 0)
from crosstab(
             'select d.name, z.name, count(*)
from customer c
         inner join district d on d.id = c.id_district
         inner join zone z on z.id = d.id_zone
group by 1, 2
order by z.name desc',
             'select z.name from zone z order by z.name desc') as (
                                                                   bairro varchar, sul integer, oeste integer,
                                                                   norte integer, leste integer
    )
order by bairro;


-- ------------------------------------------------------------------------------------------------------
-- Crie uma coluna para saber o preço unitário do item de venda,
-- crie um script para atualizar os dados já existentes e logo em
-- seguida uma trigger para preencher o campo.

alter table sale_item
    add column unit_price numeric(16, 3);

select si.*, sum(si.quantity * p.sale_price) as total
from sale_item si
         inner join product p on p.id = si.id_product
group by 1;

do
$$
    declare
        consulta record;
    begin
        for consulta in (select si.*, sum(si.quantity * p.sale_price) as total
                         from sale_item si
                                  inner join product p on p.id = si.id_product
                         group by 1
        )
            loop
                update sale_item set unit_price = consulta.total where id = consulta.id;
            end loop;
    end;
$$;

create or replace function fn_popular_unit_price() returns trigger as
$$
declare
    consulta record;
begin
    for consulta in (select si.id, sum(si.quantity * p.sale_price) as total
                     from sale_item si
                              inner join product p on p.id = si.id_product
                     group by 1
                     order by 1 desc
                     limit 1
    )
        loop
            update sale_item set unit_price = consulta.total WHERE id = consulta.id;
        end loop;
    return new;
end;
$$
    language plpgsql;

create trigger tg_popular_unit_price
    after insert
    on sale_item
    for each row
execute function fn_popular_unit_price();

insert into sale_item (id_sale, id_product, quantity)
values (1, 1, 1000);

select sum(cost_price * 1000)
from product
where id = 1;

select *
from sale_item
order by 1 desc
limit 1;

-- ------------------------------------------------------------------------------------------------------
-- Crie um campo para saber o total da venda, crie um script para atualizar os dados já existentes,
-- em seguida uma trigger para preencher o campo de forma automática.

alter table sale
    add column total numeric(16, 3);

select s.*, sum(si.quantity * p.sale_price) as total
from sale s
         inner join sale_item si on s.id = si.id_sale
         inner join product p on p.id = si.id_product
group by 1
order by 1;

do
$$
    declare
        consulta record;
    begin
        for consulta in (select s.id, sum(si.quantity * p.sale_price) as total
                         from sale s
                                  inner join sale_item si on s.id = si.id_sale
                                  inner join product p on p.id = si.id_product
                         group by 1
                         order by 1
        )
            loop
                update sale set total = consulta.total where id = consulta.id;
            end loop;
    end;
$$;

create or replace function fn_popular_total_sale() returns trigger as
$$
declare
    consulta record;
begin
    for consulta in (select s.id, sum(si.quantity * p.sale_price) as total
                     from sale s
                              inner join sale_item si on s.id = si.id_sale
                              inner join product p on p.id = si.id_product
                     group by 1
                     order by 1 desc
                     limit 1
    )
        loop
            update sale set total = consulta.total where id = consulta.id;
        end loop;
    return new;
end
$$
    language plpgsql;

create trigger tg_popular_total_sale_insert
    after insert
    on sale_item
    for each row
execute function fn_popular_total_sale();

create trigger tg_popular_total_sale_update
    after update
    on sale
    for each row
execute function fn_popular_total_sale();

insert into sale (id_customer, id_branch, id_employee, date)
values (1, 1, 1, '2021-01-01');

insert into sale_item (id_sale, id_product, quantity)
values (10012, 4, 1000);

select s.id, s.total
from sale s
         inner join sale_item si on s.id = si.id_sale
         inner join product p on p.id = si.id_product
group by 1
order by 1 desc
limit 1;

-- ------------------------------------------------------------------------------------------------------
-- Baseado no banco de dados de crime vamos fazer algumas questões.
-- 1 - Criar o banco de dados.

create database crime;

-- 2 - Criar o DDL para estrutura das tabelas.

create table arma
(
    id            serial       not null,
    numero_serie  varchar(104),
    descricao     varchar(256) not null,
    tipo          varchar(1)   not null,
    ativo         boolean      not null default true,
    criado_em     timestamp(6) not null default now(),
    modificado_em timestamp(6),
    constraint pk_arma primary key (id)
);

create table tipo_crime
(
    id                  serial       not null,
    nome                varchar(104) not null,
    tempo_minimo_prisao smallint,
    tempo_maximo_prisao smallint,
    tempo_prescricao    smallint,
    ativo               boolean      not null default true,
    criado_em           timestamp(6) not null default now(),
    modificado_em       timestamp(6),
    constraint pk_tipo_crime primary key (id)
);
create unique index ak_tipo_crime_nome on tipo_crime (nome);

create table crime
(
    id            serial       not null,
    id_tipo_crime integer      not null,
    data          timestamp(6) not null,
    local         varchar(256) not null,
    observacao    text,
    ativo         boolean      not null default true,
    criado_em     timestamp(6) not null default now(),
    modificado_em timestamp(6),
    constraint pk_crime primary key (id),
    constraint fk_crime_tipo_crime foreign key (id_tipo_crime) references tipo_crime (id)
);

create table crime_arma
(
    id            serial       not null,
    id_crime      integer      not null,
    id_arma       integer      not null,
    ativo         boolean      not null default true,
    criado_em     timestamp(6) not null default now(),
    modificado_em timestamp(6),
    constraint pk_crime_arma primary key (id),
    constraint fk_crime_arma_crime foreign key (id_crime) references crime (id),
    constraint fk_crime_arma_arma foreign key (id_arma) references arma (id)
);
create unique index ak_crime_arma on crime_arma (id_arma, id_crime);

create table pessoa
(
    id              serial       not null,
    nome            varchar(104) not null,
    cpf             varchar(11)  not null,
    telefone        varchar(11)  not null,
    data_nascimento date         not null,
    endereco        varchar(256) not null,
    ativo           boolean      not null default true,
    criado_em       timestamp(6) not null default now(),
    modificado_em   timestamp(6),
    constraint pk_pessoa primary key (id)
);
create unique index ak_pessoa_cpf on pessoa (cpf);

create table crime_pessoa
(
    id            serial       not null,
    id_crime      integer      not null,
    id_pessoa     integer      not null,
    tipo          varchar(1)   not null,
    ativo         boolean      not null default true,
    criado_em     timestamp(6) not null default now(),
    modificado_em timestamp(6),
    constraint pk_crime_pessoa primary key (id),
    constraint fk_crime_pessoa_crime foreign key (id_crime) references crime (id),
    constraint fk_crime_pessoa_pessoa foreign key (id_pessoa) references pessoa (id)
);
create unique index ak_pessoa_crime on crime_pessoa (id_pessoa, id_crime);

-- 3 - Criar um script para criar armas de forma automática, seguindo os seguintes critérios:
-- O número de série da arma deve ser gerado por o UUID, os tipos de armas são:
-- 0 - Arma de fogo
-- 1 - Arma branca
-- 2 - Outros.
-- Algumas armas de fogo - Pistola, Metralhadora, Escopeta.
-- Algumas armas brancas - Faca, Facão, estilete.
-- Outros - Corda, Garrafa.
-- A ideia é que você use essas tipos de armas para de forma aleatória gere as armas os INSERTS.

create extension if not exists "uuid-ossp";

create or replace function fn_gera_arma(tipo integer) returns varchar as
$$
declare
    arma_fogo   varchar[];
    arma_branca varchar[];
    outros      varchar[];
    descricao   varchar;
    escolha     integer;
begin
    arma_fogo := '{Pistola, Metralhadora, Escopeta}';
    arma_branca := '{Faca, Facão, Estilete}';
    outros := '{Corda, Garrafa, Pedra}';
    escolha := (SELECT 1 + round(CAST(random() * (3 - 1) AS NUMERIC), 0))::integer;
    case
        when tipo = 0 then descricao := arma_fogo[escolha];
        when tipo = 1 then descricao := arma_branca[escola];
        else descricao := outros[escolha];
        end case;
    return descricao;
end;
$$
    language plpgsql;

do
$$
    declare
        tipo      integer;
        descrisao varchar;
    begin
        tipo := round(CAST(random() * 3 AS NUMERIC), 0);
        descrisao := fn_gera_arma(tipo);
        insert into arma (numero_serie, descricao, tipo)
        values (uuid_generate_v4(), descrisao, tipo);
    end;
$$
language plpgsql;


-- Faça um script para migrar todos os clientes e funcionários da base de vendas como pessoas na
-- base de dados de crimes. Os campos que por ventura não existirem, coloque-os como nulo ou gere
-- de forma aleatória.

create extension dblink;

do
$$
    declare
        consulta1 record;
        consulta2 record;
    begin
        for consulta1 in (
            (select *
             from dblink('dbname=sale port=5432 host=127.0.0.1 user=postgres password=123456',
                         'select * from customer',
                         true) as (id integer,
                                   id_district integer,
                                   id_marital_status integer,
                                   name varchar,
                                   income numeric,
                                   gender varchar,
                                   created_at timestamp,
                                   modified_at timestamp,
                                   active boolean
                 )))
            loop
                insert into pessoa (nome, cpf, telefone, data_nascimento, endereco)
                values (consulta1.name, (SELECT 11111111111 + round(CAST(random() * 88888888888 AS NUMERIC), 0)),
                        (SELECT 11111111111 + round(CAST(random() * 88888888888 AS NUMERIC), 0)),
                        (SELECT concat((SELECT 1900 + round(CAST(random() * (2100 - 2000) AS NUMERIC), 0)::integer),
                                       '/',
                                       (SELECT 1 + round(CAST(random() * (12 - 1) AS NUMERIC), 0)::integer), '/',
                                       (SELECT 1 + round(CAST(random() * (27 - 1) AS NUMERIC), 0)::integer),
                                       ' 00:00:00.000000'))::date,
                        uuid_generate_v4()::varchar);
            end loop;
        for consulta2 in (
            select *
            from dblink('dbname=sale port=5432 host=127.0.0.1 user=postgres password=123456',
                        'select * from employee',
                        true) as (id integer,
                                  id_department integer,
                                  id_district integer,
                                  id_marital_status integer,
                                  name varchar,
                                  salary numeric,
                                  admission_date date,
                                  birth_date date,
                                  gender varchar,
                                  created_at timestamp,
                                  modified_at timestamp,
                                  active boolean
                ))
            loop
                insert into pessoa (nome, cpf, telefone, data_nascimento, endereco)
                values (consulta2.name, (SELECT 11111111111 + round(CAST(random() * 88888888888 AS NUMERIC), 0)),
                        (SELECT 11111111111 + round(CAST(random() * 88888888888 AS NUMERIC), 0)),
                        (SELECT concat((SELECT 1900 + round(CAST(random() * (2100 - 2000) AS NUMERIC), 0)::integer),
                                       '/',
                                       (SELECT 1 + round(CAST(random() * (12 - 1) AS NUMERIC), 0)::integer), '/',
                                       (SELECT 1 + round(CAST(random() * (27 - 1) AS NUMERIC), 0)::integer),
                                       ' 00:00:00.000000'))::date,
                        uuid_generate_v4()::varchar);
            end loop;
    end;
$$
language plpgsql;
