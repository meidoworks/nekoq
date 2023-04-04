/*
 * NOTE: PLEASE change the following items before running in production environment:
 *       1. schema
 *       2. db owner
 */
create table if not exists public.cellar_data_change
(
    change_id    bigserial
        constraint cellar_data_change_pk
            primary key,
    area         varchar(100) not null,
    data_key     varchar(500) not null,
    data_version bigint       not null,
    operation    integer      not null,
    time_created bigint       not null
);

alter table public.cellar_data_change
    owner to admin;

create table if not exists public.cellar_data
(
    data_id      bigserial
        constraint cellar_data_pk
            primary key,
    area         varchar(100)      not null,
    data_key     varchar(500)      not null,
    data_version bigint            not null,
    data_content text,
    group_key    varchar(100)      not null,
    data_status  integer default 0 not null,
    time_created bigint            not null,
    time_updated bigint            not null,
    constraint cellar_data_datakey_uk
        unique (area, data_key)
);

alter table public.cellar_data
    owner to admin;

create index cellar_data_group_key_index
    on public.cellar_data (group_key);

/* Initialize default area data in the database */
insert into cellar_data (area, data_key, data_version, data_content, group_key, time_created,
                         time_updated)
values ('top', 'nekoq.area_levels', 1,
        '{"top":{"parent":"","attribute":{"decription":"top level area"}},"default":{"parent":"top","attribute":{"description":"default area"}}}',
        '$sys', (extract(epoch from now()) * 1000)::bigint,
        (extract(epoch from now()) * 1000)::bigint);

insert into cellar_data_change (area, data_key, data_version, operation, time_created)
values ('top', 'nekoq.area_levels', 1, 1, (extract(epoch from now()) * 1000)::bigint)
