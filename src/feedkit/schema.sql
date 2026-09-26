create table if not exists runs (
    run_id       bigserial primary key,
    source       text not null,
    run_date     date not null,
    started_at   timestamptz not null default now(),
    finished_at  timestamptz,
    status       text not null default 'running',
    rows_seen    int not null default 0,
    rows_new     int not null default 0,
    rows_changed int not null default 0,
    blocked      int not null default 0,
    error        text
);

create table if not exists records (
    source       text not null,
    external_id  text not null,
    data         jsonb not null,
    content_hash text not null,
    first_seen   timestamptz not null,
    last_seen    timestamptz not null,
    last_run_id  bigint not null references runs,
    primary key (source, external_id)
);

create table if not exists record_versions (
    source       text not null,
    external_id  text not null,
    data         jsonb not null,
    content_hash text not null,
    valid_from   timestamptz not null,
    valid_to     timestamptz,
    run_id       bigint not null references runs,
    primary key (source, external_id, valid_from)
);

create unique index if not exists record_versions_current
    on record_versions (source, external_id) where valid_to is null;
