create schema if not exists {{ analytics_schema }};

-- create artists_listened_monthly table
drop table if exists {{ analytics_schema }}.artists_listened_monthly;

create table if not exists {{ analytics_schema }}.artists_listened_monthly (
    primary key(year_played, month_played, artist_id),
    year_played int not null,
    month_played int not null,
    artist_id VARCHAR not null references {{ prod_schema }}.artist(id),
    sec_listened int not null
);
