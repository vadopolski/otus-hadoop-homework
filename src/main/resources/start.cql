create keyspace public with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};

create table public.click("userId" bigint primary key, "timestamp" text, "page" text, "duration" int);

drop table public.click;

create table public.click(
    "userId" bigint,
    "timestamp" text,
    "page" text,
    "duration" int,
    primary key ("userId", "timestamp"));

select * from public.click;

