-- A live order board: stores, who may see each store, customers and orders.
-- Realtime needs nothing extra from the schema except a stable replica identity,
-- which a primary key gives every table here.

create table if not exists stores (
  id   integer primary key,
  name text not null
);

-- The authorization table. A live query that joins through it stays scoped to
-- the members of a store, and removing a member removes their rows live.
create table if not exists store_members (
  store_id integer not null references stores (id) on delete cascade,
  user_id  text    not null,
  primary key (store_id, user_id)
);

create table if not exists customers (
  id       integer primary key,
  store_id integer not null references stores (id) on delete cascade,
  name     text    not null
);

create table if not exists orders (
  id          integer generated always as identity primary key,
  store_id    integer not null references stores (id) on delete cascade,
  customer_id integer not null references customers (id) on delete cascade,
  status      text    not null default 'new' check (status in ('new', 'preparing', 'ready', 'done')),
  total_cents integer not null check (total_cents >= 0),
  -- the measurement scripts number their writes here; board orders leave it null
  seq         integer,
  created_at  timestamptz not null default now()
);

create index if not exists orders_store_id_idx on orders (store_id);
create index if not exists customers_store_id_idx on customers (store_id);
