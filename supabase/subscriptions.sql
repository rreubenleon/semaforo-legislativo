-- ─────────────────────────────────────────────────────────────────────────
-- FIAT — Suscripciones (Stripe + Supabase)
-- Fuente de verdad del estado de pago. Corre esto UNA vez en
-- Supabase Studio → SQL Editor (proyecto cvkmqjybsqowxivczmwx).
--
-- Modelo: freemium. status ∈ {none, active, trialing, past_due, canceled,
-- incomplete, unpaid}. El frontend considera "Pro" si status ∈ {active,trialing}.
-- Solo el Worker (service_role, vía webhook de Stripe) escribe aquí.
-- El usuario solo puede LEER su propia fila (RLS).
-- ─────────────────────────────────────────────────────────────────────────

create table if not exists public.subscriptions (
  user_id                uuid primary key references auth.users(id) on delete cascade,
  stripe_customer_id     text unique,
  stripe_subscription_id text,
  status                 text not null default 'none',
  price_id               text,
  plan                   text,                       -- 'mensual' | 'anual'
  current_period_end     timestamptz,
  cancel_at_period_end   boolean not null default false,
  updated_at             timestamptz not null default now()
);

create index if not exists subscriptions_customer_idx
  on public.subscriptions (stripe_customer_id);

alter table public.subscriptions enable row level security;

-- El usuario autenticado puede leer SOLO su propia suscripción.
drop policy if exists "read own subscription" on public.subscriptions;
create policy "read own subscription"
  on public.subscriptions
  for select
  using (auth.uid() = user_id);

-- NO hay políticas de insert/update/delete para usuarios → quedan denegadas.
-- El Worker escribe con la service_role key, que ignora RLS por diseño.

-- Helper opcional para gatear desde SQL/otros lugares si hace falta.
create or replace function public.is_pro(uid uuid)
returns boolean
language sql
stable
as $$
  select exists (
    select 1 from public.subscriptions s
    where s.user_id = uid
      and s.status in ('active', 'trialing')
      and (s.current_period_end is null or s.current_period_end > now())
  );
$$;
