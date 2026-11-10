-- SPDX-License-Identifier: MIT
-- Sent-email audit store (ADM-2). One row per handled event, keyed by the
-- envelope eventId so a redelivery never sends the same email twice. This is an
-- audit/send queue written by the sending path, not a Kafka outbox.
create table sent_email (
    id uuid primary key,
    source_event_id text not null unique,
    event_type text not null,
    reason text,
    recipient_type text not null
        check (recipient_type in ('customer', 'email', 'order')),
    recipient_key text not null,
    customer_id text,
    order_id text,
    subject text not null,
    body text not null,
    status text not null
        check (status in ('pending', 'sent', 'failed')),
    created_at timestamptz not null default now(),
    sent_at timestamptz,
    failed_at timestamptz,
    error text
);

create index sent_email_customer_id_created_at_idx
    on sent_email (customer_id, created_at desc);

create index sent_email_order_id_created_at_idx
    on sent_email (order_id, created_at desc);
