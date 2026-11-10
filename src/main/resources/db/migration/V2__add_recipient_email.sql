-- SPDX-License-Identifier: MIT
-- Recipient resolution (ADM-3/ADR 0025). recipient_type/recipient_key keep
-- recording what the event identified; recipient_email records the resolved
-- destination address. Rows whose key was already an address are backfilled;
-- customer/order-keyed historical rows cannot be resolved without HTTP calls.
alter table sent_email
    add column recipient_email text;

update sent_email
set recipient_email = recipient_key
where recipient_type = 'email';

-- Terminal unresolved recipients (no address exists) must not be retried.
alter table sent_email
    drop constraint sent_email_status_check;

alter table sent_email
    add constraint sent_email_status_check
        check (status in ('pending', 'sent', 'failed', 'unresolved'));
