-- 0001_init.sql — schema inicial do conector evolution-chatwoot.
-- Database alvo: evogo_chatwoot (ver connector/CONTRACTS.md, seção 6).

-- chatwoot_configs: configuração da integração por instância do evolutiongo.
CREATE TABLE IF NOT EXISTS chatwoot_configs (
    instance_id                TEXT PRIMARY KEY,
    instance_name               TEXT NOT NULL DEFAULT '',
    enabled                      BOOLEAN NOT NULL DEFAULT FALSE,
    url                          TEXT NOT NULL DEFAULT '',
    account_id                   TEXT NOT NULL DEFAULT '',
    token                        TEXT NOT NULL DEFAULT '',
    name_inbox                   TEXT NOT NULL DEFAULT '',
    sign_msg                     BOOLEAN NOT NULL DEFAULT FALSE,
    sign_delimiter               TEXT NOT NULL DEFAULT '',
    number                       TEXT NOT NULL DEFAULT '',
    reopen_conversation          BOOLEAN NOT NULL DEFAULT FALSE,
    conversation_pending         BOOLEAN NOT NULL DEFAULT FALSE,
    merge_brazil_contacts        BOOLEAN NOT NULL DEFAULT FALSE,
    import_contacts              BOOLEAN NOT NULL DEFAULT FALSE,
    import_messages               BOOLEAN NOT NULL DEFAULT FALSE,
    days_limit_import_messages   INTEGER NOT NULL DEFAULT 0,
    auto_create                  BOOLEAN NOT NULL DEFAULT FALSE,
    organization                 TEXT NOT NULL DEFAULT '',
    logo                         TEXT NOT NULL DEFAULT '',
    ignore_jids                  JSONB NOT NULL DEFAULT '[]'::jsonb,
    inbox_id                     INTEGER NOT NULL DEFAULT 0,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- contact_mappings: JID do WhatsApp <-> contato do Chatwoot.
CREATE TABLE IF NOT EXISTS contact_mappings (
    instance_id           TEXT NOT NULL,
    remote_jid            TEXT NOT NULL,
    chatwoot_contact_id   INTEGER NOT NULL,
    identifier            TEXT NOT NULL DEFAULT '',
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (instance_id, remote_jid)
);

-- conversation_mappings: chat do WhatsApp <-> conversa do Chatwoot.
CREATE TABLE IF NOT EXISTS conversation_mappings (
    instance_id                  TEXT NOT NULL,
    remote_jid                   TEXT NOT NULL,
    chatwoot_conversation_id     INTEGER NOT NULL,
    inbox_id                     INTEGER NOT NULL DEFAULT 0,
    status                       TEXT NOT NULL DEFAULT 'open',
    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (instance_id, remote_jid)
);

-- índice único para localizar o mapeamento a partir do lado Chatwoot
-- (ex.: webhook message_created chega apenas com conversation_id do Chatwoot).
CREATE UNIQUE INDEX IF NOT EXISTS conversation_mappings_instance_cw_conv_idx
    ON conversation_mappings (instance_id, chatwoot_conversation_id);

-- message_mappings: deduplicação e correlação de mensagens nos dois sentidos.
CREATE TABLE IF NOT EXISTS message_mappings (
    instance_id            TEXT NOT NULL,
    whatsapp_message_id    TEXT NOT NULL,
    chatwoot_message_id    INTEGER NOT NULL,
    direction               TEXT NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (instance_id, whatsapp_message_id)
);

-- índice para localizar a mensagem a partir do lado Chatwoot
-- (ex.: webhook message_created/message_updated chega com id da mensagem do Chatwoot).
CREATE INDEX IF NOT EXISTS message_mappings_instance_cw_msg_idx
    ON message_mappings (instance_id, chatwoot_message_id);

-- índice para expurgo futuro por data de criação (retention job).
CREATE INDEX IF NOT EXISTS message_mappings_created_at_idx
    ON message_mappings (created_at);
