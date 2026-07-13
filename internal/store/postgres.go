// INTERFACE-CHANGE-REQUEST atendido: GetConversationByChatwootID e ListConfigs
// foram incorporados à interface Store (store.go) pelo orquestrador.
//
// Package store — implementação Postgres (pgx/v5) da interface Store definida
// em store.go. Dono: Agente C.
package store

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/iceasa/evolution-chatwoot-connector/internal/model"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Postgres implementa store.Store usando pgx/v5 (pgxpool).
type Postgres struct {
	pool *pgxpool.Pool
}

// garante, em tempo de compilação, que Postgres satisfaz a interface Store.
var _ Store = (*Postgres)(nil)

// New cria o pool de conexões e retorna o Store Postgres.
// dsn no formato postgresql://user:pass@host:5432/evogo_chatwoot?sslmode=disable
// (ver CONNECTOR_POSTGRES_DSN em internal/config).
func New(ctx context.Context, dsn string) (*Postgres, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("store: dsn inválida: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("store: erro ao criar pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: erro ao conectar: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

// Close libera o pool de conexões.
func (p *Postgres) Close() {
	if p.pool != nil {
		p.pool.Close()
	}
}

// ---------------------------------------------------------------------------
// Migrate
// ---------------------------------------------------------------------------

// Migrate aplica, em ordem alfabética de nome de arquivo, as migrations
// embutidas em migrations/*.sql que ainda não constam em schema_migrations.
// Implementação simples e sem dependência externa de migração: cada arquivo
// é aplicado dentro de uma transação e registrado como aplicado.
func (p *Postgres) Migrate(ctx context.Context) error {
	if _, err := p.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version     TEXT PRIMARY KEY,
			applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		return fmt.Errorf("store: erro ao criar schema_migrations: %w", err)
	}

	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("store: erro ao ler migrations embutidas: %w", err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)

	for _, name := range names {
		var applied bool
		if err := p.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`,
			name,
		).Scan(&applied); err != nil {
			return fmt.Errorf("store: erro ao checar migration %s: %w", name, err)
		}
		if applied {
			continue
		}

		sqlBytes, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("store: erro ao ler migration %s: %w", name, err)
		}

		tx, err := p.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("store: erro ao iniciar tx para migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("store: erro ao aplicar migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version) VALUES ($1)`, name,
		); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("store: erro ao registrar migration %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("store: erro ao commitar migration %s: %w", name, err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Configs (chatwoot_configs)
// ---------------------------------------------------------------------------

const configColumns = `
	instance_id, instance_name, enabled, url, account_id, token, name_inbox,
	sign_msg, sign_delimiter, number, reopen_conversation, conversation_pending,
	merge_brazil_contacts, import_contacts, import_messages,
	days_limit_import_messages, auto_create, organization, logo, ignore_jids, inbox_id
`

func scanConfig(row pgx.Row) (*model.ChatwootConfig, error) {
	var cfg model.ChatwootConfig
	var ignoreJIDsRaw []byte
	err := row.Scan(
		&cfg.InstanceID, &cfg.InstanceName, &cfg.Enabled, &cfg.URL, &cfg.AccountID,
		&cfg.Token, &cfg.NameInbox, &cfg.SignMsg, &cfg.SignDelimiter, &cfg.Number,
		&cfg.ReopenConversation, &cfg.ConversationPending, &cfg.MergeBrazilContacts,
		&cfg.ImportContacts, &cfg.ImportMessages, &cfg.DaysLimitImportMessages,
		&cfg.AutoCreate, &cfg.Organization, &cfg.Logo, &ignoreJIDsRaw, &cfg.InboxID,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if len(ignoreJIDsRaw) > 0 {
		if err := json.Unmarshal(ignoreJIDsRaw, &cfg.IgnoreJids); err != nil {
			return nil, fmt.Errorf("store: erro ao decodificar ignore_jids: %w", err)
		}
	}
	return &cfg, nil
}

// GetConfig retorna a configuração da instância, ou (nil, nil) se não existir.
func (p *Postgres) GetConfig(ctx context.Context, instanceID string) (*model.ChatwootConfig, error) {
	row := p.pool.QueryRow(ctx, `SELECT `+configColumns+` FROM chatwoot_configs WHERE instance_id = $1`, instanceID)
	return scanConfig(row)
}

// ListEnabledConfigs retorna todas as configurações com enabled = true.
func (p *Postgres) ListEnabledConfigs(ctx context.Context) ([]*model.ChatwootConfig, error) {
	rows, err := p.pool.Query(ctx, `SELECT `+configColumns+` FROM chatwoot_configs WHERE enabled = TRUE ORDER BY instance_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*model.ChatwootConfig
	for rows.Next() {
		cfg, err := scanConfig(rows)
		if err != nil {
			return nil, err
		}
		if cfg != nil {
			out = append(out, cfg)
		}
	}
	return out, rows.Err()
}

// ListConfigs retorna todas as configurações, habilitadas ou não (painel).
func (p *Postgres) ListConfigs(ctx context.Context) ([]*model.ChatwootConfig, error) {
	rows, err := p.pool.Query(ctx, `SELECT `+configColumns+` FROM chatwoot_configs ORDER BY instance_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*model.ChatwootConfig
	for rows.Next() {
		cfg, err := scanConfig(rows)
		if err != nil {
			return nil, err
		}
		if cfg != nil {
			out = append(out, cfg)
		}
	}
	return out, rows.Err()
}

// SaveConfig faz upsert (ON CONFLICT DO UPDATE) da configuração da instância.
func (p *Postgres) SaveConfig(ctx context.Context, cfg *model.ChatwootConfig) error {
	if cfg == nil {
		return errors.New("store: cfg não pode ser nil")
	}
	ignoreJIDsJSON, err := json.Marshal(cfg.IgnoreJids)
	if err != nil {
		return fmt.Errorf("store: erro ao codificar ignore_jids: %w", err)
	}
	_, err = p.pool.Exec(ctx, `
		INSERT INTO chatwoot_configs (
			instance_id, instance_name, enabled, url, account_id, token, name_inbox,
			sign_msg, sign_delimiter, number, reopen_conversation, conversation_pending,
			merge_brazil_contacts, import_contacts, import_messages,
			days_limit_import_messages, auto_create, organization, logo, ignore_jids, inbox_id,
			updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
			$16, $17, $18, $19, $20::jsonb, $21, now()
		)
		ON CONFLICT (instance_id) DO UPDATE SET
			instance_name               = EXCLUDED.instance_name,
			enabled                      = EXCLUDED.enabled,
			url                          = EXCLUDED.url,
			account_id                   = EXCLUDED.account_id,
			token                        = EXCLUDED.token,
			name_inbox                   = EXCLUDED.name_inbox,
			sign_msg                     = EXCLUDED.sign_msg,
			sign_delimiter               = EXCLUDED.sign_delimiter,
			number                       = EXCLUDED.number,
			reopen_conversation          = EXCLUDED.reopen_conversation,
			conversation_pending         = EXCLUDED.conversation_pending,
			merge_brazil_contacts        = EXCLUDED.merge_brazil_contacts,
			import_contacts              = EXCLUDED.import_contacts,
			import_messages              = EXCLUDED.import_messages,
			days_limit_import_messages   = EXCLUDED.days_limit_import_messages,
			auto_create                  = EXCLUDED.auto_create,
			organization                 = EXCLUDED.organization,
			logo                         = EXCLUDED.logo,
			ignore_jids                  = EXCLUDED.ignore_jids,
			inbox_id                     = EXCLUDED.inbox_id,
			updated_at                   = now()
	`,
		cfg.InstanceID, cfg.InstanceName, cfg.Enabled, cfg.URL, cfg.AccountID,
		cfg.Token, cfg.NameInbox, cfg.SignMsg, cfg.SignDelimiter, cfg.Number,
		cfg.ReopenConversation, cfg.ConversationPending, cfg.MergeBrazilContacts,
		cfg.ImportContacts, cfg.ImportMessages, cfg.DaysLimitImportMessages,
		cfg.AutoCreate, cfg.Organization, cfg.Logo, string(ignoreJIDsJSON), cfg.InboxID,
	)
	return err
}

// DeleteConfig remove a configuração da instância.
func (p *Postgres) DeleteConfig(ctx context.Context, instanceID string) error {
	_, err := p.pool.Exec(ctx, `DELETE FROM chatwoot_configs WHERE instance_id = $1`, instanceID)
	return err
}

// ---------------------------------------------------------------------------
// Contatos (contact_mappings)
// ---------------------------------------------------------------------------

// GetContact retorna o mapeamento de contato, ou (nil, nil) se não existir.
func (p *Postgres) GetContact(ctx context.Context, instanceID, remoteJid string) (*model.ContactMapping, error) {
	row := p.pool.QueryRow(ctx, `
		SELECT instance_id, remote_jid, chatwoot_contact_id, identifier, updated_at
		FROM contact_mappings
		WHERE instance_id = $1 AND remote_jid = $2
	`, instanceID, remoteJid)

	var m model.ContactMapping
	err := row.Scan(&m.InstanceID, &m.RemoteJid, &m.ChatwootContactID, &m.Identifier, &m.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &m, nil
}

// SaveContact faz upsert do mapeamento de contato.
// updated_at é sempre definido pelo banco (now()) no momento do save.
func (p *Postgres) SaveContact(ctx context.Context, m *model.ContactMapping) error {
	if m == nil {
		return errors.New("store: m não pode ser nil")
	}
	_, err := p.pool.Exec(ctx, `
		INSERT INTO contact_mappings (instance_id, remote_jid, chatwoot_contact_id, identifier, updated_at)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (instance_id, remote_jid) DO UPDATE SET
			chatwoot_contact_id = EXCLUDED.chatwoot_contact_id,
			identifier          = EXCLUDED.identifier,
			updated_at          = now()
	`, m.InstanceID, m.RemoteJid, m.ChatwootContactID, m.Identifier)
	return err
}

// ---------------------------------------------------------------------------
// Conversas (conversation_mappings)
// ---------------------------------------------------------------------------

func scanConversation(row pgx.Row) (*model.ConversationMapping, error) {
	var m model.ConversationMapping
	err := row.Scan(&m.InstanceID, &m.RemoteJid, &m.ChatwootConversationID, &m.InboxID, &m.Status, &m.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &m, nil
}

// GetConversation retorna o mapeamento de conversa, ou (nil, nil) se não existir.
func (p *Postgres) GetConversation(ctx context.Context, instanceID, remoteJid string) (*model.ConversationMapping, error) {
	row := p.pool.QueryRow(ctx, `
		SELECT instance_id, remote_jid, chatwoot_conversation_id, inbox_id, status, updated_at
		FROM conversation_mappings
		WHERE instance_id = $1 AND remote_jid = $2
	`, instanceID, remoteJid)
	return scanConversation(row)
}

// GetConversationByChatwootID resolve o mapeamento a partir do conversation_id
// do Chatwoot (usado pelo egress ao processar webhooks de inbox). Método extra
// no struct concreto — ver INTERFACE-CHANGE-REQUEST no topo do arquivo.
func (p *Postgres) GetConversationByChatwootID(ctx context.Context, instanceID string, cwConversationID int) (*model.ConversationMapping, error) {
	row := p.pool.QueryRow(ctx, `
		SELECT instance_id, remote_jid, chatwoot_conversation_id, inbox_id, status, updated_at
		FROM conversation_mappings
		WHERE instance_id = $1 AND chatwoot_conversation_id = $2
	`, instanceID, cwConversationID)
	return scanConversation(row)
}

// SaveConversation faz upsert do mapeamento de conversa.
// updated_at é sempre definido pelo banco (now()) no momento do save.
func (p *Postgres) SaveConversation(ctx context.Context, m *model.ConversationMapping) error {
	if m == nil {
		return errors.New("store: m não pode ser nil")
	}
	_, err := p.pool.Exec(ctx, `
		INSERT INTO conversation_mappings (instance_id, remote_jid, chatwoot_conversation_id, inbox_id, status, updated_at)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (instance_id, remote_jid) DO UPDATE SET
			chatwoot_conversation_id = EXCLUDED.chatwoot_conversation_id,
			inbox_id                 = EXCLUDED.inbox_id,
			status                    = EXCLUDED.status,
			updated_at                = now()
	`, m.InstanceID, m.RemoteJid, m.ChatwootConversationID, m.InboxID, m.Status)
	return err
}

// DeleteConversation remove o mapeamento de conversa.
func (p *Postgres) DeleteConversation(ctx context.Context, instanceID, remoteJid string) error {
	_, err := p.pool.Exec(ctx, `
		DELETE FROM conversation_mappings WHERE instance_id = $1 AND remote_jid = $2
	`, instanceID, remoteJid)
	return err
}

// ---------------------------------------------------------------------------
// Mensagens (message_mappings)
// ---------------------------------------------------------------------------

func scanMessage(row pgx.Row) (*model.MessageMapping, error) {
	var m model.MessageMapping
	err := row.Scan(&m.InstanceID, &m.WhatsappMessageID, &m.ChatwootMessageID, &m.Direction, &m.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &m, nil
}

// GetMessageByWhatsappID retorna o mapeamento pela key.id do WhatsApp, ou (nil, nil).
func (p *Postgres) GetMessageByWhatsappID(ctx context.Context, instanceID, waMessageID string) (*model.MessageMapping, error) {
	row := p.pool.QueryRow(ctx, `
		SELECT instance_id, whatsapp_message_id, chatwoot_message_id, direction, created_at
		FROM message_mappings
		WHERE instance_id = $1 AND whatsapp_message_id = $2
	`, instanceID, waMessageID)
	return scanMessage(row)
}

// GetMessageByChatwootID retorna o mapeamento pelo id da mensagem no Chatwoot, ou (nil, nil).
// Quando há múltiplos registros (não deveria ocorrer no fluxo normal), retorna o mais recente.
func (p *Postgres) GetMessageByChatwootID(ctx context.Context, instanceID string, cwMessageID int) (*model.MessageMapping, error) {
	row := p.pool.QueryRow(ctx, `
		SELECT instance_id, whatsapp_message_id, chatwoot_message_id, direction, created_at
		FROM message_mappings
		WHERE instance_id = $1 AND chatwoot_message_id = $2
		ORDER BY created_at DESC
		LIMIT 1
	`, instanceID, cwMessageID)
	return scanMessage(row)
}

// SaveMessage faz upsert do mapeamento de mensagem. created_at só é definido
// na inserção (default now() da tabela) e não é alterado em atualizações,
// preservando o valor original para o expurgo futuro por data.
func (p *Postgres) SaveMessage(ctx context.Context, m *model.MessageMapping) error {
	if m == nil {
		return errors.New("store: m não pode ser nil")
	}
	_, err := p.pool.Exec(ctx, `
		INSERT INTO message_mappings (instance_id, whatsapp_message_id, chatwoot_message_id, direction)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (instance_id, whatsapp_message_id) DO UPDATE SET
			chatwoot_message_id = EXCLUDED.chatwoot_message_id,
			direction            = EXCLUDED.direction
	`, m.InstanceID, m.WhatsappMessageID, m.ChatwootMessageID, m.Direction)
	return err
}
