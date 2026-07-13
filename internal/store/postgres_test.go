package store

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/iceasa/evolution-chatwoot-connector/internal/model"
)

// ---------------------------------------------------------------------------
// fakeRow — implementação mínima de pgx.Row (interface { Scan(dest ...any) error })
// para testar a lógica de scan (scanConfig/scanConversation/scanMessage) sem
// depender de um banco Postgres real.
// ---------------------------------------------------------------------------

type fakeRow struct {
	values []any
	err    error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		panic("fakeRow: número de destinos difere do número de valores")
	}
	for i, d := range dest {
		reflect.ValueOf(d).Elem().Set(reflect.ValueOf(r.values[i]))
	}
	return nil
}

var _ pgx.Row = fakeRow{}

// ---------------------------------------------------------------------------
// scanConfig
// ---------------------------------------------------------------------------

func TestScanConfig_OK(t *testing.T) {
	ignoreJIDs := []byte(`["555@s.whatsapp.net","999@s.whatsapp.net"]`)
	row := fakeRow{values: []any{
		"inst-1", "Instância 1", true, "https://cw.example.com", "1", "cw-token",
		"WhatsApp Inbox", false, "---", "5511999999999",
		true, false, true, true, false,
		30, true, "Org", "https://logo.png", ignoreJIDs, 42,
	}}

	cfg, err := scanConfig(row)
	if err != nil {
		t.Fatalf("scanConfig retornou erro inesperado: %v", err)
	}
	if cfg == nil {
		t.Fatal("scanConfig retornou nil sem erro")
	}
	if cfg.InstanceID != "inst-1" || cfg.AccountID != "1" || cfg.InboxID != 42 {
		t.Fatalf("campos escalares não bateram: %+v", cfg)
	}
	want := []string{"555@s.whatsapp.net", "999@s.whatsapp.net"}
	if !reflect.DeepEqual(cfg.IgnoreJids, want) {
		t.Fatalf("IgnoreJids = %v, want %v", cfg.IgnoreJids, want)
	}
}

func TestScanConfig_NotFound(t *testing.T) {
	row := fakeRow{err: pgx.ErrNoRows}
	cfg, err := scanConfig(row)
	if err != nil {
		t.Fatalf("scanConfig deveria retornar (nil, nil) para ErrNoRows, veio erro: %v", err)
	}
	if cfg != nil {
		t.Fatalf("scanConfig deveria retornar nil, veio %+v", cfg)
	}
}

func TestScanConfig_EmptyIgnoreJids(t *testing.T) {
	row := fakeRow{values: []any{
		"inst-2", "", false, "", "", "", "", false, "", "",
		false, false, false, false, false,
		0, false, "", "", []byte(`[]`), 0,
	}}
	cfg, err := scanConfig(row)
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if len(cfg.IgnoreJids) != 0 {
		t.Fatalf("IgnoreJids deveria ser vazio, veio %v", cfg.IgnoreJids)
	}
}

// ---------------------------------------------------------------------------
// scanConversation
// ---------------------------------------------------------------------------

func TestScanConversation_OK(t *testing.T) {
	now := time.Now().UTC()
	row := fakeRow{values: []any{"inst-1", "5511999999999@s.whatsapp.net", 123, 7, "open", now}}

	m, err := scanConversation(row)
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if m == nil {
		t.Fatal("scanConversation retornou nil sem erro")
	}
	if m.ChatwootConversationID != 123 || m.InboxID != 7 || m.Status != "open" {
		t.Fatalf("campos não bateram: %+v", m)
	}
}

func TestScanConversation_NotFound(t *testing.T) {
	row := fakeRow{err: pgx.ErrNoRows}
	m, err := scanConversation(row)
	if err != nil || m != nil {
		t.Fatalf("esperava (nil, nil), veio (%+v, %v)", m, err)
	}
}

// ---------------------------------------------------------------------------
// scanMessage
// ---------------------------------------------------------------------------

func TestScanMessage_OK(t *testing.T) {
	now := time.Now().UTC()
	row := fakeRow{values: []any{"inst-1", "3EB0ABCDEF", 999, "in", now}}

	m, err := scanMessage(row)
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if m.WhatsappMessageID != "3EB0ABCDEF" || m.ChatwootMessageID != 999 || m.Direction != "in" {
		t.Fatalf("campos não bateram: %+v", m)
	}
}

func TestScanMessage_NotFound(t *testing.T) {
	row := fakeRow{err: pgx.ErrNoRows}
	m, err := scanMessage(row)
	if err != nil || m != nil {
		t.Fatalf("esperava (nil, nil), veio (%+v, %v)", m, err)
	}
}

// ---------------------------------------------------------------------------
// Montagem de SQL: configColumns precisa ter exatamente o mesmo número de
// colunas que scanConfig espera escanear (21 — os 20 campos de
// model.ChatwootConfig + ignore_jids bruto).
// ---------------------------------------------------------------------------

func TestConfigColumns_Count(t *testing.T) {
	cols := strings.Split(strings.ReplaceAll(strings.TrimSpace(configColumns), "\n", ""), ",")
	const want = 21
	if len(cols) != want {
		t.Fatalf("configColumns tem %d colunas, esperava %d: %q", len(cols), want, configColumns)
	}
}

// ---------------------------------------------------------------------------
// Round-trip de serialização de ignore_jids (mesma lógica usada em
// SaveConfig/scanConfig, isolada de I/O).
// ---------------------------------------------------------------------------

func TestIgnoreJidsRoundTrip(t *testing.T) {
	orig := []string{"a@s.whatsapp.net", "b@g.us"}
	raw, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got []string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(orig, got) {
		t.Fatalf("round-trip falhou: got %v, want %v", got, orig)
	}
}

// ---------------------------------------------------------------------------
// Locker no-op (não exige Redis real).
// ---------------------------------------------------------------------------

func TestNewLocker_EmptyURL_IsNoop(t *testing.T) {
	l, err := NewLocker("")
	if err != nil {
		t.Fatalf("NewLocker(\"\") não deveria falhar: %v", err)
	}
	if _, ok := l.(noopLocker); !ok {
		t.Fatalf("NewLocker(\"\") deveria retornar noopLocker, veio %T", l)
	}

	ctx := context.Background()
	ok, release, err := l.AcquireLock(ctx, "qualquer-chave", time.Second)
	if err != nil || !ok || release == nil {
		t.Fatalf("noopLocker deveria sempre adquirir: ok=%v releaseNil=%v err=%v", ok, release == nil, err)
	}
	release() // não deve panicar

	// Adquirir de novo com a mesma chave também deve funcionar (no-op nunca trava).
	ok2, release2, err2 := l.AcquireLock(ctx, "qualquer-chave", time.Second)
	if err2 != nil || !ok2 || release2 == nil {
		t.Fatalf("segunda aquisição deveria funcionar: ok=%v err=%v", ok2, err2)
	}
	release2()
}

func TestNewLocker_InvalidURL(t *testing.T) {
	if _, err := NewLocker("://not-a-valid-redis-url"); err == nil {
		t.Fatal("esperava erro para URL de Redis inválida")
	}
}

func TestRandomToken_UniqueAndFormatted(t *testing.T) {
	seen := make(map[string]bool, 1000)
	for i := 0; i < 1000; i++ {
		tok, err := randomToken()
		if err != nil {
			t.Fatalf("randomToken: %v", err)
		}
		if len(tok) != 32 { // 16 bytes em hex
			t.Fatalf("token com tamanho inesperado: %q (len=%d)", tok, len(tok))
		}
		if seen[tok] {
			t.Fatalf("token duplicado gerado: %q", tok)
		}
		seen[tok] = true
	}
}

// ---------------------------------------------------------------------------
// Testes de integração — exigem um Postgres real. Pulados automaticamente
// quando a variável de ambiente DATABASE_URL não está definida (sem
// testcontainers, conforme decisão do projeto).
// ---------------------------------------------------------------------------

func TestPostgres_Integration(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL não definida — pulando teste de integração com Postgres real")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pg, err := New(ctx, dsn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pg.Close()

	if err := pg.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	// Migrate deve ser idempotente.
	if err := pg.Migrate(ctx); err != nil {
		t.Fatalf("Migrate (segunda chamada): %v", err)
	}

	instanceID := "test-instance-" + time.Now().Format("20060102150405.000000")

	// Configs
	cfg := &model.ChatwootConfig{
		InstanceID:  instanceID,
		Enabled:     true,
		URL:         "https://cw.example.com",
		AccountID:   "1",
		Token:       "tok",
		NameInbox:   "WhatsApp",
		IgnoreJids:  []string{"555@s.whatsapp.net"},
		InboxID:     10,
	}
	if err := pg.SaveConfig(ctx, cfg); err != nil {
		t.Fatalf("SaveConfig: %v", err)
	}
	got, err := pg.GetConfig(ctx, instanceID)
	if err != nil || got == nil {
		t.Fatalf("GetConfig: got=%v err=%v", got, err)
	}
	if !reflect.DeepEqual(got.IgnoreJids, cfg.IgnoreJids) {
		t.Fatalf("IgnoreJids não persistiu corretamente: %v", got.IgnoreJids)
	}

	enabled, err := pg.ListEnabledConfigs(ctx)
	if err != nil {
		t.Fatalf("ListEnabledConfigs: %v", err)
	}
	found := false
	for _, c := range enabled {
		if c.InstanceID == instanceID {
			found = true
		}
	}
	if !found {
		t.Fatal("ListEnabledConfigs não retornou a config recém-criada")
	}

	// Contatos
	remoteJid := "5511999999999@s.whatsapp.net"
	contact := &model.ContactMapping{InstanceID: instanceID, RemoteJid: remoteJid, ChatwootContactID: 55, Identifier: "id-1"}
	if err := pg.SaveContact(ctx, contact); err != nil {
		t.Fatalf("SaveContact: %v", err)
	}
	gotContact, err := pg.GetContact(ctx, instanceID, remoteJid)
	if err != nil || gotContact == nil || gotContact.ChatwootContactID != 55 {
		t.Fatalf("GetContact: got=%+v err=%v", gotContact, err)
	}

	// Conversas
	conv := &model.ConversationMapping{InstanceID: instanceID, RemoteJid: remoteJid, ChatwootConversationID: 77, InboxID: 10, Status: "open"}
	if err := pg.SaveConversation(ctx, conv); err != nil {
		t.Fatalf("SaveConversation: %v", err)
	}
	gotConv, err := pg.GetConversation(ctx, instanceID, remoteJid)
	if err != nil || gotConv == nil || gotConv.ChatwootConversationID != 77 {
		t.Fatalf("GetConversation: got=%+v err=%v", gotConv, err)
	}
	gotConvByCw, err := pg.GetConversationByChatwootID(ctx, instanceID, 77)
	if err != nil || gotConvByCw == nil || gotConvByCw.RemoteJid != remoteJid {
		t.Fatalf("GetConversationByChatwootID: got=%+v err=%v", gotConvByCw, err)
	}
	if err := pg.DeleteConversation(ctx, instanceID, remoteJid); err != nil {
		t.Fatalf("DeleteConversation: %v", err)
	}
	if gone, err := pg.GetConversation(ctx, instanceID, remoteJid); err != nil || gone != nil {
		t.Fatalf("conversa deveria ter sido removida: got=%+v err=%v", gone, err)
	}

	// Mensagens
	msg := &model.MessageMapping{InstanceID: instanceID, WhatsappMessageID: "WA123", ChatwootMessageID: 999, Direction: "in"}
	if err := pg.SaveMessage(ctx, msg); err != nil {
		t.Fatalf("SaveMessage: %v", err)
	}
	gotMsg, err := pg.GetMessageByWhatsappID(ctx, instanceID, "WA123")
	if err != nil || gotMsg == nil || gotMsg.ChatwootMessageID != 999 {
		t.Fatalf("GetMessageByWhatsappID: got=%+v err=%v", gotMsg, err)
	}
	gotMsgByCw, err := pg.GetMessageByChatwootID(ctx, instanceID, 999)
	if err != nil || gotMsgByCw == nil || gotMsgByCw.WhatsappMessageID != "WA123" {
		t.Fatalf("GetMessageByChatwootID: got=%+v err=%v", gotMsgByCw, err)
	}

	// Limpeza
	if err := pg.DeleteConfig(ctx, instanceID); err != nil {
		t.Fatalf("DeleteConfig: %v", err)
	}
	if gone, err := pg.GetConfig(ctx, instanceID); err != nil || gone != nil {
		t.Fatalf("config deveria ter sido removida: got=%+v err=%v", gone, err)
	}
}
