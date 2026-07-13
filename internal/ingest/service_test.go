package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/iceasa/evolution-chatwoot-connector/internal/chatwoot"
	"github.com/iceasa/evolution-chatwoot-connector/internal/model"
)

// ---- fakes mínimos ----

type fakeStore struct {
	configs  map[string]*model.ChatwootConfig
	messages map[string]*model.MessageMapping
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		configs:  map[string]*model.ChatwootConfig{},
		messages: map[string]*model.MessageMapping{},
	}
}

func (f *fakeStore) GetConfig(_ context.Context, id string) (*model.ChatwootConfig, error) {
	return f.configs[id], nil
}
func (f *fakeStore) SaveConfig(context.Context, *model.ChatwootConfig) error { return nil }
func (f *fakeStore) DeleteConfig(context.Context, string) error              { return nil }
func (f *fakeStore) ListEnabledConfigs(context.Context) ([]*model.ChatwootConfig, error) {
	return nil, nil
}
func (f *fakeStore) ListConfigs(context.Context) ([]*model.ChatwootConfig, error) { return nil, nil }
func (f *fakeStore) GetContact(context.Context, string, string) (*model.ContactMapping, error) {
	return nil, nil
}
func (f *fakeStore) SaveContact(context.Context, *model.ContactMapping) error { return nil }
func (f *fakeStore) GetConversation(context.Context, string, string) (*model.ConversationMapping, error) {
	return nil, nil
}
func (f *fakeStore) GetConversationByChatwootID(context.Context, string, int) (*model.ConversationMapping, error) {
	return nil, nil
}
func (f *fakeStore) SaveConversation(context.Context, *model.ConversationMapping) error { return nil }
func (f *fakeStore) DeleteConversation(context.Context, string, string) error           { return nil }
func (f *fakeStore) GetMessageByWhatsappID(_ context.Context, id, waID string) (*model.MessageMapping, error) {
	return f.messages[id+"/"+waID], nil
}
func (f *fakeStore) GetMessageByChatwootID(context.Context, string, int) (*model.MessageMapping, error) {
	return nil, nil
}
func (f *fakeStore) SaveMessage(context.Context, *model.MessageMapping) error { return nil }
func (f *fakeStore) Migrate(context.Context) error                            { return nil }
func (f *fakeStore) Close()                                                   {}

// ---- testes ----

func TestHandleIgnoresUnknownInstance(t *testing.T) {
	s := New(newFakeStore(), nil, nil, nil, slog.Default())
	err := s.Handle(context.Background(), &model.EventEnvelope{
		Event:      "Message",
		InstanceID: "nao-existe",
		Data:       json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("instância sem config deveria ser ignorada, veio erro: %v", err)
	}
}

func TestHandleIgnoresDisabledInstance(t *testing.T) {
	st := newFakeStore()
	st.configs["inst1"] = &model.ChatwootConfig{InstanceID: "inst1", Enabled: false}
	s := New(st, nil, nil, nil, slog.Default())
	err := s.Handle(context.Background(), &model.EventEnvelope{
		Event:      "Message",
		InstanceID: "inst1",
		Data:       json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("instância desabilitada deveria ser ignorada, veio erro: %v", err)
	}
}

func TestParseMessageEventBothCasings(t *testing.T) {
	// PascalCase (encoding/json de struct Go sem tags)
	pascal := json.RawMessage(`{"Info":{"ID":"ABC123","Chat":"5511999999999@s.whatsapp.net","IsFromMe":false,"PushName":"Osmar","Timestamp":"2026-07-09T12:00:00Z"},"Message":{"conversation":"olá"}}`)
	m, err := parseMessageEvent(pascal)
	if err != nil {
		t.Fatalf("parse PascalCase: %v", err)
	}
	if m.ID != "ABC123" || m.RemoteJid != "5511999999999@s.whatsapp.net" || m.FromMe {
		t.Fatalf("parse PascalCase incorreto: %+v", m)
	}
	if got := conversationContent(m.Message); got != "olá" {
		t.Fatalf("conteúdo esperado 'olá', veio %q", got)
	}
}

func TestParseWebMessageInfo(t *testing.T) {
	wmi := map[string]any{
		"key": map[string]any{
			"remoteJid": "5511888888888@s.whatsapp.net",
			"fromMe":    true,
			"ID":        "HIST1",
		},
		"messageTimestamp": float64(time.Now().Unix()),
		"message":          map[string]any{"conversation": "histórico"},
	}
	m := parseWebMessageInfo("fallback@s.whatsapp.net", wmi)
	if m == nil || m.ID != "HIST1" {
		t.Fatalf("parseWebMessageInfo falhou: %+v", m)
	}
	if m.RemoteJid != "5511888888888@s.whatsapp.net" {
		t.Fatalf("remoteJid do key deveria prevalecer, veio %q", m.RemoteJid)
	}
	if !m.FromMe {
		t.Fatal("fromMe deveria ser true")
	}
}

func TestShouldIgnoreJid(t *testing.T) {
	cases := []struct {
		list []string
		jid  string
		want bool
	}{
		{[]string{"@g.us"}, "123@g.us", true},
		{[]string{"@g.us"}, "123@s.whatsapp.net", false},
		{[]string{"@s.whatsapp.net"}, "123@s.whatsapp.net", true},
		{[]string{"5511999999999@s.whatsapp.net"}, "5511999999999@s.whatsapp.net", true},
		{nil, "qualquer@s.whatsapp.net", false},
	}
	for _, c := range cases {
		if got := shouldIgnoreJid(c.list, c.jid); got != c.want {
			t.Errorf("shouldIgnoreJid(%v, %q) = %v, want %v", c.list, c.jid, got, c.want)
		}
	}
}

// Garante em tempo de compilação que os fakes satisfazem as interfaces usadas.
var _ chatwoot.Client = chatwoot.Client(nil)
