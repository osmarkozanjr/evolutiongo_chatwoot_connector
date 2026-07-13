package panel

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/iceasa/evolution-chatwoot-connector/internal/chatwoot"
	"github.com/iceasa/evolution-chatwoot-connector/internal/config"
	"github.com/iceasa/evolution-chatwoot-connector/internal/model"
	"github.com/iceasa/evolution-chatwoot-connector/internal/store"
)

// ---------------------------------------------------------------------
// fakeStore: implementação mínima in-memory de store.Store, só para teste
// do handler do painel (não é a implementação real — essa é do Agente C).
// ---------------------------------------------------------------------

type fakeStore struct {
	mu      sync.Mutex
	configs map[string]*model.ChatwootConfig
}

func newFakeStore() *fakeStore {
	return &fakeStore{configs: map[string]*model.ChatwootConfig{}}
}

func (f *fakeStore) GetConfig(_ context.Context, instanceID string) (*model.ChatwootConfig, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cfg, ok := f.configs[instanceID]
	if !ok {
		return nil, nil
	}
	cp := *cfg
	return &cp, nil
}

func (f *fakeStore) SaveConfig(_ context.Context, cfg *model.ChatwootConfig) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *cfg
	f.configs[cfg.InstanceID] = &cp
	return nil
}

func (f *fakeStore) DeleteConfig(_ context.Context, instanceID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.configs, instanceID)
	return nil
}

func (f *fakeStore) ListEnabledConfigs(_ context.Context) ([]*model.ChatwootConfig, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*model.ChatwootConfig
	for _, cfg := range f.configs {
		if cfg.Enabled {
			cp := *cfg
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (f *fakeStore) ListConfigs(_ context.Context) ([]*model.ChatwootConfig, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*model.ChatwootConfig
	for _, cfg := range f.configs {
		cp := *cfg
		out = append(out, &cp)
	}
	return out, nil
}

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

func (f *fakeStore) GetMessageByWhatsappID(context.Context, string, string) (*model.MessageMapping, error) {
	return nil, nil
}
func (f *fakeStore) GetMessageByChatwootID(context.Context, string, int) (*model.MessageMapping, error) {
	return nil, nil
}
func (f *fakeStore) SaveMessage(context.Context, *model.MessageMapping) error { return nil }

func (f *fakeStore) Migrate(context.Context) error { return nil }
func (f *fakeStore) Close()                        {}

var _ store.Store = (*fakeStore)(nil)

// ---------------------------------------------------------------------
// fakeChatwoot: implementação mínima de chatwoot.Client. Só EnsureInbox e
// UpdateInboxWebhook importam para o provisionamento do painel; os demais
// métodos são stubs para satisfazer a interface.
// ---------------------------------------------------------------------

type fakeChatwoot struct {
	mu             sync.Mutex
	ensureCalls    int
	nextInboxID    int
	failEnsure     error
	updatedWebhook string
}

func (f *fakeChatwoot) EnsureInbox(_ context.Context, _ *model.ChatwootConfig, webhookURL string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureCalls++
	if f.failEnsure != nil {
		return 0, f.failEnsure
	}
	if f.nextInboxID == 0 {
		f.nextInboxID = 42
	}
	f.updatedWebhook = webhookURL
	return f.nextInboxID, nil
}

func (f *fakeChatwoot) UpdateInboxWebhook(_ context.Context, _ *model.ChatwootConfig, _ int, webhookURL string) error {
	f.updatedWebhook = webhookURL
	return nil
}

func (f *fakeChatwoot) SearchContact(context.Context, *model.ChatwootConfig, string) (*chatwoot.Contact, error) {
	return nil, nil
}
func (f *fakeChatwoot) CreateContact(context.Context, *model.ChatwootConfig, int, string, string, string, string) (*chatwoot.Contact, error) {
	return nil, nil
}
func (f *fakeChatwoot) UpdateContact(context.Context, *model.ChatwootConfig, int, map[string]any) error {
	return nil
}
func (f *fakeChatwoot) MergeContacts(context.Context, *model.ChatwootConfig, int, int) error {
	return nil
}
func (f *fakeChatwoot) GetOpenConversation(context.Context, *model.ChatwootConfig, int, int) (*chatwoot.Conversation, error) {
	return nil, nil
}
func (f *fakeChatwoot) CreateConversation(context.Context, *model.ChatwootConfig, int, int, string) (*chatwoot.Conversation, error) {
	return nil, nil
}
func (f *fakeChatwoot) ToggleConversationStatus(context.Context, *model.ChatwootConfig, int, string) error {
	return nil
}
func (f *fakeChatwoot) CreateMessage(context.Context, *model.ChatwootConfig, int, string, string, []chatwoot.Attachment, string) (*chatwoot.Message, error) {
	return nil, nil
}
func (f *fakeChatwoot) DeleteMessage(context.Context, *model.ChatwootConfig, int, int) error {
	return nil
}

var _ chatwoot.Client = (*fakeChatwoot)(nil)

// ---------------------------------------------------------------------
// Helpers de teste
// ---------------------------------------------------------------------

func newTestHandler() (*Handler, *fakeStore, *fakeChatwoot) {
	st := newFakeStore()
	cw := &fakeChatwoot{}
	cfg := &config.Config{
		PublicURL: "https://evoconnector.iceasa.com.br",
		APIKey:    "test-apikey",
	}
	return NewHandler(st, cw, cfg), st, cw
}

func doRequest(t *testing.T, r http.Handler, method, path, apikey string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(b)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	if apikey != "" {
		req.Header.Set("apikey", apikey)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func newRouter(h *Handler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.Register(r)
	return r
}

// ---------------------------------------------------------------------
// Testes
// ---------------------------------------------------------------------

func TestAuthMiddleware_RejeitaSemApikey(t *testing.T) {
	h, _, _ := newTestHandler()
	r := newRouter(h)

	w := doRequest(t, r, http.MethodGet, "/api/instances", "", nil)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("esperado 401, obtido %d: %s", w.Code, w.Body.String())
	}

	w2 := doRequest(t, r, http.MethodGet, "/api/instances", "chave-errada", nil)
	if w2.Code != http.StatusUnauthorized {
		t.Fatalf("esperado 401 com apikey errada, obtido %d", w2.Code)
	}
}

func TestFindChatwoot_InstanciaInexistenteRetornaVazio(t *testing.T) {
	h, _, _ := newTestHandler()
	r := newRouter(h)

	w := doRequest(t, r, http.MethodGet, "/api/chatwoot/inst-1", "test-apikey", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("esperado 200, obtido %d: %s", w.Code, w.Body.String())
	}

	var resp findResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("json inválido: %v", err)
	}
	if resp.Enabled {
		t.Fatalf("esperado enabled=false para instância inexistente")
	}
	if resp.WebhookURL != "" {
		t.Fatalf("esperado webhook_url vazio para instância inexistente, obtido %q", resp.WebhookURL)
	}
}

func TestSetChatwoot_EnabledSemCamposObrigatoriosFalha(t *testing.T) {
	h, _, _ := newTestHandler()
	r := newRouter(h)

	cases := []struct {
		name string
		body setRequest
	}{
		{"sem url", setRequest{Enabled: true, AccountID: "1", Token: "tok", SignMsg: boolPtr(true)}},
		{"sem accountId", setRequest{Enabled: true, URL: "https://chatwoot.exemplo.com", Token: "tok", SignMsg: boolPtr(true)}},
		{"sem token", setRequest{Enabled: true, URL: "https://chatwoot.exemplo.com", AccountID: "1", SignMsg: boolPtr(true)}},
		{"sem signMsg", setRequest{Enabled: true, URL: "https://chatwoot.exemplo.com", AccountID: "1", Token: "tok"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := doRequest(t, r, http.MethodPost, "/api/chatwoot/inst-1", "test-apikey", tc.body)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("esperado 400, obtido %d: %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestSetChatwoot_EnabledProvisionaInbox(t *testing.T) {
	h, st, cw := newTestHandler()
	r := newRouter(h)

	body := setRequest{
		Enabled:   true,
		URL:       "https://chatwoot.exemplo.com",
		AccountID: "1",
		Token:     "tok-123",
		SignMsg:   boolPtr(true),
		NameInbox: "Suporte",
	}

	w := doRequest(t, r, http.MethodPost, "/api/chatwoot/inst-1", "test-apikey", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("esperado 201, obtido %d: %s", w.Code, w.Body.String())
	}

	if cw.ensureCalls != 1 {
		t.Fatalf("esperado 1 chamada a EnsureInbox, obtido %d", cw.ensureCalls)
	}

	var resp findResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("json inválido: %v", err)
	}
	if resp.InboxID != 42 {
		t.Fatalf("esperado inboxId=42, obtido %d", resp.InboxID)
	}
	wantWebhook := "https://evoconnector.iceasa.com.br/chatwoot/webhook/inst-1"
	if resp.WebhookURL != wantWebhook {
		t.Fatalf("webhook_url esperado %q, obtido %q", wantWebhook, resp.WebhookURL)
	}

	saved, err := st.GetConfig(context.Background(), "inst-1")
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if saved == nil || saved.InboxID != 42 {
		t.Fatalf("esperado InboxID=42 persistido, obtido %+v", saved)
	}

	// GET /api/instances deve listar a instância habilitada.
	wList := doRequest(t, r, http.MethodGet, "/api/instances", "test-apikey", nil)
	var list []instanceSummary
	if err := json.Unmarshal(wList.Body.Bytes(), &list); err != nil {
		t.Fatalf("json inválido: %v", err)
	}
	if len(list) != 1 || list[0].InstanceID != "inst-1" {
		t.Fatalf("esperado 1 instância listada = inst-1, obtido %+v", list)
	}
}

func TestSetChatwoot_ProvisionamentoFalhaRetorna422(t *testing.T) {
	h, st, cw := newTestHandler()
	cw.failEnsure = errors.New("chatwoot indisponível")
	r := newRouter(h)

	body := setRequest{
		Enabled:   true,
		URL:       "https://chatwoot.exemplo.com",
		AccountID: "1",
		Token:     "tok-123",
		SignMsg:   boolPtr(true),
	}

	w := doRequest(t, r, http.MethodPost, "/api/chatwoot/inst-2", "test-apikey", body)
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("esperado 422, obtido %d: %s", w.Code, w.Body.String())
	}

	// A config deve continuar salva mesmo com falha no provisionamento
	// (para o operador não perder o que preencheu no formulário).
	saved, err := st.GetConfig(context.Background(), "inst-2")
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if saved == nil {
		t.Fatalf("esperado config salva mesmo com falha de provisionamento")
	}
}

func TestSetChatwoot_DisabledNaoProvisiona(t *testing.T) {
	h, st, cw := newTestHandler()
	r := newRouter(h)

	body := setRequest{Enabled: false, NameInbox: "Qualquer"}
	w := doRequest(t, r, http.MethodPost, "/api/chatwoot/inst-3", "test-apikey", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("esperado 201, obtido %d: %s", w.Code, w.Body.String())
	}
	if cw.ensureCalls != 0 {
		t.Fatalf("esperado 0 chamadas a EnsureInbox quando enabled=false, obtido %d", cw.ensureCalls)
	}

	saved, _ := st.GetConfig(context.Background(), "inst-3")
	if saved == nil || saved.Enabled {
		t.Fatalf("esperado config salva com enabled=false, obtido %+v", saved)
	}
}

func TestDeleteChatwoot(t *testing.T) {
	h, st, _ := newTestHandler()
	r := newRouter(h)

	_ = st.SaveConfig(context.Background(), &model.ChatwootConfig{InstanceID: "inst-4", Enabled: false})

	w := doRequest(t, r, http.MethodDelete, "/api/chatwoot/inst-4", "test-apikey", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("esperado 200, obtido %d: %s", w.Code, w.Body.String())
	}

	got, err := st.GetConfig(context.Background(), "inst-4")
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if got != nil {
		t.Fatalf("esperado config removida, obtido %+v", got)
	}
}

func boolPtr(b bool) *bool { return &b }
