// Testes do fluxo do Service (HandleWebhook) com fakes em memória — sem rede.
package egress

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/iceasa/evolution-chatwoot-connector/internal/chatwoot"
	"github.com/iceasa/evolution-chatwoot-connector/internal/evolution"
	"github.com/iceasa/evolution-chatwoot-connector/internal/model"
)

// --- Fakes -----------------------------------------------------------------

type fakeStore struct {
	cfg          *model.ChatwootConfig
	msgsByCwID   map[int]*model.MessageMapping
	savedMsgs    []*model.MessageMapping
	deletedConvs []string
}

func (f *fakeStore) GetConfig(ctx context.Context, instanceID string) (*model.ChatwootConfig, error) {
	return f.cfg, nil
}
func (f *fakeStore) SaveConfig(ctx context.Context, cfg *model.ChatwootConfig) error { return nil }
func (f *fakeStore) DeleteConfig(ctx context.Context, instanceID string) error       { return nil }
func (f *fakeStore) ListEnabledConfigs(ctx context.Context) ([]*model.ChatwootConfig, error) {
	return nil, nil
}
func (f *fakeStore) ListConfigs(ctx context.Context) ([]*model.ChatwootConfig, error) {
	return nil, nil
}
func (f *fakeStore) GetContact(ctx context.Context, instanceID, remoteJid string) (*model.ContactMapping, error) {
	return nil, nil
}
func (f *fakeStore) SaveContact(ctx context.Context, m *model.ContactMapping) error { return nil }
func (f *fakeStore) GetConversation(ctx context.Context, instanceID, remoteJid string) (*model.ConversationMapping, error) {
	return nil, nil
}
func (f *fakeStore) GetConversationByChatwootID(ctx context.Context, instanceID string, cwConversationID int) (*model.ConversationMapping, error) {
	return nil, nil
}
func (f *fakeStore) SaveConversation(ctx context.Context, m *model.ConversationMapping) error {
	return nil
}
func (f *fakeStore) DeleteConversation(ctx context.Context, instanceID, remoteJid string) error {
	f.deletedConvs = append(f.deletedConvs, remoteJid)
	return nil
}
func (f *fakeStore) GetMessageByWhatsappID(ctx context.Context, instanceID, waMessageID string) (*model.MessageMapping, error) {
	return nil, nil
}
func (f *fakeStore) GetMessageByChatwootID(ctx context.Context, instanceID string, cwMessageID int) (*model.MessageMapping, error) {
	if f.msgsByCwID == nil {
		return nil, nil
	}
	return f.msgsByCwID[cwMessageID], nil
}
func (f *fakeStore) SaveMessage(ctx context.Context, m *model.MessageMapping) error {
	f.savedMsgs = append(f.savedMsgs, m)
	return nil
}
func (f *fakeStore) Migrate(ctx context.Context) error { return nil }
func (f *fakeStore) Close()                            {}

type sentText struct {
	token string
	msg   *evolution.TextMessage
}

type sentMedia struct {
	token string
	msg   *evolution.MediaMessage
}

type fakeEvo struct {
	texts   []sentText
	medias  []sentMedia
	textErr error
	nextID  string
}

func (f *fakeEvo) SendText(ctx context.Context, token string, msg *evolution.TextMessage) (*evolution.SendResult, error) {
	if f.textErr != nil {
		return nil, f.textErr
	}
	f.texts = append(f.texts, sentText{token, msg})
	return &evolution.SendResult{MessageID: f.nextID}, nil
}
func (f *fakeEvo) SendMedia(ctx context.Context, token string, msg *evolution.MediaMessage) (*evolution.SendResult, error) {
	f.medias = append(f.medias, sentMedia{token, msg})
	return &evolution.SendResult{MessageID: f.nextID}, nil
}
func (f *fakeEvo) FetchProfilePicture(ctx context.Context, token, number string) (string, error) {
	return "", nil
}
func (f *fakeEvo) CheckNumber(ctx context.Context, token, number string) (string, bool, error) {
	return number, true, nil
}

type createdMessage struct {
	conversationID int
	content        string
	messageType    string
	sourceID       string
}

type fakeCw struct {
	created []createdMessage
}

func (f *fakeCw) FindInboxByName(ctx context.Context, cfg *model.ChatwootConfig, name string) (int, error) {
	return 0, nil
}
func (f *fakeCw) CreateInbox(ctx context.Context, cfg *model.ChatwootConfig, name, webhookURL string) (int, error) {
	return 0, nil
}
func (f *fakeCw) UpdateInboxWebhook(ctx context.Context, cfg *model.ChatwootConfig, inboxID int, webhookURL string) error {
	return nil
}
func (f *fakeCw) SearchContact(ctx context.Context, cfg *model.ChatwootConfig, q string) (*chatwoot.Contact, error) {
	return nil, nil
}
func (f *fakeCw) CreateContact(ctx context.Context, cfg *model.ChatwootConfig, inboxID int, phone, name, avatarURL, identifier string) (*chatwoot.Contact, error) {
	return nil, nil
}
func (f *fakeCw) UpdateContact(ctx context.Context, cfg *model.ChatwootConfig, contactID int, fields map[string]any) error {
	return nil
}
func (f *fakeCw) MergeContacts(ctx context.Context, cfg *model.ChatwootConfig, baseID, mergeID int) error {
	return nil
}
func (f *fakeCw) GetOpenConversation(ctx context.Context, cfg *model.ChatwootConfig, contactID, inboxID int) (*chatwoot.Conversation, error) {
	return nil, nil
}
func (f *fakeCw) CreateConversation(ctx context.Context, cfg *model.ChatwootConfig, contactID, inboxID int, status string) (*chatwoot.Conversation, error) {
	return nil, nil
}
func (f *fakeCw) ToggleConversationStatus(ctx context.Context, cfg *model.ChatwootConfig, conversationID int, status string) error {
	return nil
}
func (f *fakeCw) CreateMessage(ctx context.Context, cfg *model.ChatwootConfig, conversationID int, content, messageType string, attachments []chatwoot.Attachment, sourceID string) (*chatwoot.Message, error) {
	f.created = append(f.created, createdMessage{conversationID, content, messageType, sourceID})
	return &chatwoot.Message{ID: 1, Content: content, MessageType: messageType, ConversationID: conversationID}, nil
}
func (f *fakeCw) DeleteMessage(ctx context.Context, cfg *model.ChatwootConfig, conversationID, messageID int) error {
	return nil
}

// --- Helpers ------------------------------------------------------------------

func enabledConfig() *model.ChatwootConfig {
	return &model.ChatwootConfig{
		InstanceID:    "inst-1",
		Enabled:       true,
		URL:           "https://cw.example.com",
		AccountID:     "1",
		Token:         "tok",
		NameInbox:     "wa-inbox",
		SignMsg:       true,
		SignDelimiter: `\n`,
	}
}

func newTestService(st *fakeStore, cw *fakeCw, evo *fakeEvo) *Service {
	return NewService(st, cw, evo, func(ctx context.Context, instanceID string) (string, error) {
		return "instance-token", nil
	}, nil)
}

func outgoingPayload() *WebhookPayload {
	var p WebhookPayload
	if err := json.Unmarshal([]byte(sampleMessageCreated), &p); err != nil {
		panic(err)
	}
	return &p
}

// --- Testes ---------------------------------------------------------------------

func TestHandleWebhookOutgoingTextSendsAndMaps(t *testing.T) {
	st := &fakeStore{cfg: enabledConfig()}
	evo := &fakeEvo{nextID: "3EB0WAID123"}
	cw := &fakeCw{}
	svc := newTestService(st, cw, evo)

	p := outgoingPayload()
	if err := svc.HandleWebhook(context.Background(), "inst-1", p); err != nil {
		t.Fatalf("HandleWebhook: %v", err)
	}

	if len(evo.texts) != 1 {
		t.Fatalf("esperado 1 texto enviado, houve %d", len(evo.texts))
	}
	sent := evo.texts[0]
	if sent.token != "instance-token" {
		t.Errorf("token = %q", sent.token)
	}
	if sent.msg.Number != "5582988887777@s.whatsapp.net" {
		t.Errorf("number = %q", sent.msg.Number)
	}
	// signMsg=true: assinatura + markdown convertido (** -> *, * -> _).
	want := "*Agente X:*\nOlá *cliente*, _tudo bem_?"
	if sent.msg.Text != want {
		t.Errorf("text = %q, esperado %q", sent.msg.Text, want)
	}

	if len(st.savedMsgs) != 1 {
		t.Fatalf("esperado 1 mapping salvo, houve %d", len(st.savedMsgs))
	}
	m := st.savedMsgs[0]
	if m.Direction != "out" || m.WhatsappMessageID != "3EB0WAID123" || m.ChatwootMessageID != 4567 {
		t.Errorf("mapping = %+v", m)
	}
}

func TestHandleWebhookSkipsEchoBySourceID(t *testing.T) {
	st := &fakeStore{cfg: enabledConfig()}
	evo := &fakeEvo{nextID: "id"}
	svc := newTestService(st, &fakeCw{}, evo)

	p := outgoingPayload()
	p.Conversation.Messages[0].SourceID = "WAID:3EB0ABCDEF"

	if err := svc.HandleWebhook(context.Background(), "inst-1", p); err != nil {
		t.Fatalf("HandleWebhook: %v", err)
	}
	if len(evo.texts) != 0 {
		t.Errorf("echo WAID: não deveria enviar; enviou %d", len(evo.texts))
	}
}

func TestHandleWebhookSkipsAlreadyMappedMessage(t *testing.T) {
	st := &fakeStore{
		cfg: enabledConfig(),
		msgsByCwID: map[int]*model.MessageMapping{
			4567: {InstanceID: "inst-1", ChatwootMessageID: 4567, Direction: "in"},
		},
	}
	evo := &fakeEvo{nextID: "id"}
	svc := newTestService(st, &fakeCw{}, evo)

	if err := svc.HandleWebhook(context.Background(), "inst-1", outgoingPayload()); err != nil {
		t.Fatalf("HandleWebhook: %v", err)
	}
	if len(evo.texts) != 0 {
		t.Errorf("mensagem já mapeada não deveria reenviar; enviou %d", len(evo.texts))
	}
}

func TestHandleWebhookSkipsPrivateNotes(t *testing.T) {
	st := &fakeStore{cfg: enabledConfig()}
	evo := &fakeEvo{nextID: "id"}
	svc := newTestService(st, &fakeCw{}, evo)

	p := outgoingPayload()
	p.Private = true

	if err := svc.HandleWebhook(context.Background(), "inst-1", p); err != nil {
		t.Fatalf("HandleWebhook: %v", err)
	}
	if len(evo.texts) != 0 {
		t.Errorf("private note não deveria enviar; enviou %d", len(evo.texts))
	}
}

func TestHandleWebhookDisabledConfigIgnored(t *testing.T) {
	cfg := enabledConfig()
	cfg.Enabled = false
	st := &fakeStore{cfg: cfg}
	evo := &fakeEvo{nextID: "id"}
	svc := newTestService(st, &fakeCw{}, evo)

	if err := svc.HandleWebhook(context.Background(), "inst-1", outgoingPayload()); err != nil {
		t.Fatalf("HandleWebhook: %v", err)
	}
	if len(evo.texts) != 0 {
		t.Errorf("config desabilitada não deveria enviar; enviou %d", len(evo.texts))
	}
}

func TestHandleWebhookAttachmentsSendMedia(t *testing.T) {
	st := &fakeStore{cfg: enabledConfig()}
	evo := &fakeEvo{nextID: "3EB0MEDIA1"}
	svc := newTestService(st, &fakeCw{}, evo)

	p := outgoingPayload()
	p.Content = "legenda"
	p.Conversation.Messages[0].Content = "legenda"
	p.Conversation.Messages[0].Attachments = []AttachmentRef{
		{ID: 1, DataURL: "https://cw.example.com/blobs/x/foto.jpg", FileType: "image"},
		{ID: 2, DataURL: "https://cw.example.com/blobs/x/doc.pdf", FileType: "file"},
	}

	if err := svc.HandleWebhook(context.Background(), "inst-1", p); err != nil {
		t.Fatalf("HandleWebhook: %v", err)
	}

	if len(evo.medias) != 2 {
		t.Fatalf("esperado 2 mídias, houve %d", len(evo.medias))
	}
	if len(evo.texts) != 0 {
		t.Errorf("com attachments não envia texto separado; enviou %d", len(evo.texts))
	}

	img := evo.medias[0].msg
	if img.Type != "image" || img.Filename != "foto.jpg" || img.URL != "https://cw.example.com/blobs/x/foto.jpg" {
		t.Errorf("media[0] = %+v", img)
	}
	if !strings.Contains(img.Caption, "legenda") || !strings.Contains(img.Caption, "*Agente X:*") {
		t.Errorf("caption = %q", img.Caption)
	}

	doc := evo.medias[1].msg
	if doc.Type != "document" || doc.Filename != "doc.pdf" {
		t.Errorf("media[1] = %+v", doc)
	}

	if len(st.savedMsgs) != 2 {
		t.Errorf("esperado 2 mappings, houve %d", len(st.savedMsgs))
	}
}

func TestHandleWebhookAttachmentWithoutContentHasNoCaption(t *testing.T) {
	st := &fakeStore{cfg: enabledConfig()}
	evo := &fakeEvo{nextID: "id"}
	svc := newTestService(st, &fakeCw{}, evo)

	p := outgoingPayload()
	p.Content = ""
	p.Conversation.Messages[0].Content = ""
	p.Conversation.Messages[0].Attachments = []AttachmentRef{
		{ID: 1, DataURL: "https://cw.example.com/blobs/x/foto.jpg"},
	}

	if err := svc.HandleWebhook(context.Background(), "inst-1", p); err != nil {
		t.Fatalf("HandleWebhook: %v", err)
	}
	if len(evo.medias) != 1 || evo.medias[0].msg.Caption != "" {
		t.Errorf("caption deveria ser vazio: %+v", evo.medias)
	}
}

func TestHandleWebhookSendErrorCreatesErrorMessage(t *testing.T) {
	st := &fakeStore{cfg: enabledConfig()}
	evo := &fakeEvo{textErr: errors.New("boom")}
	cw := &fakeCw{}
	svc := newTestService(st, cw, evo)

	err := svc.HandleWebhook(context.Background(), "inst-1", outgoingPayload())
	if err == nil {
		t.Fatal("esperado erro de envio")
	}

	if len(cw.created) != 1 {
		t.Fatalf("esperado 1 mensagem de erro no chatwoot, houve %d", len(cw.created))
	}
	created := cw.created[0]
	if created.conversationID != 99 {
		t.Errorf("conversationID = %d", created.conversationID)
	}
	if !strings.Contains(created.content, msgNotSentPrefix) {
		t.Errorf("content = %q", created.content)
	}
	if !strings.HasPrefix(created.sourceID, "WAID:") {
		t.Errorf("sourceID de erro deve levar prefixo WAID: (anti-loop), veio %q", created.sourceID)
	}
}

func TestHandleWebhookNumberNotInWhatsapp(t *testing.T) {
	st := &fakeStore{cfg: enabledConfig()}
	evo := &fakeEvo{textErr: errors.New("number 5582988887777 is not registered on WhatsApp")}
	cw := &fakeCw{}
	svc := newTestService(st, cw, evo)

	_ = svc.HandleWebhook(context.Background(), "inst-1", outgoingPayload())

	if len(cw.created) != 1 {
		t.Fatalf("esperado 1 mensagem de erro, houve %d", len(cw.created))
	}
	if cw.created[0].content != msgNumberNotInWhatsapp {
		t.Errorf("content = %q", cw.created[0].content)
	}
}

func TestHandleWebhookStatusResolvedDeletesMapping(t *testing.T) {
	cfg := enabledConfig()
	cfg.ReopenConversation = false
	st := &fakeStore{cfg: cfg}
	svc := newTestService(st, &fakeCw{}, &fakeEvo{})

	p := &WebhookPayload{
		Event:  "conversation_status_changed",
		Status: "resolved",
		Meta:   &Meta{Sender: &MetaSender{Identifier: "5582911112222@s.whatsapp.net"}},
	}
	if err := svc.HandleWebhook(context.Background(), "inst-1", p); err != nil {
		t.Fatalf("HandleWebhook: %v", err)
	}
	if len(st.deletedConvs) != 1 || st.deletedConvs[0] != "5582911112222@s.whatsapp.net" {
		t.Errorf("deletedConvs = %v", st.deletedConvs)
	}

	// Com reopenConversation=true o mapping é preservado.
	cfg2 := enabledConfig()
	cfg2.ReopenConversation = true
	st2 := &fakeStore{cfg: cfg2}
	svc2 := newTestService(st2, &fakeCw{}, &fakeEvo{})
	if err := svc2.HandleWebhook(context.Background(), "inst-1", p); err != nil {
		t.Fatalf("HandleWebhook: %v", err)
	}
	if len(st2.deletedConvs) != 0 {
		t.Errorf("reopenConversation=true não deveria apagar mapping: %v", st2.deletedConvs)
	}
}

func TestHandleWebhookMessageUpdatedWithoutDeleteIgnored(t *testing.T) {
	st := &fakeStore{cfg: enabledConfig()}
	evo := &fakeEvo{nextID: "id"}
	svc := newTestService(st, &fakeCw{}, evo)

	p := outgoingPayload()
	p.Event = "message_updated"
	p.ContentAttributes = &ContentAttributes{Deleted: false}

	if err := svc.HandleWebhook(context.Background(), "inst-1", p); err != nil {
		t.Fatalf("HandleWebhook: %v", err)
	}
	if len(evo.texts) != 0 {
		t.Errorf("message_updated sem deleted não deveria enviar; enviou %d", len(evo.texts))
	}
}

func TestHandlerRespondsBot(t *testing.T) {
	gin.SetMode(gin.TestMode)

	st := &fakeStore{cfg: enabledConfig()}
	evo := &fakeEvo{nextID: "id"}
	svc := newTestService(st, &fakeCw{}, evo)

	r := gin.New()
	r.POST("/chatwoot/webhook/:instanceId", Handler(svc))

	req := httptest.NewRequest(http.MethodPost, "/chatwoot/webhook/inst-1",
		strings.NewReader(sampleMessageCreated))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, corpo: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["message"] != "bot" {
		t.Errorf("resposta = %v, esperado message=bot", resp)
	}
	if len(evo.texts) != 1 {
		t.Errorf("esperado 1 envio via handler, houve %d", len(evo.texts))
	}

	// Payload inválido → 400.
	req = httptest.NewRequest(http.MethodPost, "/chatwoot/webhook/inst-1",
		strings.NewReader("{invalid"))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("payload inválido: status = %d", w.Code)
	}
}
