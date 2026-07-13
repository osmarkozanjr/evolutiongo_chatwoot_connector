// Testes de parsing do payload do webhook e das funções puras de montagem.
// Payloads de exemplo seguem o formato consumido por receiveWebhook() no
// serviço antigo (body.event, body.conversation.meta.sender, messages[], etc).
package egress

import (
	"encoding/json"
	"testing"
)

const sampleMessageCreated = `{
  "event": "message_created",
  "id": 4567,
  "content": "Olá **cliente**, *tudo bem*?",
  "message_type": "outgoing",
  "private": false,
  "source_id": null,
  "inbox": {"id": 12, "name": "wa-inbox"},
  "sender": {"name": "Agente X", "available_name": "Agente X"},
  "conversation": {
    "id": 99,
    "contact_inbox": {"source_id": "abc-123"},
    "meta": {"sender": {"identifier": "5582988887777@s.whatsapp.net", "phone_number": "+5582988887777"}},
    "messages": [
      {
        "id": 4567,
        "content": "Olá **cliente**, *tudo bem*?",
        "source_id": null,
        "sender": {"available_name": "Agente X"},
        "attachments": []
      }
    ]
  }
}`

func TestWebhookPayloadParsing(t *testing.T) {
	var p WebhookPayload
	if err := json.Unmarshal([]byte(sampleMessageCreated), &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if p.Event != "message_created" {
		t.Errorf("event = %q", p.Event)
	}
	if p.ID != 4567 {
		t.Errorf("id = %d", p.ID)
	}
	if p.MessageType != "outgoing" {
		t.Errorf("message_type = %q", p.MessageType)
	}
	if p.Private {
		t.Error("private deveria ser false")
	}
	if p.Conversation == nil || p.Conversation.ID != 99 {
		t.Fatalf("conversation = %+v", p.Conversation)
	}
	if got := p.chatID(); got != "5582988887777@s.whatsapp.net" {
		t.Errorf("chatID = %q", got)
	}
	if got := p.senderName(); got != "Agente X" {
		t.Errorf("senderName = %q", got)
	}
	if p.Conversation.ContactInbox == nil || p.Conversation.ContactInbox.SourceID != "abc-123" {
		t.Errorf("contact_inbox = %+v", p.Conversation.ContactInbox)
	}
}

func TestChatIDFallsBackToPhoneNumber(t *testing.T) {
	p := WebhookPayload{
		Conversation: &ConversationRef{
			Meta: &Meta{Sender: &MetaSender{PhoneNumber: "+5582988887777"}},
		},
	}
	if got := p.chatID(); got != "5582988887777" {
		t.Errorf("chatID = %q, esperado 5582988887777", got)
	}
}

func TestStatusChangedPayloadParsing(t *testing.T) {
	// conversation_status_changed não tem body.conversation; o identifier vem
	// em body.meta.sender.identifier (checado antes do early-return no antigo).
	raw := `{
	  "event": "conversation_status_changed",
	  "status": "resolved",
	  "meta": {"sender": {"identifier": "5582911112222@s.whatsapp.net"}}
	}`
	var p WebhookPayload
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if p.Status != "resolved" {
		t.Errorf("status = %q", p.Status)
	}
	if got := p.metaSenderIdentifier(); got != "5582911112222@s.whatsapp.net" {
		t.Errorf("metaSenderIdentifier = %q", got)
	}
}

func TestConvertMarkdown(t *testing.T) {
	cases := []struct{ in, want string }{
		{"*negrito*", "_negrito_"},
		{"**negrito**", "*negrito*"},
		{"~~riscado~~", "~riscado~"},
		{"`code`", "```code```"},
		{"**b** e *i*", "*b* e _i_"},
		{"sem formatação", "sem formatação"},
		{"", ""},
		{"```bloco```", "```bloco```"},
	}
	for _, c := range cases {
		if got := convertMarkdown(c.in); got != c.want {
			t.Errorf("convertMarkdown(%q) = %q, esperado %q", c.in, got, c.want)
		}
	}
}

func TestFormatOutgoingText(t *testing.T) {
	// signMsg=true com delimiter literal "\n" (como armazenado no config).
	got := formatOutgoingText("Oi", "Agente", true, `\n`)
	if got != "*Agente:*\nOi" {
		t.Errorf("assinado = %q", got)
	}

	// signMsg=false: sem assinatura mesmo com senderName.
	got = formatOutgoingText("Oi", "Agente", false, `\n`)
	if got != "Oi" {
		t.Errorf("sem assinatura = %q", got)
	}

	// senderName vazio: só a conversão de markdown.
	got = formatOutgoingText("**Oi**", "", true, `\n`)
	if got != "*Oi*" {
		t.Errorf("sem sender = %q", got)
	}

	// Delimiter customizado.
	got = formatOutgoingText("Oi", "Agente", true, " - ")
	if got != "*Agente:* - Oi" {
		t.Errorf("delimiter custom = %q", got)
	}
}

func TestDeduceAttachment(t *testing.T) {
	cases := []struct {
		url      string
		wantType string
		wantName string
	}{
		{"https://cw.example.com/blobs/x/foto.jpg", "image", "foto.jpg"},
		{"https://cw.example.com/blobs/x/video.mp4", "video", "video.mp4"},
		{"https://cw.example.com/blobs/x/audio.ogg", "audio", "audio.ogg"},
		{"https://cw.example.com/blobs/x/doc.pdf", "document", "doc.pdf"},
		// Extensões de imagem forçadas a documento (lista de sendAttachment).
		{"https://cw.example.com/blobs/x/anim.gif", "document", "anim.gif"},
		// Sem mime conhecido → document.
		{"https://cw.example.com/blobs/x/file.bin", "document", "file.bin"},
		// Nome URL-encoded e query string.
		{"https://cw.example.com/blobs/x/foto%20praia.jpg?token=1", "image", "foto praia.jpg"},
	}
	for _, c := range cases {
		gotType, gotName := deduceAttachment(c.url)
		if gotType != c.wantType || gotName != c.wantName {
			t.Errorf("deduceAttachment(%q) = (%q, %q), esperado (%q, %q)",
				c.url, gotType, gotName, c.wantType, c.wantName)
		}
	}
}

func TestNormalizeTemplateText(t *testing.T) {
	// O regex original (/\\\r\n|\\\n|\n/) casa barra+CRLF, barra+LF ou LF.
	in := "linha1\\\r\nlinha2\\\nlinha3\nlinha4"
	if got := normalizeTemplateText(in); got != "linha1\nlinha2\nlinha3\nlinha4" {
		t.Errorf("normalizeTemplateText = %q", got)
	}
}
