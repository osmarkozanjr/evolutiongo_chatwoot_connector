// message.go — evento Message/SendMessage (WA→Chatwoot).
//
// Porta do fluxo "messages.upsert"/"send.message" de eventWhatsapp() +
// createConversation() + createContact() + createMessage() do serviço antigo
// (chatwoot.service.ts), adaptado ao payload do evolutiongo.
//
// INTERFACE-CHANGE-REQUEST: chatwoot.Client.CreateMessage não aceita
// content_attributes/source_reply_id; o serviço antigo enviava
// {in_reply_to: <chatwootMessageId>, in_reply_to_external_id: <stanzaId>} para
// exibir reply/quote no Chatwoot. Sugestão: adicionar parâmetro opcional
// (ex. replyTo *ReplyRef{ChatwootMessageID int, ExternalID string}) em
// CreateMessage. Enquanto isso, o quoted é resolvido via
// store.GetMessageByWhatsappID apenas para log (sem efeito visual no Chatwoot).
//
// INTERFACE-CHANGE-REQUEST (opcional): o serviço antigo atualizava o
// "last seen" da conversa no evento messages.read via API pública
// (/public/api/v1/inboxes/{identifier}/contacts/{sourceId}/conversations/{id}/update_last_seen);
// suportar isso exigiria um método novo no chatwoot.Client.
package ingest

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/iceasa/evolution-chatwoot-connector/internal/chatwoot"
	"github.com/iceasa/evolution-chatwoot-connector/internal/model"
)

// waMessage é a visão normalizada do payload de Message/SendMessage.
//
// Formato verificado em whatsmeow.go (myEventHandler, case *events.Message):
//
//	data = {
//	  "Info":    { ...types.MessageInfo... },   // chave "Info" verificada (o código lê dataMap["Message"], irmão de Info)
//	  "Message": { "conversation": "...", "imageMessage": {...}, ... },  // chaves camelCase do protobuf waE2E
//	  "quoted":  { "stanzaID": "...", "quotedMessage": {...} },          // injetado quando há reply
//	  "isQuoted": true,
//	  "groupData": {...},                       // injetado quando chat é grupo (types.GroupInfo)
//	  "referral": {...},
//	}
//
// Mídia (WEBHOOKFILES=true): o evolutiongo injeta dentro de data.Message:
//   - sem MinIO:  "base64": "<conteúdo em base64>"
//   - com MinIO:  "mediaUrl": "https://...", "mimetype": "image/jpeg"
//
// TODO(VERIFY): as chaves internas de "Info" ("ID"/"id", "Chat"/"chat",
// "IsFromMe"/"isFromMe"...) dependem das tags JSON do types.MessageInfo do
// whatsmeow (módulo não disponível no workspace). O swagger do evolutiongo
// sugere camelCase, mas swagger é gerado por introspecção e pode divergir do
// encoding/json real. O parsing abaixo aceita as duas grafias.
type waMessage struct {
	ID        string
	RemoteJid string // chat
	SenderJid string // participante (grupo) ou o próprio contato
	FromMe    bool
	IsGroup   bool
	PushName  string
	Timestamp time.Time

	Message   map[string]any // data.Message (conteúdo waE2E)
	GroupData map[string]any // data.groupData (quando grupo)

	QuotedStanzaID string // data.quoted.stanzaID

	Base64   string // data.Message.base64 (WEBHOOKFILES sem MinIO)
	MediaURL string // data.Message.mediaUrl (WEBHOOKFILES com MinIO)
	MimeType string // data.Message.mimetype (só no modo MinIO)
}

// parseMessageEvent decodifica o data do evento Message/SendMessage.
func parseMessageEvent(data json.RawMessage) (*waMessage, error) {
	var root map[string]any
	if err := json.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("ingest: data de message inválido: %w", err)
	}

	info := getMap(root, "Info", "info")
	msg := getMap(root, "Message", "message")
	if info == nil {
		return nil, fmt.Errorf("ingest: data de message sem campo Info")
	}

	m := &waMessage{
		ID:        getString(info, "ID", "id", "Id"),
		FromMe:    getBool(info, "IsFromMe", "isFromMe"),
		PushName:  getString(info, "PushName", "pushName"),
		Message:   msg,
		GroupData: getMap(root, "groupData", "GroupData"),
	}
	if chat, ok := getAny(info, "Chat", "chat"); ok {
		m.RemoteJid = jidToString(chat)
	}
	if sender, ok := getAny(info, "Sender", "sender"); ok {
		m.SenderJid = jidToString(sender)
	}
	if ts, ok := getAny(info, "Timestamp", "timestamp"); ok {
		m.Timestamp = parseTimestamp(ts)
	}
	m.IsGroup = isGroupJid(m.RemoteJid) || getBool(info, "IsGroup", "isGroup")

	if quoted := getMap(root, "quoted"); quoted != nil {
		m.QuotedStanzaID = getString(quoted, "stanzaID", "stanzaId")
	}
	if msg != nil {
		m.Base64 = getString(msg, "base64")
		m.MediaURL = getString(msg, "mediaUrl")
		m.MimeType = getString(msg, "mimetype")
	}
	return m, nil
}

// handleMessageEvent é o fluxo principal WA→Chatwoot.
func (s *Service) handleMessageEvent(ctx context.Context, cfg *model.ChatwootConfig, ev *model.EventEnvelope) error {
	m, err := parseMessageEvent(ev.Data)
	if err != nil {
		s.log.Warn("ingest: payload de message descartado", "instanceId", ev.InstanceID, "error", err)
		return nil // requeue não resolve payload malformado
	}
	if m.ID == "" || m.RemoteJid == "" {
		s.log.Warn("ingest: message sem id/remoteJid, descartado", "instanceId", ev.InstanceID)
		return nil
	}
	// status@broadcast não vira conversa (comportamento do serviço antigo).
	if strings.HasPrefix(m.RemoteJid, "status@broadcast") {
		return nil
	}
	if shouldIgnoreJid(cfg.IgnoreJids, m.RemoteJid) {
		s.log.Debug("ingest: jid ignorado por ignoreJids", "remoteJid", m.RemoteJid)
		return nil
	}

	// Dedup: mensagem já mapeada (inclui o eco fromMe de mensagens enviadas
	// pelo egress, que salvou o mapping com o id retornado pelo /send/*).
	if existing, err := s.store.GetMessageByWhatsappID(ctx, cfg.InstanceID, m.ID); err != nil {
		return fmt.Errorf("ingest: erro no dedup da mensagem %s: %w", m.ID, err)
	} else if existing != nil {
		return nil
	}

	content := formatWAMarkdown(conversationContent(m.Message))
	hasMedia := isMediaMessage(m.Message)
	reactionText, _ := reactionContent(m.Message)

	if content == "" && !hasMedia && reactionText == "" {
		s.log.Debug("ingest: mensagem sem corpo suportado, ignorada", "id", m.ID)
		return nil
	}
	// Eco de resposta de pesquisa CSAT do próprio Chatwoot (comportamento antigo).
	if strings.Contains(content, "/survey/responses/") && strings.Contains(content, "http") {
		return nil
	}
	if content == "" && reactionText != "" {
		content = reactionText
	}

	conv, err := s.resolveConversation(ctx, cfg, ev, m)
	if err != nil {
		return err
	}
	if conv == nil {
		return nil // sem inbox provisionado etc. — já logado
	}

	// fromMe (enviado pelo celular ou por outro cliente) => outgoing,
	// como no serviço antigo: messageType = body.key.fromMe ? outgoing : incoming.
	messageType := "incoming"
	if m.FromMe {
		messageType = "outgoing"
	}

	// Grupo: prefixa remetente ("**+55 11 9... - Nome:**\n\n<msg>").
	if m.IsGroup && !m.FromMe {
		prefix := fmt.Sprintf("**+%s - %s:**", jidUser(m.SenderJid), m.PushName)
		if content != "" {
			content = prefix + "\n\n" + content
		} else {
			content = prefix
		}
	}

	// Quoted/reply: resolvido para correlação; sem efeito visual até a
	// interface CreateMessage suportar content_attributes (ver
	// INTERFACE-CHANGE-REQUEST no topo do arquivo).
	if m.QuotedStanzaID != "" {
		if quoted, err := s.store.GetMessageByWhatsappID(ctx, cfg.InstanceID, m.QuotedStanzaID); err == nil && quoted != nil {
			s.log.Debug("ingest: reply detectado", "stanzaId", m.QuotedStanzaID, "chatwootMessageId", quoted.ChatwootMessageID)
		}
	}

	var attachments []chatwoot.Attachment
	if hasMedia {
		att, err := s.buildAttachment(ctx, m)
		if err != nil {
			// Igual ao evolutiongo quando o download falha: segue sem mídia.
			s.log.Warn("ingest: falha ao montar anexo, enviando sem mídia", "id", m.ID, "error", err)
		} else if att != nil {
			attachments = append(attachments, *att)
		}
	}

	sourceID := "WAID:" + m.ID
	created, err := s.cw.CreateMessage(ctx, cfg, conv.ChatwootConversationID, content, messageType, attachments, sourceID)
	if err != nil {
		return fmt.Errorf("ingest: erro ao criar mensagem no chatwoot (conv %d): %w", conv.ChatwootConversationID, err)
	}

	mapping := &model.MessageMapping{
		InstanceID:        cfg.InstanceID,
		WhatsappMessageID: m.ID,
		ChatwootMessageID: created.ID,
		Direction:         "in",
		CreatedAt:         time.Now(),
	}
	if err := s.store.SaveMessage(ctx, mapping); err != nil {
		// A mensagem já foi criada no Chatwoot; requeue duplicaria. Só loga.
		s.log.Error("ingest: erro ao salvar mapping de mensagem", "waId", m.ID, "cwId", created.ID, "error", err)
	}
	return nil
}

// resolveConversation garante contato + conversa no Chatwoot para o chat,
// respeitando reopenConversation e conversationPending, com lock por
// instância+remoteJid (porta do lock Redis de createConversation).
func (s *Service) resolveConversation(ctx context.Context, cfg *model.ChatwootConfig, ev *model.EventEnvelope, m *waMessage) (*model.ConversationMapping, error) {
	if cfg.InboxID == 0 {
		s.log.Warn("ingest: config sem inboxId (provisionamento pendente?)", "instanceId", cfg.InstanceID)
		return nil, nil
	}

	// Lock igual ao antigo: "{instance}:lock:createConversation-{remoteJid}",
	// TTL 30s, espera de até 5s com polling.
	lockKey := fmt.Sprintf("%s:lock:createConversation-%s", cfg.InstanceID, m.RemoteJid)
	release := s.acquireLockWithWait(ctx, lockKey, 30*time.Second, 5*time.Second)
	if release != nil {
		defer release()
	}

	// Mapping local (cache persistente equivalente ao cacheKey do antigo).
	if mp, err := s.store.GetConversation(ctx, cfg.InstanceID, m.RemoteJid); err != nil {
		return nil, fmt.Errorf("ingest: erro ao buscar conversa mapeada: %w", err)
	} else if mp != nil {
		if cfg.ReopenConversation {
			// Reutiliza sempre a mesma conversa; se conversationPending e ela
			// não está aberta, volta para pending (comportamento antigo).
			if cfg.ConversationPending && mp.Status != "open" {
				if err := s.cw.ToggleConversationStatus(ctx, cfg, mp.ChatwootConversationID, "pending"); err != nil {
					s.log.Warn("ingest: falha ao mover conversa para pending", "conversationId", mp.ChatwootConversationID, "error", err)
				} else {
					mp.Status = "pending"
					mp.UpdatedAt = time.Now()
					_ = s.store.SaveConversation(ctx, mp)
				}
			}
			return mp, nil
		}
		// reopenConversation=false: só reutiliza se não estiver resolvida.
		if mp.Status != "resolved" {
			return mp, nil
		}
		// Resolvida => cria conversa nova abaixo.
	}

	contact, err := s.resolveContact(ctx, cfg, ev, m)
	if err != nil {
		return nil, err
	}
	if contact == nil {
		return nil, nil
	}

	// Conversa aberta existente no inbox (fallback quando o mapping local não
	// existe — ex. conversa criada antes do conector).
	conv, err := s.cw.GetOpenConversation(ctx, cfg, contact.ChatwootContactID, cfg.InboxID)
	if err != nil {
		return nil, fmt.Errorf("ingest: erro ao buscar conversa aberta: %w", err)
	}
	if conv == nil {
		status := "open"
		if cfg.ConversationPending {
			status = "pending"
		}
		conv, err = s.cw.CreateConversation(ctx, cfg, contact.ChatwootContactID, cfg.InboxID, status)
		if err != nil {
			return nil, fmt.Errorf("ingest: erro ao criar conversa: %w", err)
		}
	}

	inboxID := conv.InboxID
	if inboxID == 0 {
		inboxID = cfg.InboxID
	}
	status := conv.Status
	if status == "" {
		status = "open"
	}
	mp := &model.ConversationMapping{
		InstanceID:             cfg.InstanceID,
		RemoteJid:              m.RemoteJid,
		ChatwootConversationID: conv.ID,
		InboxID:                inboxID,
		Status:                 status,
		UpdatedAt:              time.Now(),
	}
	if err := s.store.SaveConversation(ctx, mp); err != nil {
		return nil, fmt.Errorf("ingest: erro ao salvar mapping de conversa: %w", err)
	}
	return mp, nil
}

// acquireLockWithWait tenta adquirir o lock por até maxWait, com polling —
// porta do "while (await this.cache.has(lockKey))" do serviço antigo. Se não
// conseguir (timeout ou erro), retorna nil e o fluxo segue sem lock (mesma
// degradação do antigo após o timeout).
func (s *Service) acquireLockWithWait(ctx context.Context, key string, ttl, maxWait time.Duration) func() {
	deadline := time.Now().Add(maxWait)
	for {
		ok, release, err := s.locker.AcquireLock(ctx, key, ttl)
		if err != nil {
			s.log.Warn("ingest: erro no lock, seguindo sem lock", "key", key, "error", err)
			return nil
		}
		if ok {
			return release
		}
		if time.Now().After(deadline) {
			s.log.Warn("ingest: timeout aguardando lock", "key", key)
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// resolveContact garante o contato do chat no Chatwoot (com cache no store).
// Porta de findContact/createContact/mergeBrazilianContacts.
func (s *Service) resolveContact(ctx context.Context, cfg *model.ChatwootConfig, ev *model.EventEnvelope, m *waMessage) (*model.ContactMapping, error) {
	if cm, err := s.store.GetContact(ctx, cfg.InstanceID, m.RemoteJid); err != nil {
		return nil, fmt.Errorf("ingest: erro ao buscar contato mapeado: %w", err)
	} else if cm != nil {
		return cm, nil
	}

	isGroup := m.IsGroup
	phone := jidUser(m.RemoteJid)

	// Nome: pushName quando a mensagem é do contato; número quando fromMe
	// (porta de nameContact). Grupo: "<subject> (GROUP)" usando o groupData
	// injetado pelo evolutiongo no evento.
	name := m.PushName
	if m.FromMe || name == "" {
		name = phone
	}
	identifier := m.RemoteJid
	if isGroup {
		// TODO(VERIFY): chaves de data.groupData = types.GroupInfo do
		// whatsmeow marshalado (GroupName embutido). Aceitamos as grafias
		// prováveis; se nenhuma existir, cai no próprio JID.
		if subject := getString(m.GroupData, "Name", "name", "Subject", "subject"); subject != "" {
			name = subject + " (GROUP)"
		} else {
			name = m.RemoteJid + " (GROUP)"
		}
	}

	var found *chatwoot.Contact
	var err error
	if isGroup {
		// Grupos: busca por identifier = JID do grupo (como no antigo).
		found, err = s.cw.SearchContact(ctx, cfg, m.RemoteJid)
	} else {
		found, err = s.searchContactWithBrazilVariants(ctx, cfg, phone)
		// Fallback: se o telefone não achou (variantes BR, phone_number
		// divergente), busca pelo identifier = JID, que é a chave estável do
		// contato no Chatwoot. Evita tentar criar um contato que já existe.
		if err == nil && found == nil {
			found, err = s.cw.SearchContact(ctx, cfg, identifier)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("ingest: erro ao buscar contato no chatwoot: %w", err)
	}

	if found == nil {
		avatarURL := ""
		if !isGroup && s.evo != nil {
			// Best-effort: avatar via POST /user/avatar do evolutiongo.
			if url, err := s.evo.FetchProfilePicture(ctx, ev.InstanceToken, phone); err == nil {
				avatarURL = url
			}
		}
		phoneE164 := ""
		if !isGroup {
			phoneE164 = "+" + phone
		}
		if isGroup {
			identifier = m.RemoteJid
		}
		found, err = s.cw.CreateContact(ctx, cfg, cfg.InboxID, phoneE164, name, avatarURL, identifier)
		if err != nil {
			return nil, fmt.Errorf("ingest: erro ao criar contato no chatwoot: %w", err)
		}
		if found == nil {
			s.log.Warn("ingest: contato não criado", "remoteJid", m.RemoteJid)
			return nil, nil
		}
	}

	cm := &model.ContactMapping{
		InstanceID:        cfg.InstanceID,
		RemoteJid:         m.RemoteJid,
		ChatwootContactID: found.ID,
		Identifier:        identifier,
		UpdatedAt:         time.Now(),
	}
	if err := s.store.SaveContact(ctx, cm); err != nil {
		return nil, fmt.Errorf("ingest: erro ao salvar mapping de contato: %w", err)
	}
	return cm, nil
}

// searchContactWithBrazilVariants busca o contato pelo telefone e, para
// números BR, também pela variante com/sem o nono dígito. Se as duas variantes
// existirem como contatos distintos e mergeBrazilContacts estiver habilitado,
// faz o merge (base = número de 13 dígitos/phone_number len 14 com "+", mergee
// = o de 12 dígitos — porta de mergeBrazilianContacts).
func (s *Service) searchContactWithBrazilVariants(ctx context.Context, cfg *model.ChatwootConfig, phone string) (*chatwoot.Contact, error) {
	primary, err := s.cw.SearchContact(ctx, cfg, "+"+phone)
	if err != nil {
		return nil, err
	}

	variants := brazilPhoneVariants("+" + phone)
	if len(variants) == 0 {
		return primary, nil
	}

	alt, err := s.cw.SearchContact(ctx, cfg, variants[0])
	if err != nil {
		// Busca da variante é best-effort.
		s.log.Warn("ingest: erro buscando variante BR do contato", "variant", variants[0], "error", err)
		return primary, nil
	}

	if primary != nil && alt != nil && primary.ID != alt.ID && cfg.MergeBrazilContacts {
		base, mergee := primary, alt
		// Antigo: base = contato cujo phone_number tem 14 chars ("+55" + 11
		// dígitos, com o nono dígito).
		if len(alt.PhoneNumber) == 14 && len(primary.PhoneNumber) != 14 {
			base, mergee = alt, primary
		}
		if err := s.cw.MergeContacts(ctx, cfg, base.ID, mergee.ID); err != nil {
			s.log.Warn("ingest: falha no merge de contatos BR", "base", base.ID, "mergee", mergee.ID, "error", err)
			return primary, nil
		}
		return base, nil
	}
	if primary != nil {
		return primary, nil
	}
	return alt, nil
}

// brazilPhoneVariants porta getNumbers(): para "+55..." com 14 chars devolve a
// forma sem o nono dígito; com 13 chars devolve a forma com o nono dígito.
func brazilPhoneVariants(query string) []string {
	if !strings.HasPrefix(query, "+55") {
		return nil
	}
	switch len(query) {
	case 14: // +55 + DDD + 9 dígitos
		return []string{query[:5] + query[6:]}
	case 13: // +55 + DDD + 8 dígitos
		return []string{query[:5] + "9" + query[5:]}
	default:
		return nil
	}
}

// ---------------------------------------------------------------------------
// Conteúdo da mensagem (porta de getTypeMessage/getMessageContent).
// ---------------------------------------------------------------------------

// conversationContent extrai o texto exibível de um waE2E.Message
// (chaves camelCase do protobuf, verificadas no whatsmeow.go que manipula
// "imageMessage"/"videoMessage"/"documentMessage" dentro de data.Message).
func conversationContent(msg map[string]any) string {
	if msg == nil {
		return ""
	}
	if v := getString(msg, "conversation"); v != "" {
		return v
	}
	if im := getMap(msg, "imageMessage"); im != nil {
		return getString(im, "caption")
	}
	if vm := getMap(msg, "videoMessage"); vm != nil {
		return getString(vm, "caption")
	}
	if et := getMap(msg, "extendedTextMessage"); et != nil {
		return getString(et, "text")
	}
	if dm := getMap(msg, "documentMessage"); dm != nil {
		return getString(dm, "caption")
	}
	if dwc := getMap(msg, "documentWithCaptionMessage"); dwc != nil {
		if inner := getMap(getMap(dwc, "message"), "documentMessage"); inner != nil {
			return getString(inner, "caption")
		}
		return ""
	}
	if am := getMap(msg, "audioMessage"); am != nil {
		return getString(am, "caption") // normalmente vazio; o anexo carrega o áudio
	}
	if cm := getMap(msg, "contactMessage"); cm != nil {
		return formatVCard(getString(cm, "vcard"))
	}
	if lm := getMap(msg, "locationMessage"); lm != nil {
		return formatLocation(lm)
	}
	if llm := getMap(msg, "liveLocationMessage"); llm != nil {
		return formatLocation(llm)
	}
	if lr := getMap(msg, "listResponseMessage"); lr != nil {
		return getString(lr, "title")
	}
	if br := getMap(msg, "buttonsResponseMessage"); br != nil {
		return getString(br, "selectedDisplayText")
	}
	// stickerMessage e demais tipos de mídia sem caption => "".
	return ""
}

// reactionContent devolve (emoji, stanzaId alvo) de um reactionMessage.
func reactionContent(msg map[string]any) (string, string) {
	rm := getMap(msg, "reactionMessage")
	if rm == nil {
		return "", ""
	}
	target := ""
	if key := getMap(rm, "key"); key != nil {
		target = getString(key, "id", "ID")
	}
	return getString(rm, "text"), target
}

// isMediaMessage porta a lista de tipos de mídia do serviço antigo.
func isMediaMessage(msg map[string]any) bool {
	if msg == nil {
		return false
	}
	for _, k := range []string{
		"imageMessage", "documentMessage", "documentWithCaptionMessage",
		"audioMessage", "videoMessage", "stickerMessage", "viewOnceMessageV2",
	} {
		if _, ok := msg[k]; ok {
			return true
		}
	}
	return false
}

// Conversão WhatsApp→Chatwoot markdown, porta das regexes do serviço antigo
// (*bold* => **bold**, _italic_ => *italic*, ~strike~ => ~~strike~~).
// Go/RE2 não suporta lookaround; o grupo exige primeiro/último caractere
// não-espaço, equivalente ao (?!\s)...(?<!\s) original.
var (
	reWABold   = regexp.MustCompile(`\*([^\s*](?:[^\n*]*[^\s*])?)\*`)
	reWAItalic = regexp.MustCompile(`_([^\s_](?:[^\n_]*[^\s_])?)_`)
	reWAStrike = regexp.MustCompile(`~([^\s~](?:[^\n~]*[^\s~])?)~`)
)

func formatWAMarkdown(s string) string {
	if s == "" {
		return s
	}
	s = reWABold.ReplaceAllString(s, "**$1**")
	s = reWAItalic.ReplaceAllString(s, "*$1*")
	s = reWAStrike.ReplaceAllString(s, "~~$1~~")
	return s
}

func formatVCard(vcard string) string {
	if vcard == "" {
		return ""
	}
	info := map[string]string{}
	var tels []string
	for _, line := range strings.Split(vcard, "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), ":")
		if !ok || k == "" || v == "" {
			continue
		}
		info[k] = v
		if strings.Contains(k, "TEL") {
			tels = append(tels, v)
		}
	}
	b := &strings.Builder{}
	fmt.Fprintf(b, "*Contato:*\n\n_Nome:_ %s", info["FN"])
	for i, tel := range tels {
		fmt.Fprintf(b, "\n_Número (%d):_ %s", i+1, tel)
	}
	return b.String()
}

func formatLocation(lm map[string]any) string {
	lat, _ := getAny(lm, "degreesLatitude")
	lng, _ := getAny(lm, "degreesLongitude")
	name := getString(lm, "name")
	address := getString(lm, "address")

	b := &strings.Builder{}
	fmt.Fprintf(b, "*Localização:*\n\n_Latitude:_ %v \n_Longitude:_ %v \n", lat, lng)
	if name != "" {
		fmt.Fprintf(b, "_Nome:_ %s\n", name)
	}
	if address != "" {
		fmt.Fprintf(b, "_Endereço:_ %s \n", address)
	}
	fmt.Fprintf(b, "_URL:_ https://www.google.com/maps/search/?api=1&query=%v,%v", lat, lng)
	return b.String()
}

// ---------------------------------------------------------------------------
// Mídia.
// ---------------------------------------------------------------------------

// buildAttachment materializa a mídia do evento: base64 embutido
// (WEBHOOKFILES sem MinIO) ou download de mediaUrl (WEBHOOKFILES com MinIO).
// Sem nenhum dos dois (WEBHOOKFILES desabilitado), retorna nil — a mensagem
// segue só com o texto/caption.
func (s *Service) buildAttachment(ctx context.Context, m *waMessage) (*chatwoot.Attachment, error) {
	var data []byte
	mime := m.MimeType

	switch {
	case m.Base64 != "":
		var err error
		data, err = base64.StdEncoding.DecodeString(m.Base64)
		if err != nil {
			return nil, fmt.Errorf("ingest: base64 de mídia inválido: %w", err)
		}
	case m.MediaURL != "":
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.MediaURL, nil)
		if err != nil {
			return nil, fmt.Errorf("ingest: url de mídia inválida: %w", err)
		}
		resp, err := s.httpc.Do(req)
		if err != nil {
			return nil, fmt.Errorf("ingest: erro ao baixar mídia: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("ingest: download de mídia retornou status %d", resp.StatusCode)
		}
		const maxMediaBytes = 64 << 20 // 64 MiB
		data, err = io.ReadAll(io.LimitReader(resp.Body, maxMediaBytes))
		if err != nil {
			return nil, fmt.Errorf("ingest: erro lendo corpo da mídia: %w", err)
		}
		if mime == "" {
			mime = resp.Header.Get("Content-Type")
		}
	default:
		// WEBHOOKFILES desabilitado no evolutiongo: o evento não carrega a
		// mídia (nem base64 nem mediaUrl).
		s.log.Warn("ingest: mensagem de mídia sem base64/mediaUrl — habilite WEBHOOKFILES no evolutiongo", "id", m.ID)
		return nil, nil
	}

	if mime == "" {
		mime = mimeFromMessage(m.Message)
	}
	return &chatwoot.Attachment{
		Filename: attachmentFilename(m.Message, mime),
		Mime:     mime,
		Data:     data,
	}, nil
}

// mimeFromMessage lê o mimetype declarado no submessage waE2E (chave protobuf
// "mimetype"). Necessário no modo base64, em que o evolutiongo não injeta a
// chave "mimetype" no topo (só no modo MinIO).
func mimeFromMessage(msg map[string]any) string {
	for _, k := range []string{"imageMessage", "videoMessage", "audioMessage", "documentMessage", "stickerMessage"} {
		if sub := getMap(msg, k); sub != nil {
			if mt := getString(sub, "mimetype"); mt != "" {
				return mt
			}
		}
	}
	// Defaults usados pelo próprio evolutiongo ao converter (sticker=>png).
	switch {
	case getMap(msg, "imageMessage") != nil:
		return "image/jpeg"
	case getMap(msg, "audioMessage") != nil:
		return "audio/ogg"
	case getMap(msg, "videoMessage") != nil:
		return "video/mp4"
	case getMap(msg, "stickerMessage") != nil:
		return "image/png"
	default:
		return "application/octet-stream"
	}
}

// attachmentFilename usa o fileName do documento quando existe; caso contrário
// gera um nome aleatório com extensão derivada do mime (porta do nameFile).
func attachmentFilename(msg map[string]any, mime string) string {
	if dm := getMap(msg, "documentMessage"); dm != nil {
		if fn := getString(dm, "fileName", "filename"); fn != "" {
			return fn
		}
	}
	if dwc := getMap(msg, "documentWithCaptionMessage"); dwc != nil {
		if inner := getMap(getMap(dwc, "message"), "documentMessage"); inner != nil {
			if fn := getString(inner, "fileName", "filename"); fn != "" {
				return fn
			}
		}
	}
	return randomName() + extensionForMime(mime)
}

func extensionForMime(mime string) string {
	if i := strings.IndexByte(mime, ';'); i >= 0 {
		mime = strings.TrimSpace(mime[:i])
	}
	switch mime {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/webp":
		return ".webp"
	case "image/gif":
		return ".gif"
	case "audio/ogg":
		return ".ogg"
	case "audio/mpeg":
		return ".mp3"
	case "audio/mp4":
		return ".m4a"
	case "video/mp4":
		return ".mp4"
	case "application/pdf":
		return ".pdf"
	default:
		return ".bin"
	}
}

func randomName() string {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("file-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
