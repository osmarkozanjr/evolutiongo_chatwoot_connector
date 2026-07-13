// importer.go — import de contatos e mensagens históricas (porta adaptada do
// chatwoot-import-helper.ts da integração antiga).
//
// Diferença estrutural vs. o serviço antigo: lá o import lia o banco Prisma da
// própria Evolution API e inseria DIRETO no Postgres do Chatwoot (bulk SQL).
// Aqui os dados chegam pelos eventos do evolutiongo (HistorySync/Contact/
// PushName) e o import usa a API HTTP do Chatwoot — mais lento, porém sem
// acoplamento com o schema interno do Chatwoot.
//
// TODO(VERIFY): as chaves do payload de HistorySync vêm do protobuf
// waHistorySync serializado por encoding/json (tags lowerCamel geradas pelo
// protoc: "conversations", "messages", "message", "key", "remoteJID"...).
// events.Contact/events.PushName são structs Go sem tags (chaves PascalCase:
// "JID", "Action", "NewPushName"). O parsing abaixo aceita as duas grafias,
// como no restante do pacote.
package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/iceasa/evolution-chatwoot-connector/internal/model"
)

// handleContactEvent processa events.Contact (sync/update da agenda).
// Comportamento portado: com importContacts habilitado garante o contato no
// Chatwoot; sem importContacts, apenas atualiza o nome de contatos já mapeados.
func (s *Service) handleContactEvent(ctx context.Context, cfg *model.ChatwootConfig, ev *model.EventEnvelope) error {
	var root map[string]any
	if err := json.Unmarshal(ev.Data, &root); err != nil {
		s.log.Warn("ingest: payload de contact descartado", "instanceId", ev.InstanceID, "error", err)
		return nil
	}

	jid := ""
	if v, ok := getAny(root, "JID", "jid"); ok {
		jid = jidToString(v)
	}
	if jid == "" || isGroupJid(jid) || shouldIgnoreJid(cfg.IgnoreJids, jid) {
		return nil
	}

	action := getMap(root, "Action", "action")
	name := getString(action, "FullName", "fullName")
	if name == "" {
		name = getString(action, "FirstName", "firstName")
	}
	if name == "" {
		return nil
	}

	return s.upsertContactName(ctx, cfg, jid, name, cfg.ImportContacts)
}

// handlePushNameEvent processa events.PushName: atualiza o nome de exibição de
// contatos já mapeados (não cria contato novo — igual ao serviço antigo, que
// só atualizava em contacts.update).
func (s *Service) handlePushNameEvent(ctx context.Context, cfg *model.ChatwootConfig, ev *model.EventEnvelope) error {
	var root map[string]any
	if err := json.Unmarshal(ev.Data, &root); err != nil {
		s.log.Warn("ingest: payload de pushname descartado", "instanceId", ev.InstanceID, "error", err)
		return nil
	}

	jid := ""
	if v, ok := getAny(root, "JID", "jid"); ok {
		jid = jidToString(v)
	}
	name := getString(root, "NewPushName", "newPushName")
	if jid == "" || name == "" || isGroupJid(jid) || shouldIgnoreJid(cfg.IgnoreJids, jid) {
		return nil
	}

	return s.upsertContactName(ctx, cfg, jid, name, false)
}

// upsertContactName atualiza (e opcionalmente cria) um contato no Chatwoot.
func (s *Service) upsertContactName(ctx context.Context, cfg *model.ChatwootConfig, jid, name string, createIfMissing bool) error {
	mapping, err := s.store.GetContact(ctx, cfg.InstanceID, jid)
	if err != nil {
		return fmt.Errorf("ingest: erro ao buscar mapping de contato %s: %w", jid, err)
	}

	if mapping != nil {
		if err := s.cw.UpdateContact(ctx, cfg, mapping.ChatwootContactID, map[string]any{"name": name}); err != nil {
			// Não bloqueia a fila por falha de update de nome.
			s.log.Warn("ingest: falha ao atualizar nome do contato", "jid", jid, "error", err)
		}
		return nil
	}

	if !createIfMissing {
		return nil
	}

	phone := jidUser(jid)
	found, err := s.searchContactWithBrazilVariants(ctx, cfg, "+"+phone)
	if err != nil {
		return fmt.Errorf("ingest: erro ao buscar contato %s no chatwoot: %w", phone, err)
	}
	contactID := 0
	if found != nil {
		contactID = found.ID
	} else {
		created, err := s.cw.CreateContact(ctx, cfg, cfg.InboxID, "+"+phone, name, "", jid)
		if err != nil {
			return fmt.Errorf("ingest: erro ao criar contato %s no import: %w", phone, err)
		}
		contactID = created.ID
	}

	return s.store.SaveContact(ctx, &model.ContactMapping{
		InstanceID:        cfg.InstanceID,
		RemoteJid:         jid,
		ChatwootContactID: contactID,
		Identifier:        jid,
		UpdatedAt:         time.Now(),
	})
}

// handleHistorySync processa events.HistorySync (histórico recebido ao parear
// via QR Code). Respeita importContacts, importMessages e
// daysLimitImportMessages, como o chatwoot-import-helper antigo.
func (s *Service) handleHistorySync(ctx context.Context, cfg *model.ChatwootConfig, ev *model.EventEnvelope) error {
	if !cfg.ImportContacts && !cfg.ImportMessages {
		return nil
	}

	var root map[string]any
	if err := json.Unmarshal(ev.Data, &root); err != nil {
		s.log.Warn("ingest: payload de historysync descartado", "instanceId", ev.InstanceID, "error", err)
		return nil
	}
	// events.HistorySync{Data *waHistorySync.HistorySync}
	sync := getMap(root, "Data", "data")
	if sync == nil {
		sync = root // tolera payload já "achatado"
	}

	var cutoff time.Time
	if cfg.DaysLimitImportMessages > 0 {
		cutoff = time.Now().AddDate(0, 0, -cfg.DaysLimitImportMessages)
	}

	imported := 0
	for _, c := range getSlice(sync, "Conversations", "conversations") {
		conv := asMap(c)
		if conv == nil {
			continue
		}
		remoteJid := getString(conv, "ID", "id", "Id")
		if remoteJid == "" || strings.HasPrefix(remoteJid, "status@broadcast") {
			continue
		}
		if shouldIgnoreJid(cfg.IgnoreJids, remoteJid) {
			continue
		}

		chatName := getString(conv, "Name", "name", "DisplayName", "displayName")

		// Import de contato da conversa (somente contatos diretos).
		if cfg.ImportContacts && !isGroupJid(remoteJid) && chatName != "" {
			if err := s.upsertContactName(ctx, cfg, remoteJid, chatName, true); err != nil {
				s.log.Warn("ingest: import de contato falhou", "jid", remoteJid, "error", err)
			}
		}

		if !cfg.ImportMessages {
			continue
		}

		for _, hm := range getSlice(conv, "Messages", "messages") {
			hmm := asMap(hm)
			if hmm == nil {
				continue
			}
			// HistorySyncMsg{Message *WebMessageInfo}
			wmi := getMap(hmm, "Message", "message")
			if wmi == nil {
				continue
			}
			m := parseWebMessageInfo(remoteJid, wmi)
			if m == nil || m.ID == "" {
				continue
			}
			if !cutoff.IsZero() && !m.Timestamp.IsZero() && m.Timestamp.Before(cutoff) {
				continue
			}

			if err := s.importHistoryMessage(ctx, cfg, ev, m); err != nil {
				// Erro num item não deve derrubar o sync inteiro (requeue
				// reprocessaria tudo); loga e segue.
				s.log.Warn("ingest: import de mensagem histórica falhou", "waId", m.ID, "error", err)
				continue
			}
			imported++
		}
	}

	if imported > 0 {
		s.log.Info("ingest: historysync importado", "instanceId", ev.InstanceID, "messages", imported)
	}
	return nil
}

// parseWebMessageInfo converte um WebMessageInfo (protobuf serializado) do
// histórico em waMessage reutilizável pelo fluxo normal.
func parseWebMessageInfo(remoteJid string, wmi map[string]any) *waMessage {
	key := getMap(wmi, "Key", "key")
	if key == nil {
		return nil
	}
	m := &waMessage{
		ID:        getString(key, "ID", "id", "Id"),
		RemoteJid: remoteJid,
		FromMe:    getBool(key, "FromMe", "fromMe"),
		PushName:  getString(wmi, "PushName", "pushName"),
		Message:   getMap(wmi, "Message", "message"),
	}
	if rj := getString(key, "RemoteJID", "remoteJID", "remoteJid"); rj != "" {
		m.RemoteJid = rj
	}
	if p := getString(key, "Participant", "participant"); p != "" {
		m.SenderJid = p
	} else if m.FromMe {
		m.SenderJid = "" // o próprio dono da instância
	} else {
		m.SenderJid = m.RemoteJid
	}
	m.IsGroup = isGroupJid(m.RemoteJid)
	if ts, ok := getAny(wmi, "MessageTimestamp", "messageTimestamp"); ok {
		m.Timestamp = parseTimestamp(ts)
	}
	return m
}

// importHistoryMessage cria a mensagem histórica no Chatwoot reutilizando os
// mesmos blocos do fluxo normal (dedup, contato, conversa, conteúdo), porém
// sem download de mídia (histórico importa só o texto/caption, como o
// import-helper antigo em modo API).
func (s *Service) importHistoryMessage(ctx context.Context, cfg *model.ChatwootConfig, ev *model.EventEnvelope, m *waMessage) error {
	// Dedup
	if existing, err := s.store.GetMessageByWhatsappID(ctx, cfg.InstanceID, m.ID); err != nil {
		return err
	} else if existing != nil {
		return nil
	}

	content := formatWAMarkdown(conversationContent(m.Message))
	if content == "" && isMediaMessage(m.Message) {
		content = "📎 (mídia do histórico não importada)"
	}
	if content == "" {
		return nil
	}

	conv, err := s.resolveConversation(ctx, cfg, ev, m)
	if err != nil {
		return err
	}
	if conv == nil {
		return nil
	}

	messageType := "incoming"
	if m.FromMe {
		messageType = "outgoing"
	}
	if m.IsGroup && !m.FromMe && m.SenderJid != "" {
		content = fmt.Sprintf("**+%s - %s:**\n\n%s", jidUser(m.SenderJid), m.PushName, content)
	}

	created, err := s.cw.CreateMessage(ctx, cfg, conv.ChatwootConversationID, content, messageType, nil, "WAID:"+m.ID)
	if err != nil {
		return err
	}
	return s.store.SaveMessage(ctx, &model.MessageMapping{
		InstanceID:        cfg.InstanceID,
		WhatsappMessageID: m.ID,
		ChatwootMessageID: created.ID,
		Direction:         "in",
		CreatedAt:         time.Now(),
	})
}
