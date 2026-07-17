// Painel Chatwoot — JS vanilla, sem framework.
// Fala com a API do próprio conector (GET/POST/DELETE /api/chatwoot/:id e
// GET /api/instances), autenticando com o header "apikey" guardado em
// localStorage.

(function () {
  "use strict";

  const LS_KEY = "cw_panel_apikey";

  const el = (id) => document.getElementById(id);

  const state = {
    apikey: localStorage.getItem(LS_KEY) || "",
    currentId: null,
    // nomes reais das instâncias do evolutiongo (id -> nome), para exibir e
    // persistir o nome em vez do UUID.
    names: {},
  };

  function ensureApikey() {
    if (!state.apikey) {
      const v = window.prompt("Informe a apikey do painel (CONNECTOR_API_KEY):");
      if (v) {
        state.apikey = v.trim();
        localStorage.setItem(LS_KEY, state.apikey);
      }
    }
    return state.apikey;
  }

  async function api(path, opts, isRetry) {
    opts = opts || {};
    const headers = Object.assign({ "Content-Type": "application/json" }, opts.headers || {});
    headers["apikey"] = state.apikey;
    const res = await fetch(path, Object.assign({}, opts, { headers }));
    let body = null;
    try {
      body = await res.json();
    } catch (_) {
      // resposta sem corpo JSON (ex.: 204)
    }
    if (res.status === 401 && !isRetry) {
      // Chave errada/desatualizada no localStorage: descarta, pede de novo
      // e repete a chamada uma única vez.
      state.apikey = "";
      localStorage.removeItem(LS_KEY);
      if (ensureApikey()) {
        return api(path, opts, true);
      }
    }
    if (!res.ok) {
      const msg = (body && body.error) || res.statusText || "erro desconhecido";
      const err = new Error(msg);
      err.status = res.status;
      throw err;
    }
    return body;
  }

  function setStatus(msg, kind) {
    const box = el("status-msg");
    box.textContent = msg || "";
    box.className = "status-msg" + (kind ? " " + kind : "");
    // O formulário é longo e o botão Salvar fica no fim da página; o status
    // junto ao título fica fora da tela, então o resultado de save/delete
    // também aparece como toast flutuante.
    if (msg && kind) {
      toast(msg, kind);
    }
  }

  function toast(msg, kind) {
    let container = el("toast-container");
    if (!container) {
      container = document.createElement("div");
      container.id = "toast-container";
      document.body.appendChild(container);
    }
    const t = document.createElement("div");
    t.className = "toast " + (kind || "");
    t.textContent = msg;
    container.appendChild(t);
    setTimeout(() => t.classList.add("show"), 10);
    setTimeout(() => {
      t.classList.remove("show");
      setTimeout(() => t.remove(), 300);
    }, 4000);
  }

  // --- Sidebar ---------------------------------------------------------

  async function loadInstances() {
    const list = el("instance-list");
    list.innerHTML = '<li class="instance-empty">Carregando…</li>';

    // Duas fontes, mescladas por instanceId:
    //  - api/evolution/instances: instâncias REAIS do evolutiongo (mesmo sem
    //    config de Chatwoot ainda) — é o que o usuário espera ver;
    //  - api/instances: configs salvas no conector (traz o status enabled).
    let configs = [];
    let evoInstances = [];
    let evoError = null;
    try {
      configs = (await api("api/instances")) || [];
    } catch (e) {
      list.innerHTML = '<li class="instance-empty">Erro: ' + escapeHtml(e.message) + "</li>";
      return;
    }
    try {
      evoInstances = (await api("api/evolution/instances")) || [];
    } catch (e) {
      evoError = e.message; // evolutiongo fora do ar não impede ver configs salvas
    }

    const byId = new Map();
    evoInstances.forEach((it) => {
      // guarda o nome REAL da instância do evolutiongo para usar ao salvar.
      if (it.instanceName) state.names[it.instanceId] = it.instanceName;
      byId.set(it.instanceId, {
        instanceId: it.instanceId,
        instanceName: it.instanceName || it.instanceId,
        enabled: false,
        configured: false,
      });
    });
    configs.forEach((cfg) => {
      const cur = byId.get(cfg.instanceId) || {
        instanceId: cfg.instanceId,
        instanceName: cfg.instanceName || cfg.instanceId,
      };
      cur.enabled = !!cfg.enabled;
      cur.configured = true;
      // Só usa o nome salvo se for um nome REAL (configs antigas gravaram o
      // UUID como nome); nesse caso mantém o nome do evolutiongo já definido.
      if (cfg.instanceName && cfg.instanceName !== cfg.instanceId) {
        cur.instanceName = cfg.instanceName;
      }
      byId.set(cfg.instanceId, cur);
    });

    renderInstances(Array.from(byId.values()), evoError);
  }

  function renderInstances(items, evoError) {
    const list = el("instance-list");
    list.innerHTML = "";
    if (evoError) {
      list.innerHTML =
        '<li class="instance-empty">⚠ evolutiongo: ' + escapeHtml(evoError) + "</li>";
    }
    if (items.length === 0) {
      list.innerHTML += '<li class="instance-empty">Nenhuma instância ainda</li>';
      return;
    }
    items.forEach((item) => {
      const li = document.createElement("li");
      li.className = item.enabled ? "enabled" : "";
      if (item.instanceId === state.currentId) li.classList.add("active");
      const label = item.configured ? "" : ' <small style="opacity:.55">(sem config)</small>';
      li.innerHTML =
        '<span class="dot"></span><span>' +
        escapeHtml(item.instanceName || item.instanceId) + label + "</span>";
      li.addEventListener("click", () => selectInstance(item.instanceId));
      list.appendChild(li);
    });
  }

  function escapeHtml(s) {
    const d = document.createElement("div");
    d.textContent = String(s == null ? "" : s);
    return d.innerHTML;
  }

  // --- Form --------------------------------------------------------------

  function fillForm(cfg) {
    el("f-instanceId").value = cfg.instanceId || "";
    el("f-enabled").checked = !!cfg.enabled;
    el("f-url").value = cfg.url || "";
    el("f-accountId").value = cfg.accountId || "";
    el("f-token").value = cfg.token || "";
    el("f-signMsg").checked = !!cfg.signMsg;
    el("f-signDelimiter").value = cfg.signDelimiter || "";
    el("f-nameInbox").value = cfg.nameInbox || "";
    el("f-organization").value = cfg.organization || "";
    el("f-logo").value = cfg.logo || "";
    el("f-conversationPending").checked = !!cfg.conversationPending;
    el("f-reopenConversation").checked = !!cfg.reopenConversation;
    el("f-importContacts").checked = !!cfg.importContacts;
    el("f-importMessages").checked = !!cfg.importMessages;
    el("f-daysLimitImportMessages").value = cfg.daysLimitImportMessages || 0;
    el("f-number").value = cfg.number || "";
    el("f-mergeBrazilContacts").checked = !!cfg.mergeBrazilContacts;
    el("f-autoCreate").checked = !!cfg.autoCreate;
    el("f-ignoreJids").value = (cfg.ignoreJids || []).join(",");
    el("f-webhookUrl").value = cfg.webhook_url || "";
  }

  function clearForm(instanceId) {
    // Instância nova já vem com Enabled marcado por padrão (é o caso comum:
    // quem abre a config quer habilitar a integração).
    fillForm({ instanceId: instanceId || "", enabled: true });
    el("panel-title").textContent = instanceId ? "Chatwoot — " + instanceId : "Chatwoot — nova instância";
  }

  function readForm() {
    const ignoreJids = el("f-ignoreJids")
      .value.split(",")
      .map((s) => s.trim())
      .filter(Boolean);

    const instanceId = el("f-instanceId").value.trim();
    return {
      // Nome REAL da instância (do evolutiongo); nunca o UUID. Antes gravava o
      // id como nome, e a lista passava a mostrar o UUID após salvar.
      instanceName: state.names[instanceId] || instanceId,
      enabled: el("f-enabled").checked,
      url: el("f-url").value.trim(),
      accountId: el("f-accountId").value.trim(),
      token: el("f-token").value,
      nameInbox: el("f-nameInbox").value.trim(),
      signMsg: el("f-signMsg").checked,
      signDelimiter: el("f-signDelimiter").value,
      number: el("f-number").value.trim(),
      reopenConversation: el("f-reopenConversation").checked,
      conversationPending: el("f-conversationPending").checked,
      mergeBrazilContacts: el("f-mergeBrazilContacts").checked,
      importContacts: el("f-importContacts").checked,
      importMessages: el("f-importMessages").checked,
      daysLimitImportMessages: parseInt(el("f-daysLimitImportMessages").value || "0", 10),
      autoCreate: el("f-autoCreate").checked,
      organization: el("f-organization").value.trim(),
      logo: el("f-logo").value.trim(),
      ignoreJids: ignoreJids,
    };
  }

  async function selectInstance(instanceId) {
    state.currentId = instanceId;
    setStatus("");
    try {
      const cfg = await api("api/chatwoot/" + encodeURIComponent(instanceId));
      cfg.instanceId = instanceId;
      fillForm(cfg);
      el("panel-title").textContent = "Chatwoot — " + instanceId;
      highlightActive();
    } catch (e) {
      if (e.status === 401) {
        clearForm(instanceId);
        highlightActive();
        setStatus("Não autorizado: " + e.message, "err");
        return;
      }
      // Instância do evolutiongo ainda sem config no conector: abre o
      // formulário em branco com o id preenchido, pronto para salvar.
      clearForm(instanceId);
      highlightActive();
      setStatus("Instância ainda sem configuração — preencha e salve para habilitar.", "");
    }
  }

  function highlightActive() {
    document.querySelectorAll("#instance-list li").forEach((li) => li.classList.remove("active"));
    loadInstances();
  }

  async function saveForm(ev) {
    ev.preventDefault();
    const instanceId = el("f-instanceId").value.trim();
    if (!instanceId) {
      setStatus("Informe o identificador da instância.", "err");
      return;
    }
    setStatus("Salvando…");
    try {
      const payload = readForm();
      const cfg = await api("api/chatwoot/" + encodeURIComponent(instanceId), {
        method: "POST",
        body: JSON.stringify(payload),
      });
      state.currentId = instanceId;
      fillForm(Object.assign({ instanceId: instanceId }, cfg));
      setStatus("Salvo com sucesso.", "ok");
      loadInstances();
    } catch (e) {
      setStatus("Erro ao salvar: " + e.message, "err");
    }
  }

  async function deleteCurrent() {
    const instanceId = el("f-instanceId").value.trim();
    if (!instanceId) return;
    if (!window.confirm("Excluir a configuração Chatwoot da instância '" + instanceId + "'?")) return;
    setStatus("Excluindo…");
    try {
      await api("api/chatwoot/" + encodeURIComponent(instanceId), { method: "DELETE" });
      setStatus("Excluída.", "ok");
      state.currentId = null;
      clearForm("");
      loadInstances();
    } catch (e) {
      setStatus("Erro ao excluir: " + e.message, "err");
    }
  }

  function newInstance() {
    const id = window.prompt("Identificador da instância (instanceId):");
    if (!id) return;
    state.currentId = id.trim();
    clearForm(state.currentId);
    setStatus("");
  }

  // --- Bootstrap -----------------------------------------------------------

  function init() {
    ensureApikey();

    el("cw-form").addEventListener("submit", saveForm);
    el("btn-delete").addEventListener("click", deleteCurrent);
    el("btn-new").addEventListener("click", newInstance);
    el("btn-apikey").addEventListener("click", () => {
      state.apikey = "";
      localStorage.removeItem(LS_KEY);
      ensureApikey();
      loadInstances();
    });

    clearForm("");
    loadInstances();

    // Deep-link: /connector/?instance=<id> abre direto a config daquela
    // instância (usado pelo atalho injetado na dashboard do evolutiongo,
    // que passa a instância aberta no manager). Instância ainda sem config
    // cai no formulário "novo" já com o id preenchido.
    const deepLink = new URLSearchParams(window.location.search).get("instance");
    if (deepLink) {
      state.currentId = deepLink;
      api("api/chatwoot/" + encodeURIComponent(deepLink))
        .then((cfg) => {
          cfg.instanceId = deepLink;
          fillForm(cfg);
          el("panel-title").textContent = "Chatwoot — " + deepLink;
          highlightActive();
        })
        .catch(() => {
          clearForm(deepLink);
          setStatus("Instância ainda sem configuração — preencha e salve para habilitar.", "");
        });
    }
  }

  document.addEventListener("DOMContentLoaded", init);
})();
