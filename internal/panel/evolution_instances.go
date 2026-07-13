// evolution_instances.go — proxy da listagem de instâncias do evolutiongo
// para a sidebar do painel: o usuário vê as instâncias REAIS do evolutiongo
// (mesmo sem config de Chatwoot salva) e configura a partir delas.
//
// Contrato (CONTRACTS.md §2 + verificado em instance_handler.go do
// evolutiongo): GET /instance/all, header apikey = GLOBAL_API_KEY, resposta
// {"message":"success","data":[Instance...]}. As tags JSON de Id/Name do
// model não foram verificadas no código-fonte (repo de referência removido
// do workspace), então o parsing aceita as grafias id/Id/ID e name/Name.
// O token da instância NUNCA é repassado ao navegador.
package panel

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// evolutionInstance é o item retornado por GET /api/evolution/instances.
type evolutionInstance struct {
	InstanceID   string `json:"instanceId"`
	InstanceName string `json:"instanceName"`
}

var evolutionHTTP = &http.Client{Timeout: 15 * time.Second}

// listEvolutionInstances proxia GET /instance/all do evolutiongo.
func (h *Handler) listEvolutionInstances(c *gin.Context) {
	req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodGet,
		h.Config.EvolutionBaseURL+"/instance/all", nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	req.Header.Set("apikey", h.Config.EvolutionAPIKey)

	res, err := evolutionHTTP.Do(req)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": fmt.Sprintf("evolutiongo inacessível: %v", err)})
		return
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		c.JSON(http.StatusBadGateway, gin.H{"error": fmt.Sprintf("evolutiongo /instance/all => HTTP %d (confira EVOLUTION_GLOBAL_API_KEY)", res.StatusCode)})
		return
	}

	var body struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "resposta inválida do evolutiongo: " + err.Error()})
		return
	}

	out := make([]evolutionInstance, 0, len(body.Data))
	for _, inst := range body.Data {
		id := firstString(inst, "id", "Id", "ID", "instanceId")
		if id == "" {
			continue
		}
		out = append(out, evolutionInstance{
			InstanceID:   id,
			InstanceName: firstString(inst, "name", "Name", "instanceName"),
		})
	}
	c.JSON(http.StatusOK, out)
}

// firstString devolve o primeiro valor string não-vazio dentre as chaves.
func firstString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k].(string); ok && v != "" {
			return v
		}
	}
	return ""
}
