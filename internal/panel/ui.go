package panel

import (
	"embed"
	"io/fs"
	"net/http"

	"github.com/gin-gonic/gin"
)

// staticFiles embute a UI estática (HTML+CSS+JS vanilla) no binário —
// sem framework de frontend, conforme regra do projeto.
//
//go:embed static
var staticFiles embed.FS

// RegisterUI registra a página do painel em GET / e os assets estáticos
// (app.js, style.css) em GET /static/*. Não exige apikey para carregar a
// página em si — a UI pede a apikey ao usuário e a usa apenas nas chamadas
// à API (guardada em localStorage no navegador), assim como o próprio
// endpoint /api/* já é protegido pelo authMiddleware.
func RegisterUI(r *gin.Engine) {
	sub, err := fs.Sub(staticFiles, "static")
	if err != nil {
		// Só pode falhar se o diretório "static" não existir no embed,
		// o que indicaria erro de build — falha alto e cedo.
		panic("panel: embed static inválido: " + err.Error())
	}

	r.GET("/", func(c *gin.Context) {
		data, err := fs.ReadFile(sub, "index.html")
		if err != nil {
			c.String(http.StatusInternalServerError, "painel indisponível")
			return
		}
		c.Header("Cache-Control", "no-cache")
		c.Data(http.StatusOK, "text/html; charset=utf-8", data)
	})

	// no-cache (revalidação a cada acesso): sem isso o Cloudflare na frente
	// do painel guarda app.js/style.css por horas no edge e os usuários
	// continuam com a UI antiga depois de um deploy.
	static := r.Group("/static", func(c *gin.Context) {
		c.Header("Cache-Control", "no-cache")
	})
	static.StaticFS("/", http.FS(sub))
}
