package numgen

import (
	"net/http"
	"strconv"
	"strings"

	chiraw "github.com/go-chi/chi"
	"github.com/meidoworks/nekoq-component/component/comphttp"
	"github.com/meidoworks/nekoq-component/http/chi"
)

type chiGenNum struct {
	s *ServiceNumGen
}

func (c chiGenNum) ParentUrl() string {
	return ""
}

func (c chiGenNum) Url() string {
	return "/v1/{gen_key}/{count}"
}

func (c chiGenNum) HttpMethod() []string {
	return []string{http.MethodGet}
}

func (c chiGenNum) Handle(r *http.Request) (comphttp.ResponseHandler[http.ResponseWriter], error) {
	key := chiraw.URLParam(r, "gen_key")
	countStr := chiraw.URLParam(r, "count")
	count, err := strconv.Atoi(countStr)
	//FIXME hardcoded max 100 IDs
	if err != nil || count <= 0 || count > 100 {
		return chi.RenderStatus(http.StatusBadRequest), nil
	}

	ids, err := c.s.GenerateFor(key, count)

	var idstrings = make([]string, 0, len(ids))
	for _, v := range ids {
		idstrings = append(idstrings, v.HexString())
	}

	result := strings.Join(idstrings, "\n")
	return chi.RenderOKString(result), nil
}
