package warehouse

import (
	"net/http"
	"strings"

	"github.com/meidoworks/nekoq-component/component/comphttp"
	"github.com/meidoworks/nekoq-component/http/chi"

	"github.com/meidoworks/nekoq/service/inproc"
)

var httpApis []comphttp.HttpApi[*http.Request, http.ResponseWriter]

func init() {
	httpApis = append(httpApis, new(AreaHttpApi))
	httpApis = append(httpApis, new(PutAreaHttpApi))
}

type abstractWarehouseHttpApi struct {
}

func (a *abstractWarehouseHttpApi) ParentUrl() string {
	return "/naming/warehouse"
}

func (a *abstractWarehouseHttpApi) Url() string {
	panic("implement me")
}

func (a *abstractWarehouseHttpApi) HttpMethod() []string {
	panic("implement me")
}

func (a *abstractWarehouseHttpApi) Handle(r *http.Request) (comphttp.ResponseHandler[http.ResponseWriter], error) {
	panic("implement me")
}

type AreaHttpApi struct {
	abstractWarehouseHttpApi
}

func (a *AreaHttpApi) Url() string {
	return "/area/{area}"
}

func (a *AreaHttpApi) HttpMethod() []string {
	return []string{http.MethodGet}
}

func (a *AreaHttpApi) Handle(r *http.Request) (comphttp.ResponseHandler[http.ResponseWriter], error) {
	areaStr := chi.GetUrlParam(r, "area")
	if areaStr == "" {
		return chi.RenderStatus(http.StatusBadRequest), nil
	}
	levels, err := inproc.WarehouseInst.AreaLevel(areaStr)
	if err != nil {
		return chi.RenderError(err), nil
	} else {
		return chi.RenderOKString(strings.Join(levels, ",")), nil
	}
}

type PutAreaHttpApi struct {
	abstractWarehouseHttpApi
}

func (a *PutAreaHttpApi) Url() string {
	return "/area/{parent}/{area}"
}

func (a *PutAreaHttpApi) HttpMethod() []string {
	return []string{http.MethodPut}
}

func (a *PutAreaHttpApi) Handle(r *http.Request) (comphttp.ResponseHandler[http.ResponseWriter], error) {
	areaStr := chi.GetUrlParam(r, "area")
	parentStr := chi.GetUrlParam(r, "parent")
	if areaStr == "" || parentStr == "" {
		return chi.RenderStatus(http.StatusBadRequest), nil
	}
	err := inproc.WarehouseInst.PutArea(parentStr, areaStr)
	if err != nil {
		return chi.RenderError(err), nil
	} else {
		return chi.RenderOKString("OK"), nil
	}
}
