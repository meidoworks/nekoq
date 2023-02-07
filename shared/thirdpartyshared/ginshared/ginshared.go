package ginshared

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type Render interface {
}

type statusOnlyRender struct {
	Status int
}

func RenderStatus(status int) Render {
	return statusOnlyRender{Status: status}
}

type stringRender struct {
	Status int
	String string
}

func RenderOKString(str string) Render {
	return &stringRender{
		Status: http.StatusOK,
		String: str,
	}
}

func RenderString(status int, str string) Render {
	return &stringRender{
		Status: status,
		String: str,
	}
}

type errorRender struct {
	Err error
}

func RenderError(err error) Render {
	return errorRender{Err: err}
}

type DefaultHandler func(ctx *gin.Context) Render

func Wrap(f DefaultHandler) func(ctx *gin.Context) {
	return func(ctx *gin.Context) {
		render := f(ctx)
		switch r := render.(type) {
		case errorRender:
			//FIXME should call this method only?
			_ = ctx.Error(r.Err)
		case statusOnlyRender:
			ctx.Status(r.Status)
		case *stringRender:
			ctx.String(r.Status, r.String)
		default:
			ctx.Status(http.StatusInternalServerError)
		}
	}
}
