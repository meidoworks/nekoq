package discovery

import (
	"net/http"

	"github.com/meidoworks/nekoq/shared/thirdpartyshared/ginshared"

	"github.com/gin-gonic/gin"
)

type ExternalHttpService struct {
	engine *gin.Engine
	addr   string
}

func NewHttpService(addr string) *ExternalHttpService {
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(func(context *gin.Context) {
		context.Next()
		var err error
		// handling first error to respond
		for _, v := range context.Errors {
			err = v
			break
		}
		if err != nil {
			context.String(http.StatusInternalServerError, err.Error())
		}
	})

	engine.GET("/utility/self_ip", ginshared.Wrap(func(ctx *gin.Context) ginshared.Render {
		return ginshared.RenderOKString(ctx.ClientIP())
	}))

	return &ExternalHttpService{
		engine: engine,
		addr:   addr,
	}
}
