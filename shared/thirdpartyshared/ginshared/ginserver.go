package ginshared

import (
	"net"
	"net/http"

	"github.com/gin-gonic/gin"
)

func StartBareMetalGinServer(l net.Listener, engine *gin.Engine) (*http.Server, error) {
	server := &http.Server{Handler: engine}
	if err := server.Serve(l); err != nil {
		return nil, err
	} else {
		return server, nil
	}
}
