package http

import "github.com/gin-gonic/gin"

type ServerSetup struct {
	router *gin.Engine
}

func (svs *ServerSetup) Router() *gin.Engine {
	return svs.router
}

type SetupConfig struct {
	Router *gin.Engine
}

func NewServerSetup(config SetupConfig) ServerSetup {
	return ServerSetup{
		router: config.Router,
	}
}

func (server *ServerSetup) Run() {
}
