package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func (server *ServerSetup) RegisterRoutes() {
	server.router.GET("/", func(ctx *gin.Context) {

		data := []byte{1, 2, 3, 3, 20, 28, 20, 29, 200, 50, 67, 89, 45, 23, 230, 67, 65, 66, 67}
		ctx.Header("Content-Description", "File Transfer")
		ctx.Header("Content-Disposition", "attachment; filename=ff.txt")
		ctx.Data(http.StatusOK, "application/octet-stream", data)
		// ctx.String(200, "App is up and running")
	})

	v1 := server.router.Group("/api/v1")

	v1.Use(TimeoutMiddleware())
	v1.POST("/readfile", ReadExcel)
	v1.GET("/populate", PopulateExcel)
}
