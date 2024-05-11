package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	appsetup "github.com/mrbeyond/readexceltodb/http"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		panic("Cannot load env")
	}

	app := appsetup.NewServerSetup(appsetup.SetupConfig{
		Router: gin.Default(),
	})

	app.RegisterRoutes()
	server := http.Server{
		Addr:         fmt.Sprintf(":%s", os.Getenv("PORT")),
		Handler:      app.Router(),
		ReadTimeout:  1 * time.Minute, // Default is 10s, increaased incase of large files
		WriteTimeout: 2 * time.Minute, // Default is 10s, increaased incase of longer operation
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("listen:", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server with a timeout context.
	// Visit https://gin-gonic.com/docs/examples/graceful-restart-or-stop/
	quit := make(chan os.Signal, 1)
	// kill (no param) default send syscanll.SIGTERM
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatal("Server shutdown error", err)
	}

	// Catching ctx.Done().

	<-ctx.Done()
	log.Println("Graceful shutdown completed")
}
