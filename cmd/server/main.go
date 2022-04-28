package main

import (
	"fmt"
	"github.com/erfanz/proglog/internal/server"
	"log"
)

func main() {
	srv := server.NewHttpServer(":8080")
	fmt.Println("Server is started and listetning ...")
	log.Fatal(srv.ListenAndServe())
}
