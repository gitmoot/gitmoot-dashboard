// Command gitmoot-dashboard-dev serves the dashboard backed by a FakeDataSource
// for local development. It is not part of the production gitmoot binary.
package main

import (
	"flag"
	"log"
	"net/http"

	dashboard "github.com/gitmoot/gitmoot-dashboard"
)

func main() {
	addr := flag.String("addr", ":8099", "HTTP listen address")
	flag.Parse()
	handler := dashboard.Serve(dashboard.NewFakeDataSource())
	log.Printf("gitmoot-dashboard-dev listening on %s", *addr)
	if err := http.ListenAndServe(*addr, handler); err != nil {
		log.Fatal(err)
	}
}
