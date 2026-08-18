// Command aliquotseal is the single-node HTTP entry point for the AliquotSeal
// biological sample mother-tube aliquoting service. It opens the SQLite store,
// runs startup recovery verification, and serves the HTTP API around the
// aggregate domain service.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aggregate"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/httpapi"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store/sqlite"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	dbPath := flag.String("db", "aliquotseal.db", "SQLite database path (or :memory:)")
	flag.Parse()

	if err := run(*addr, *dbPath); err != nil {
		log.Fatalf("aliquotseal: %v", err)
	}
}

func run(addr, dbPath string) error {
	st, err := sqlite.Open(dbPath, nil)
	if err != nil {
		return err
	}
	defer st.Close()

	// Startup recovery: refuse to accept writes if committed data violates the
	// conservation, lineage, occupancy, or terminal invariants (domain rule 14).
	if err := st.Recover(context.Background()); err != nil {
		return err
	}

	svc := aggregate.New(st, nil)
	srv := httpapi.New(svc)

	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("aliquotseal listening on %s", addr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case <-stop:
		log.Printf("shutting down")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return httpSrv.Shutdown(ctx)
	}
}
