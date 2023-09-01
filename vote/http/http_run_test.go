package http_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/OpenSlides/openslides-autoupdate-service/pkg/datastore/dsmock"
	"github.com/OpenSlides/openslides-autoupdate-service/pkg/environment"
	"github.com/OpenSlides/openslides-vote-service/backend/memory"
	"github.com/OpenSlides/openslides-vote-service/vote"
	votehttp "github.com/OpenSlides/openslides-vote-service/vote/http"
)

func waitForServer(addr string) error {
	for i := 0; i < 100; i++ {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("waiting for server failed")
}

type autherStub struct {
	userID int
}

func (a *autherStub) Authenticate(w http.ResponseWriter, r *http.Request) (context.Context, error) {
	return r.Context(), nil
}

func (a *autherStub) FromContext(context.Context) int {
	return a.userID
}

func TestRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	backend := memory.New()
	ds := dsmock.NewFlow(nil)
	service, _, _ := vote.New(ctx, backend, backend, ds, true)
	httpServer := votehttp.New(environment.ForTests(map[string]string{"VOTE_PORT": "0"}))

	if err := httpServer.StartListener(); err != nil {
		t.Fatalf("start listening: %v", err)
	}

	go func() {
		if err := httpServer.Run(ctx, new(autherStub), service); err != nil {
			t.Errorf("vote.Run: %v", err)
		}
	}()

	if err := waitForServer(httpServer.Addr); err != nil {
		t.Errorf("waiting for server: %v", err)
	}

	t.Run("URLs", func(t *testing.T) {
		for _, url := range []string{
			"/internal/vote/start",
			"/internal/vote/stop",
			"/internal/vote/clear",
			"/internal/vote/clear_all",
			"/internal/vote/vote_count",
			"/system/vote",
			"/system/vote/voted",
			"/system/vote/health",
		} {
			resp, err := http.Get(fmt.Sprintf("http://%s%s", httpServer.Addr, url))
			if err != nil {
				t.Fatalf("sending request: %v", err)
			}

			if resp.StatusCode == 404 {
				t.Errorf("url %s does not exist", url)
			}
		}
	})
}
