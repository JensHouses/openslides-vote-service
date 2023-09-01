package system_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/OpenSlides/openslides-autoupdate-service/pkg/auth/authtest"
	"github.com/OpenSlides/openslides-autoupdate-service/pkg/datastore/dsmock"
)

const (
	addr = "http://localhost:9013"
)

func TestHealth(t *testing.T) {
	skip(t)

	req, err := http.NewRequest("GET", addr+"/system/vote/health", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("send request: %v", err)
	}

	if resp.StatusCode != 200 {
		t.Errorf("Health request returned status %s", resp.Status)
	}
}

func TestStartVoteStopClear(t *testing.T) {
	skip(t)
	ctx := context.Background()

	db, err := newPostgresTestData(ctx)
	if err != nil {
		t.Fatalf("Create test DB: %v", err)
	}
	defer db.Close(ctx)
	defer func() {
		if err := clearVoteService(ctx); err != nil {
			t.Fatalf("clear vote service: %v", err)
		}
	}()

	if err := startPoll(ctx, db, 1); err != nil {
		t.Fatalf("Start poll: %v", err)
	}

	if err := vote(ctx, db, 1, 1, strings.NewReader(`{"value":"Y"}`)); err != nil {
		t.Fatalf("Vote: %v", err)
	}

	stopBody, err := stopPoll(ctx, db, 1)
	if err != nil {
		t.Fatalf("Stop poll: %v", err)
	}

	expectBody := `{"votes":[{"request_user_id":1,"vote_user_id":1,"value":"Y","weight":"1.000000"}],"user_ids":[1]}`
	if strings.TrimSpace(string(stopBody)) != expectBody {
		t.Fatalf("Got != expect\n%s\n%s", stopBody, expectBody)
	}

	if err := clearPoll(ctx, db, 1); err != nil {
		t.Fatalf("Clear poll: %v", err)
	}
}

func clearVoteService(ctx context.Context) error {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/internal/vote/clear_all", addr), nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}

	if resp.StatusCode != 200 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			body = []byte("can not read body")
		}
		return fmt.Errorf("got %s: %s", resp.Status, body)
	}

	return nil
}

func startPoll(ctx context.Context, db *postgresTestData, pollID int) error {
	db.addTestData(ctx, dsmock.YAMLData(fmt.Sprintf(`---
		poll/%d:
			meeting_id: 1
			type: named
			state: started
			backend: fast
			pollmethod: Y
			entitled_group_ids: [1]
			global_yes: true

		group/1/meeting_user_ids: [10]
		meeting_user/10:
			user_id: 1
			meeting_id: 1
			group_ids: [1]
		user/1:
			is_present_in_meeting_ids: [1]
			meeting_user_ids: [10]
		meeting/1/id: 5
		`,
		pollID)))

	req, err := http.NewRequest("GET", fmt.Sprintf("%s/internal/vote/start?id=%d", addr, pollID), nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}

	if resp.StatusCode != 200 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			body = []byte("can not read body")
		}
		return fmt.Errorf("got %s: %s", resp.Status, body)
	}

	return nil
}

func vote(ctx context.Context, db *postgresTestData, pollID, userID int, body io.Reader) error {
	cookie, headerName, headerValue, err := authtest.ValidTokens([]byte("openslides"), []byte("openslides"), userID)
	if err != nil {
		return fmt.Errorf("creating user tokens: %w", err)
	}

	req, err := http.NewRequest("GET", fmt.Sprintf("%s/system/vote?id=%d", addr, pollID), body)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.AddCookie(cookie)
	req.Header.Add(headerName, headerValue)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}

	if resp.StatusCode != 200 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			body = []byte("can not read body")
		}
		return fmt.Errorf("got %s: %s", resp.Status, body)
	}

	return nil
}

func stopPoll(ctx context.Context, db *postgresTestData, pollID int) ([]byte, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/internal/vote/stop?id=%d", addr, pollID), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}

	if resp.StatusCode != 200 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			body = []byte("can not read body")
		}
		return nil, fmt.Errorf("got %s: %s", resp.Status, body)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading body: %w", err)
	}

	return body, nil
}

func clearPoll(ctx context.Context, db *postgresTestData, pollID int) error {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/internal/vote/clear?id=%d", addr, pollID), nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}

	if resp.StatusCode != 200 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			body = []byte("can not read body")
		}
		return fmt.Errorf("got %s: %s", resp.Status, body)
	}

	return nil
}

func skip(t *testing.T) {
	if _, ok := os.LookupEnv("VOTE_SYSTEM_TEST"); !ok {
		t.SkipNow()
	}
}
