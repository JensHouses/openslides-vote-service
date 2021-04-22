package vote_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/OpenSlides/openslides-autoupdate-service/pkg/dsmock"
	"github.com/OpenSlides/openslides-vote-service/internal/backends/memory"
	"github.com/OpenSlides/openslides-vote-service/internal/vote"
)

const (
	validConfig1 = `{"content_object_id":"motion/1","backend":"fast","entitled_group_ids":[1]}`
	validConfig2 = `{"content_object_id":"assignment/1","backend":"fast"}`
)

func TestVoteCreate(t *testing.T) {
	closed := make(chan struct{})
	defer close(closed)

	backend := memory.New()

	ds := StubGetter{data: dsmock.YAMLData(`
	poll/1:
		id: 1
		meeting_id: 5

	group/1/user_ids: [1]
	user/1/is_present_in_meeting_ids: [1]
	`)}

	v := vote.New(backend, backend, &ds)

	t.Run("Unknown poll", func(t *testing.T) {
		if err := v.Create(context.Background(), 1); err != nil {
			t.Errorf("Create returned unexpected error: %v", err)
		}

		// After a poll was created, it has to be possible to send votes.
		err := backend.Vote(context.Background(), 1, 1, []byte("something"))
		if err != nil {
			t.Errorf("Vote after create retuen and unexpected error: %v", err)
		}
	})

	t.Run("Create poll a second time", func(t *testing.T) {
		if err := v.Create(context.Background(), 1); err != nil {
			t.Errorf("Create returned unexpected error: %v", err)
		}
	})

	t.Run("Create a stopped poll", func(t *testing.T) {
		if _, err := backend.Stop(context.Background(), 2); err != nil {
			t.Fatalf("Stop returned unexpected error: %v", err)
		}

		if err := v.Create(context.Background(), 2); err != nil {
			t.Errorf("Create returned unexpected error: %v", err)
		}
	})
}

type StubGetter struct {
	data      map[string]string
	err       error
	requested map[string]bool
}

func (g *StubGetter) Get(ctx context.Context, keys ...string) ([]json.RawMessage, error) {
	if g.err != nil {
		return nil, g.err
	}
	if g.requested == nil {
		g.requested = make(map[string]bool)
	}

	values := make([]json.RawMessage, len(keys))
	for i, key := range keys {
		g.requested[key] = true

		v, ok := g.data[key]
		if ok {
			values[i] = []byte(v)
		}
	}
	return values, nil
}

func (g *StubGetter) assertKeys(t *testing.T, keys ...string) {
	t.Helper()
	for _, key := range keys {
		if !g.requested[key] {
			t.Errorf("Key %s is was not requested", key)
		}
	}
}

func TestVoteCreatePreloadData(t *testing.T) {
	backend := memory.New()
	// TODO: add poll config fields
	ds := StubGetter{data: dsmock.YAMLData(`
	poll/1:
		meeting_id: 1
		entitled_group_ids: [1]
	
	group:
		1:
			user_ids: [1,2]
	user:
		1:
			is_present_in_meeting_ids: [1]
		2:
			is_present_in_meeting_ids: [1]
	`)}
	v := vote.New(backend, backend, &ds)

	if err := v.Create(context.Background(), 1); err != nil {
		t.Errorf("Create returned unexpected error: %v", err)
	}

	ds.assertKeys(t, "poll/1/meeting_id", "user/1/is_present_in_meeting_ids", "user/2/is_present_in_meeting_ids")
}

func TestVoteCreateDSError(t *testing.T) {
	backend := memory.New()
	ds := StubGetter{err: errors.New("Some error")}
	v := vote.New(backend, backend, &ds)
	err := v.Create(context.Background(), 1)

	if err == nil {
		t.Errorf("Got no error, expected `Some error`")
	}
}

func TestVoteStop(t *testing.T) {
	closed := make(chan struct{})
	defer close(closed)

	backend := memory.New()
	v := vote.New(backend, backend, &StubGetter{data: dsmock.YAMLData(`
	poll/1/id: 1
	poll/2/id: 2
	`)})

	t.Run("Unknown poll", func(t *testing.T) {
		buf := new(bytes.Buffer)
		if err := v.Stop(context.Background(), 1, buf); err != nil {
			t.Errorf("Stopping a unknown poll is not an error, got: %v", err)
		}
	})

	t.Run("Known poll", func(t *testing.T) {
		if err := backend.Start(context.Background(), 2); err != nil {
			t.Fatalf("Start returned an unexpected error: %v", err)
		}

		backend.Vote(context.Background(), 2, 1, []byte(`"polldata1"`))
		backend.Vote(context.Background(), 2, 2, []byte(`"polldata2"`))

		buf := new(bytes.Buffer)
		if err := v.Stop(context.Background(), 2, buf); err != nil {
			t.Fatalf("Stop returned unexpected error: %v", err)
		}

		expect := `["polldata1","polldata2"]`
		if got := strings.TrimSpace(buf.String()); got != expect {
			t.Errorf("Stop wrote `%s`, expected `%s`", got, expect)
		}

		err := backend.Vote(context.Background(), 2, 3, []byte(`"polldata3"`))
		var errStopped interface{ Stopped() }
		if !errors.As(err, &errStopped) {
			t.Errorf("Stop did not stop the poll in the backend.")
		}
	})
}

func TestVoteClear(t *testing.T) {
	closed := make(chan struct{})
	defer close(closed)

	backend := memory.New()
	v := vote.New(backend, backend, &StubGetter{})

	if err := v.Clear(context.Background(), 1); err != nil {
		t.Fatalf("Clear returned unexpected error: %v", err)
	}
}

func TestVoteVote(t *testing.T) {
	closed := make(chan struct{})
	defer close(closed)

	backend := memory.New()
	v := vote.New(backend, backend, &StubGetter{})

	t.Run("Unknown poll", func(t *testing.T) {
		err := v.Vote(context.Background(), 1, 1, strings.NewReader(`"Y"`))

		if !errors.Is(err, vote.ErrNotExists) {
			t.Errorf("Expected ErrNotExists, got: %v", err)
		}
	})

	if err := backend.Start(context.Background(), 1); err != nil {
		t.Fatalf("Starting poll returned unexpected error: %v", err)
	}

	t.Run("Invalid json", func(t *testing.T) {
		err := v.Vote(context.Background(), 1, 1, strings.NewReader(`{123`))

		var errTyped vote.TypeError
		if !errors.As(err, &errTyped) {
			t.Fatalf("Vote() did not return an TypeError, got: %v", err)
		}

		if errTyped != vote.ErrInvalid {
			t.Errorf("Got error type `%s`, expected `%s`", errTyped.Type(), vote.ErrInvalid.Type())
		}
	})

	t.Run("Invalid format", func(t *testing.T) {
		err := v.Vote(context.Background(), 1, 1, strings.NewReader(`{}`))

		var errTyped vote.TypeError
		if !errors.As(err, &errTyped) {
			t.Fatalf("Vote() did not return an TypeError, got: %v", err)
		}

		if errTyped != vote.ErrInvalid {
			t.Errorf("Got error type `%s`, expected `%s`", errTyped.Type(), vote.ErrInvalid.Type())
		}
	})

	t.Run("Valid data", func(t *testing.T) {
		// TODO: Fix format.
		err := v.Vote(context.Background(), 1, 1, strings.NewReader(`"Y"`))
		if err != nil {
			t.Fatalf("Vote returned unexpected error: %v", err)
		}
	})

	t.Run("User has voted", func(t *testing.T) {
		err := v.Vote(context.Background(), 1, 1, strings.NewReader(`"Y"`))
		if err == nil {
			t.Fatalf("Vote returned no error")
		}

		var errTyped vote.TypeError
		if !errors.As(err, &errTyped) {
			t.Fatalf("Vote() did not return an TypeError, got: %v", err)
		}

		if errTyped != vote.ErrDoubleVote {
			t.Errorf("Got error type `%s`, expected `%s`", errTyped.Type(), vote.ErrDoubleVote.Type())
		}
	})

	t.Run("Poll is stopped", func(t *testing.T) {
		backend.Stop(context.Background(), 1)

		err := v.Vote(context.Background(), 1, 1, strings.NewReader(`"Y"`))
		if err == nil {
			t.Fatalf("Vote returned no error")
		}

		var errTyped vote.TypeError
		if !errors.As(err, &errTyped) {
			t.Fatalf("Vote() did not return an TypeError, got: %v", err)
		}

		if errTyped != vote.ErrStopped {
			t.Errorf("Got error type `%s`, expected `%s`", errTyped.Type(), vote.ErrStopped.Type())
		}
	})
}
