package vote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Vote holds the state of the service.
//
// Vote has to be initializes with vote.New().
type Vote struct {
	fastBackend Backend
	longBackend Backend
	config      Configer
}

// New creates an initializes vote service.
func New(fast, long Backend, config Configer) *Vote {
	return &Vote{
		fastBackend: fast,
		longBackend: long,
		config:      config,
	}
}

// Create an electronic vote.
//
// This function is idempotence. If you call it with the same input, you will
// get the same output. This means, that when a poll is stopped, Create() will
// not throw an error.
func (v *Vote) Create(ctx context.Context, pollID int, configReader io.Reader) error {
	var config PollConfig
	if err := json.NewDecoder(configReader).Decode(&config); err != nil {
		return MessageError{ErrInvalid, fmt.Sprintf("PollConfig is invalid json: %v", err)}
	}

	if err := config.validate(); err != nil {
		return fmt.Errorf("validating config: %w", err)
	}

	decoded, err := config.encode()
	if err != nil {
		return fmt.Errorf("encoding poll config: %w", err)
	}

	if err := v.config.SetConfig(ctx, pollID, decoded); err != nil {
		var errDoesExist interface{ DoesExist() }
		if errors.As(err, &errDoesExist) {
			return ErrExists
		}
		return fmt.Errorf("save config: %w", err)
	}
	return nil
}

// Stop ends a poll.
func (v *Vote) Stop(ctx context.Context, pollID int, w io.Writer) error {
	decodedConfig, err := v.config.Config(ctx, pollID)
	if err != nil {
		var errDoesExist interface{ DoesNotExist() }
		if errors.As(err, &errDoesExist) {
			return ErrNotExists
		}
		return fmt.Errorf("fetchig config: %w", err)
	}

	config, err := PollConfigFromJSON(decodedConfig)
	if err != nil {
		return fmt.Errorf("decoding config: %w", err)
	}

	backend := v.longBackend
	if config.Backend == "fast" {
		backend = v.fastBackend
	}

	objects, err := backend.Stop(ctx, pollID)
	if err != nil {
		return fmt.Errorf("fetching poll objects: %w", err)
	}

	encodableObjects := make([]json.RawMessage, len(objects))
	for i := range objects {
		encodableObjects[i] = objects[i]
	}

	if err := json.NewEncoder(w).Encode(encodableObjects); err != nil {
		return fmt.Errorf("encoding and sending objects: %w", err)
	}

	return nil
}

// Clear removes all knowlage of a poll.
func (v *Vote) Clear(ctx context.Context, pollID int) error {
	// TODO:
	//    * Clear poll in config and backend.
	return errors.New("TODO")
}

// Vote validates and saves the vote.
func (v *Vote) Vote(ctx context.Context, pollID int, r io.Reader) error {
	// TODO:
	//   * Read the config.
	//   * Read and validate the input.
	//   * Give the vote object to the backend. It checks, if the user has voted and that the vote is open.
	return errors.New("TODO")
}

// Configer gets and saves the config for a poll.
//
// The Method SetCofig has to return an error with the method `DoesExist()` if
// the config already exists and is not the same.
type Configer interface {
	Config(ctx context.Context, pollID int) ([]byte, error)
	SetConfig(ctx context.Context, pollID int, config []byte) error
	Clear(ctx context.Context, pollID int) error
}

// Backend is a storage for the poll options.
type Backend interface {
	Vote(ctx context.Context, pollID int, userID int, object []byte) error
	Stop(ctx context.Context, pollID int) ([][]byte, error)
	Clear(ctx context.Context, pollID int) error
}

// PollConfig is data needed to validate a vote.
type PollConfig struct {
	Backend       string          `json:"backend"`
	ContentObject genericRelation `json:"content_object_id"`

	// On motion poll and assignment poll.
	Anonymous bool   `json:"is_pseudoanonymized"`
	Method    string `json:"pollmethod"`
	Groups    []int  `json:"entitled_group_ids"`

	// Only on assignment poll.
	GlobalYes     bool `json:"global_yes"`
	GlobalNo      bool `json:"global_no"`
	GlobalAbstain bool `json:"global_abstain"`
	MultipleVotes bool `json:"multiple_votes"` // TODO: Not in models.yml
	MinAmount     int  `json:"min_votes_amount"`
	MaxAmount     int  `json:"max_votes_amount"`
}

// PollConfigFromJSON creates a new PollConfig object from a json input.
func PollConfigFromJSON(input []byte) (*PollConfig, error) {
	var config PollConfig
	json.Unmarshal(input, &config)
	return &config, nil
}

func (p *PollConfig) validate() error {
	// TODO: Implement all cases where the config is invalid.
	if p.ContentObject.collection != "motion" && p.ContentObject.collection != "assignment" {
		return MessageError{ErrInvalid, "poll config collection_object_id has to point to motion or assignment"}
	}

	if p.Backend != "fast" && p.Backend != "long" {
		return MessageError{ErrInvalid, "poll config backend has to be fast or long"}
	}
	return nil
}

// encode translates the config to json. The format is an internal detail and
// may change in the future.
func (p *PollConfig) encode() ([]byte, error) {
	return json.Marshal(p)
}

type genericRelation struct {
	collection string
	id         int
}

func (g *genericRelation) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, g.String())), nil
}

func (g *genericRelation) UnmarshalJSON(bs []byte) error {
	var s string
	if err := json.Unmarshal(bs, &s); err != nil {
		return err
	}

	parts := strings.Split(s, "/")
	if len(parts) != 2 {
		return fmt.Errorf("field has to contain exact one '/', got: %s", s)
	}

	g.collection = parts[0]

	id, err := strconv.Atoi(parts[1])
	if err != nil {
		return fmt.Errorf("second part of field has to be an int, got: %s", parts[1])
	}
	g.id = id
	return nil
}

func (g *genericRelation) String() string {
	return g.collection + "/" + strconv.Itoa(g.id)
}
