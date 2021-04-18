package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

const (
	keyConfig  = "vote_config_%d"
	keyStopped = "vote_stopped_%d"
	keyVote    = "vote_data_%d"
)

// Redis is a vote-Backend.
//
// Is tries to save the votes as fast as possible. All necessary checkes are
// done inside a lua-script so everything is done in one atomic step. It is
// expected that there is no backup from the redis database. Everyone with
// access to the redis database can see the vote results and how each user has
// voted.
//
// Has to be created with redis.New().
type Redis struct {
	pool *redis.Pool
}

// New creates an initializes Redis instance.
func New(addr string) *Redis {
	pool := redis.Pool{
		MaxActive:   100,
		Wait:        true,
		MaxIdle:     10,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
	}
	return &Redis{
		pool: &pool,
	}
}

// Wait blocks until a connection to redis can be established.
func (r *Redis) Wait(log func(format string, a ...interface{})) {
	for {
		conn := r.pool.Get()
		_, err := conn.Do("PING")
		conn.Close()
		if err == nil {
			return
		}
		if log != nil {
			log("Waiting for redis: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// Config returs the config for a poll.
func (r *Redis) Config(ctx context.Context, pollID int) ([]byte, error) {
	conn := r.pool.Get()
	defer conn.Close()

	key := fmt.Sprintf(keyConfig, pollID)

	bs, err := redis.Bytes(conn.Do("Get", key))
	if err != nil {
		return nil, fmt.Errorf("fetching config from key %s: %w", key, err)
	}
	return bs, nil
}

// SetConfig saves the config for a poll.
func (r *Redis) SetConfig(ctx context.Context, pollID int, config []byte) error {
	conn := r.pool.Get()
	defer conn.Close()

	key := fmt.Sprintf(keyConfig, pollID)

	_, err := conn.Do("SET", key, config)
	if err != nil {
		return fmt.Errorf("saving config to key %s: %w", key, err)
	}

	return nil
}

// luaVoteScript checks for condition and saves a vote if all checks pass.
//
// KEY[1] == stop key
// KEY[2] == vote data
// ARGV[1] == userID
// ARGV[2] == Vote object
//
// Returns 0 on success.
// Returns 1 if the poll was stopped.
// Returns 2 if the user has already voted.
const luaVoteScript = `
local stopped = redis.call("EXISTS",KEYS[1])
if stopped == 1 then
	return 1
end

local saved = redis.call("HSETNX",KEYS[2],ARGV[1],ARGV[2])
if saved == 0 then
	return 2
end

return 0`

// Vote saves a vote in redis.
//
// It also checks, that the user did not vote before and that the poll is open.
func (r *Redis) Vote(ctx context.Context, pollID int, userID int, object []byte) error {
	conn := r.pool.Get()
	defer conn.Close()

	vKey := fmt.Sprintf(keyVote, pollID)
	sKey := fmt.Sprintf(keyStopped, pollID)

	result, err := redis.Int(conn.Do("EVAL", luaVoteScript, 2, sKey, vKey, userID, object))
	if err != nil {
		return fmt.Errorf("executing luaVoteScript: %w", err)
	}
	if result == 1 {
		return stoppedPollError{}
	}
	if result == 2 {
		return doupleVoteError{}
	}
	if result != 0 {
		return fmt.Errorf("lua returned with %d", result)
	}
	return nil
}

// Stop ends a poll.
//
// It returns all vote objects.
func (r *Redis) Stop(ctx context.Context, pollID int) ([][]byte, error) {
	conn := r.pool.Get()
	defer conn.Close()

	vKey := fmt.Sprintf(keyVote, pollID)
	sKey := fmt.Sprintf(keyStopped, pollID)

	if _, err := conn.Do("SET", sKey, "stopped"); err != nil {
		return nil, fmt.Errorf("set key %s to stopped", sKey)
	}

	voteObjects, err := redis.ByteSlices(conn.Do("HVALS", vKey))
	if err != nil {
		return nil, fmt.Errorf("getting vote objects from %s: %w", vKey, err)
	}
	return voteObjects, nil
}

// Clear delete all information from a poll.
func (r *Redis) Clear(ctx context.Context, pollID int) error {
	conn := r.pool.Get()
	defer conn.Close()

	vKey := fmt.Sprintf(keyVote, pollID)
	sKey := fmt.Sprintf(keyStopped, pollID)
	cKey := fmt.Sprintf(keyConfig, pollID)

	if _, err := conn.Do("DEL", vKey, sKey, cKey); err != nil {
		return fmt.Errorf("removing keys: %w", err)
	}
	return nil
}

type doupleVoteError struct{}

func (doupleVoteError) Error() string {
	return "User has already voted"
}

func (doupleVoteError) DoupleVote() {}

type stoppedPollError struct{}

func (stoppedPollError) Error() string {
	return "poll is stopped"
}

func (stoppedPollError) Stopped() {}
