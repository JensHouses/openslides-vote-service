package backend

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/OpenSlides/openslides-autoupdate-service/pkg/environment"
	"github.com/OpenSlides/openslides-vote-service/backend/memory"
	"github.com/OpenSlides/openslides-vote-service/backend/postgres"
	"github.com/OpenSlides/openslides-vote-service/backend/redis"
	"github.com/OpenSlides/openslides-vote-service/vote"
)

var (
	envRedisHost = environment.NewVariable("CACHE_HOST", "localhost", "Host of the redis used for the fast backend.")
	envRedisPort = environment.NewVariable("CACHE_PORT", "6379", "Port of the redis used for the fast backend.")

	envPostgresHost         = environment.NewVariable("VOTE_DATABASE_HOST", "localhost", "Host of the postgres database used for long polls.")
	envPostgresPort         = environment.NewVariable("VOTE_DATABASE_PORT", "5432", "Port of the postgres database used for long polls.")
	envPostgresUser         = environment.NewVariable("VOTE_DATABASE_USER", "openslides", "Databasename of the postgres database used for long polls.")
	envPostgresDatabase     = environment.NewVariable("VOTE_DATABASE_NAME", "openslides", "Name of the database to save long running polls.")
	envPostgresPasswordFile = environment.NewVariable("VOTE_DATABASE_PASSWORD_FILE", "/run/secrets/postgres_password", "Password of the postgres database used for long polls.")

	envSingleInstance = environment.NewVariable("VOTE_SINGLE_INSTANCE", "false", "More performance if the serice is not scalled horizontally.")
)

// Build builds a fast and a long backends from the environment.
func Build(lookup environment.Environmenter) (fast, long func(context.Context) (vote.Backend, error), singleInstance bool, err error) {
	// All environment variables have to be called in this function and not in a
	// sub function. In other case they will not be included in the generated
	// file environment.md.

	buildMemory := func(_ context.Context) (vote.Backend, error) {
		return memory.New(), nil
	}

	redisAddr := envRedisHost.Value(lookup) + ":" + envRedisPort.Value(lookup)
	buildRedis := func(ctx context.Context) (vote.Backend, error) {
		r := redis.New(redisAddr)
		r.Wait(ctx)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		return r, nil
	}

	dbPassword, err := environment.ReadSecret(lookup, envPostgresPasswordFile)
	if err != nil {
		return nil, nil, false, fmt.Errorf("reading postgres password: %w", err)
	}

	postgresAddr := fmt.Sprintf(
		`user='%s' password='%s' host='%s' port='%s' dbname='%s'`,
		encodePostgresConfig(envPostgresUser.Value(lookup)),
		dbPassword,
		encodePostgresConfig(envPostgresHost.Value(lookup)),
		encodePostgresConfig(envPostgresPort.Value(lookup)),
		encodePostgresConfig(envPostgresDatabase.Value(lookup)),
	)

	buildPostgres := func(ctx context.Context) (vote.Backend, error) {
		p, err := postgres.New(ctx, postgresAddr)
		if err != nil {
			return nil, fmt.Errorf("creating postgres connection pool: %w", err)
		}

		p.Wait(ctx)
		if err := p.Migrate(ctx); err != nil {
			return nil, fmt.Errorf("creating shema: %w", err)
		}
		return p, nil
	}

	long = buildPostgres
	fast = buildRedis
	singleInstace, _ := strconv.ParseBool(envSingleInstance.Value(lookup))
	if singleInstace {
		fast = buildMemory
	}

	return fast, long, singleInstace, nil
}

// encodePostgresConfig encodes a string to be used in the postgres key value style.
//
// See: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
func encodePostgresConfig(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return s
}
