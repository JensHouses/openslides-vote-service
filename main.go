package main

import (
	"context"
	"errors"
	"fmt"
	golog "log"
	"os"
	"strconv"

	"github.com/OpenSlides/openslides-autoupdate-service/pkg/auth"
	"github.com/OpenSlides/openslides-autoupdate-service/pkg/environment"
	messageBusRedis "github.com/OpenSlides/openslides-autoupdate-service/pkg/redis"
	"github.com/OpenSlides/openslides-vote-service/backend"
	"github.com/OpenSlides/openslides-vote-service/log"
	"github.com/OpenSlides/openslides-vote-service/vote"
	"github.com/OpenSlides/openslides-vote-service/vote/http"
	"github.com/alecthomas/kong"
)

var envDebugLog = environment.NewVariable("VOTE_DEBUG_LOG", "false", "Show debug log.")

//go:generate  sh -c "go run main.go build-doc > environment.md"

var cli struct {
	Run      struct{} `cmd:"" help:"Runs the service." default:"withargs"`
	BuildDoc struct{} `cmd:"" help:"Build the environment documentation."`
	Health   struct {
		Host     string `help:"Host of the service" short:"h" default:"localhost"`
		Port     string `help:"Port of the service" short:"p" default:"9013" env:"VOTE_PORT"`
		UseHTTPS bool   `help:"Use https to connect to the service" short:"s"`
		Insecure bool   `help:"Accept invalid cert" short:"k"`
	} `cmd:"" help:"Runs a health check."`
}

func main() {
	ctx, cancel := environment.InterruptContext()
	defer cancel()
	log.SetInfoLogger(golog.Default())

	kongCTX := kong.Parse(&cli, kong.UsageOnError())
	switch kongCTX.Command() {
	case "run":
		if err := contextDone(run(ctx)); err != nil {
			handleError(err)
			os.Exit(1)
		}

	case "build-doc":
		if err := contextDone(buildDocu()); err != nil {
			handleError(err)
			os.Exit(1)
		}

	case "health":
		if err := contextDone(http.HealthClient(ctx, cli.Health.UseHTTPS, cli.Health.Host, cli.Health.Port, cli.Health.Insecure)); err != nil {
			handleError(err)
			os.Exit(1)
		}
	}
}

func run(ctx context.Context) error {
	lookup := new(environment.ForProduction)

	if debug, _ := strconv.ParseBool(envDebugLog.Value(lookup)); debug {
		log.SetDebugLogger(golog.Default())
	}

	service, err := initService(lookup)
	if err != nil {
		return fmt.Errorf("init services: %w", err)
	}

	return service(ctx)
}

func buildDocu() error {
	lookup := new(environment.ForDocu)

	if _, err := initService(lookup); err != nil {
		return fmt.Errorf("init services: %w", err)
	}

	doc, err := lookup.BuildDoc()
	if err != nil {
		return fmt.Errorf("build doc: %w", err)
	}

	fmt.Println(doc)
	return nil
}

// initService initializes all packages needed for the vote service.
//
// Returns a the service as callable.
func initService(lookup environment.Environmenter) (func(context.Context) error, error) {
	var backgroundTasks []func(context.Context, func(error))

	httpServer := http.New(lookup)

	// Redis as message bus for datastore and logout events.
	messageBus := messageBusRedis.New(lookup)

	// Datastore Service.
	database, err := vote.Flow(lookup, messageBus)
	if err != nil {
		return nil, fmt.Errorf("init database: %w", err)
	}

	// Auth Service.
	authService, authBackground, err := auth.New(lookup, messageBus)
	if err != nil {
		return nil, fmt.Errorf("init auth system: %w", err)
	}
	backgroundTasks = append(backgroundTasks, authBackground)

	fastBackendStarter, longBackendStarter, singleInstance, err := backend.Build(lookup)
	if err != nil {
		return nil, fmt.Errorf("init vote backend: %w", err)
	}

	service := func(ctx context.Context) error {
		fastBackend, err := fastBackendStarter(ctx)
		if err != nil {
			return fmt.Errorf("start fast backend: %w", err)
		}

		longBackend, err := longBackendStarter(ctx)
		if err != nil {
			return fmt.Errorf("start long backend: %w", err)
		}

		voteService, voteBackground, err := vote.New(ctx, fastBackend, longBackend, database, singleInstance)
		if err != nil {
			return fmt.Errorf("starting service: %w", err)
		}
		backgroundTasks = append(backgroundTasks, voteBackground)

		for _, bg := range backgroundTasks {
			go bg(ctx, handleError)
		}

		return httpServer.Run(ctx, authService, voteService)
	}

	return service, nil
}

// contextDone returns an empty error if the context is done or exceeded
func contextDone(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return err
}

// handleError handles an error.
//
// Ignores context closed errors.
func handleError(err error) {
	if contextDone(err) == nil {
		return
	}

	log.Info("Error: %v", err)
}
