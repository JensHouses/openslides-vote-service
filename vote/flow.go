package vote

import (
	"fmt"

	"github.com/OpenSlides/openslides-autoupdate-service/pkg/datastore"
	"github.com/OpenSlides/openslides-autoupdate-service/pkg/datastore/cache"
	"github.com/OpenSlides/openslides-autoupdate-service/pkg/datastore/flow"
	"github.com/OpenSlides/openslides-autoupdate-service/pkg/environment"
)

// Flow initializes a cached connection to postgres.
func Flow(lookup environment.Environmenter, messageBus flow.Updater) (flow.Flow, error) {
	postgres, err := datastore.NewFlowPostgres(lookup, messageBus)
	if err != nil {
		return nil, fmt.Errorf("init postgres: %w", err)
	}

	cache := cache.New(postgres)

	return cache, nil
}
