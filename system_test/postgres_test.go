package system_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/OpenSlides/openslides-autoupdate-service/pkg/datastore/dskey"
	"github.com/jackc/pgx/v5"
)

const (
	dbUser     = "openslides"
	dbPassword = "openslides"
	dbHost     = "localhost"
	dbPort     = "5432"
	dbName     = "openslides"
)

type postgresTestData struct {
	pgxConfig *pgx.ConnConfig
}

func newPostgresTestData(ctx context.Context) (p *postgresTestData, err error) {
	addr := fmt.Sprintf(
		`user=%s password='%s' host=%s port=%s dbname=%s`,
		dbUser,
		dbPassword,
		dbHost,
		dbPort,
		dbName,
	)
	config, err := pgx.ParseConfig(addr)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	ptd := postgresTestData{
		pgxConfig: config,
	}

	defer func() {
		if err != nil {
			if err := ptd.Close(ctx); err != nil {
				log.Printf("Closing postgres: %v", err)
			}
		}
	}()

	if err := ptd.addSchema(ctx); err != nil {
		return nil, fmt.Errorf("add schema: %w", err)
	}

	return &ptd, nil
}

func (p *postgresTestData) Close(ctx context.Context) error {
	if err := p.dropData(ctx); err != nil {
		return fmt.Errorf("remove old data: %w", err)
	}

	return nil
}

func (p *postgresTestData) conn(ctx context.Context) (*pgx.Conn, error) {
	var conn *pgx.Conn

	for {
		var err error
		if p == nil {
			return nil, fmt.Errorf("some error")
		}
		conn, err = pgx.ConnectConfig(ctx, p.pgxConfig)
		if err == nil {
			return conn, nil
		}

		select {
		case <-time.After(200 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (p *postgresTestData) addSchema(ctx context.Context) error {
	// Schema from datastore-repo
	schema := `
	CREATE TABLE IF NOT EXISTS models (
		fqid VARCHAR(48) PRIMARY KEY,
		data JSONB NOT NULL,
		deleted BOOLEAN NOT NULL
	);`
	conn, err := p.conn(ctx)
	if err != nil {
		return fmt.Errorf("creating connection: %w", err)
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, schema); err != nil {
		return fmt.Errorf("adding schema: %w", err)
	}
	return nil
}

func (p *postgresTestData) addTestData(ctx context.Context, data map[dskey.Key][]byte) error {
	objects := make(map[string]map[string]json.RawMessage)
	for k, v := range data {
		fqid := k.FQID()
		if _, ok := objects[fqid]; !ok {
			objects[fqid] = make(map[string]json.RawMessage)
		}
		objects[fqid][k.Field()] = v
	}

	conn, err := p.conn(ctx)
	if err != nil {
		return fmt.Errorf("creating connection: %w", err)
	}
	defer conn.Close(ctx)

	for fqid, data := range objects {
		encoded, err := json.Marshal(data)
		if err != nil {
			return fmt.Errorf("encode %v: %v", data, err)
		}

		sql := fmt.Sprintf(`INSERT INTO models (fqid, data, deleted) VALUES ('%s', '%s', false);`, fqid, encoded)
		if _, err := conn.Exec(ctx, sql); err != nil {
			return fmt.Errorf("executing psql `%s`: %w", sql, err)
		}
	}

	return nil
}

func (p *postgresTestData) dropData(ctx context.Context) error {
	conn, err := p.conn(ctx)
	if err != nil {
		return fmt.Errorf("creating connection: %w", err)
	}
	defer conn.Close(ctx)

	sql := `TRUNCATE models;`
	if _, err := conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("executing psql `%s`: %w", sql, err)
	}

	return nil
}
