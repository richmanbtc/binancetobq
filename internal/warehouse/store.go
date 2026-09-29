package warehouse

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"collector/internal/model"
	"collector/internal/retry"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

type Store struct {
	client *bigquery.Client
	config Options
}

type Options struct {
	Project, Dataset string
	Symbols          []string
	Tables           map[int64]string
	Intervals        []int64
}

func New(ctx context.Context, o Options) (*Store, error) {
	client, err := bigquery.NewClient(ctx, o.Project)
	if err != nil {
		return nil, errors.New("initializing warehouse client failed")
	}
	return &Store{client: client, config: o}, nil
}

func schema(interval int64) bigquery.Schema {
	t := reflect.TypeFor[model.Row]()
	var result bigquery.Schema
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Name == "TWAP5M" && interval == 300 {
			continue
		}
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		kind := bigquery.FloatFieldType
		if field.Name == "Symbol" {
			kind = bigquery.StringFieldType
		}
		if field.Name == "Time" {
			kind = bigquery.IntegerFieldType
		}
		result = append(result, &bigquery.FieldSchema{Name: name, Type: kind})
	}
	return result
}

func apiStatus(err error) int {
	var e *googleapi.Error
	if errors.As(err, &e) {
		return e.Code
	}
	return 0
}

func permanentAPIError(err error) bool {
	code := apiStatus(err)
	return code >= 400 && code < 500 && code != 408 && code != 429
}

// The destination supplies this field; it must stay out of the load input schema.
func (s *Store) prepareTable(ctx context.Context, interval int64) error {
	c := s.config
	table := s.client.Dataset(c.Dataset).Table(c.Tables[interval])
	metadata, err := table.Metadata(ctx)
	if apiStatus(err) == 404 {
		fields := append(schema(interval), &bigquery.FieldSchema{
			Name: "ingested_at", Type: bigquery.TimestampFieldType,
			DefaultValueExpression: "CURRENT_TIMESTAMP()",
		})
		err = table.Create(ctx, &bigquery.TableMetadata{Schema: fields})
		if err == nil {
			return nil
		}
		if apiStatus(err) == 409 {
			metadata, err = table.Metadata(ctx)
		}
	}
	if err != nil {
		return errors.New("preparing warehouse table failed; check table read/create permissions")
	}
	for _, field := range metadata.Schema {
		if !strings.EqualFold(field.Name, "ingested_at") {
			continue
		}
		if field.Type != bigquery.TimestampFieldType || field.Repeated || field.Required {
			return errors.New("ingested_at must be a nullable TIMESTAMP")
		}
		if strings.EqualFold(strings.TrimSpace(field.DefaultValueExpression), "CURRENT_TIMESTAMP()") {
			return nil
		}
	}
	// Existing rows remain NULL. Separate statements also recover a partial migration.
	name := fmt.Sprintf("`%s.%s.%s`", c.Project, c.Dataset, c.Tables[interval])
	q := s.client.Query(fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMP; ALTER TABLE %s ALTER COLUMN ingested_at SET DEFAULT CURRENT_TIMESTAMP()", name, name))
	job, err := q.Run(ctx)
	if err != nil {
		return errors.New("setting ingestion timestamp failed; check schema update and query permissions")
	}
	status, err := job.Wait(ctx)
	if err != nil || status.Err() != nil {
		return errors.New("ingestion timestamp migration failed")
	}
	return nil
}

func (s *Store) Checkpoints(ctx context.Context) (model.Checkpoints, error) {
	c := s.config
	result := make(model.Checkpoints)
	for _, interval := range c.Intervals {
		if err := s.prepareTable(ctx, interval); err != nil {
			return nil, err
		}
		result[interval] = make(map[string]int64)
		// Identifiers are validated in the application configuration; values use query parameters.
		q := s.client.Query(fmt.Sprintf("SELECT symbol, MAX(timestamp) AS last_time FROM `%s.%s.%s` WHERE symbol IN UNNEST(@symbols) GROUP BY symbol", c.Project, c.Dataset, c.Tables[interval]))
		q.Parameters = []bigquery.QueryParameter{{Name: "symbols", Value: c.Symbols}}
		it, err := q.Read(ctx)
		if err != nil {
			return nil, errors.New("reading warehouse checkpoints failed")
		}
		var row struct {
			Symbol string             `bigquery:"symbol"`
			Last   bigquery.NullInt64 `bigquery:"last_time"`
		}
		for err := it.Next(&row); err != iterator.Done; err = it.Next(&row) {
			if err != nil {
				return nil, errors.New("reading warehouse checkpoint rows failed")
			}
			if row.Last.Valid {
				result[interval][row.Symbol] = row.Last.Int64
			}
		}
	}
	return result, nil
}

func (s *Store) Append(ctx context.Context, interval int64, rows []model.Row) error {
	var payload bytes.Buffer
	enc := json.NewEncoder(&payload)
	for _, row := range rows {
		if enc.Encode(row) != nil {
			return errors.New("encoding warehouse rows failed")
		}
	}
	jobID := "collector_" + rand.Text()
	var job *bigquery.Job
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if job == nil {
			source := bigquery.NewReaderSource(bytes.NewReader(payload.Bytes()))
			source.SourceFormat = bigquery.JSON
			source.Schema = schema(interval)
			loader := s.client.Dataset(s.config.Dataset).Table(s.config.Tables[interval]).LoaderFrom(source)
			loader.WriteDisposition = bigquery.WriteAppend
			loader.CreateDisposition = bigquery.CreateNever
			loader.JobID = jobID
			var err error
			job, err = loader.Run(ctx)
			if apiStatus(err) == 409 {
				// Regional job lookup needs a location even when submission inferred it.
				metadata, lookupErr := s.client.Dataset(s.config.Dataset).Metadata(ctx)
				err = lookupErr
				if err == nil {
					job, err = s.client.JobFromIDLocation(ctx, jobID, metadata.Location)
				}
			}
			if permanentAPIError(err) {
				return fmt.Errorf("warehouse load submission failed (HTTP %d)", apiStatus(err))
			}
		}
		if job != nil {
			status, err := job.Wait(ctx)
			switch {
			case permanentAPIError(err):
				return fmt.Errorf("reading warehouse load status failed (HTTP %d)", apiStatus(err))
			case err == nil && status.Err() != nil:
				return errors.New("warehouse load job failed")
			case err == nil:
				return nil
			}
		}
		// Ambiguous transport failures reuse the same job ID, never blindly append again.
		if err := retry.Wait(ctx, retry.Delay(attempt)); err != nil {
			return err
		}
	}
}
