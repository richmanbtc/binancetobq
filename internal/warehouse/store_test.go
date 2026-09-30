package warehouse

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"collector/internal/model"

	"cloud.google.com/go/bigquery"
	bqapi "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
)

func testWarehouseClient(t *testing.T, ctx context.Context, server *httptest.Server) *bigquery.Client {
	t.Helper()
	client, err := bigquery.NewClient(ctx, "test-project", option.WithEndpoint(server.URL), option.WithoutAuthentication(), option.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatal("creating test client failed")
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func TestLoadJobConflictResolvesExistingJob(t *testing.T) {
	for _, target := range []struct{ dataset, project string }{
		{"test_dataset", "test-project"},
		{"destination-project.test_dataset", "destination-project"},
	} {
		t.Run(target.project, func(t *testing.T) {
			for _, loseAcknowledgement := range []bool{false, true} {
				t.Run(fmt.Sprint(loseAcknowledgement), func(t *testing.T) {
					var id string
					posts, gets, datasets := 0, 0, 0
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.Header().Set("Content-Type", "application/json")
						if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/datasets/test_dataset") {
							datasets++
							if !strings.Contains(r.URL.Path, "/projects/"+target.project+"/datasets/") {
								t.Error("dataset location lookup used the wrong project")
							}
							fmt.Fprint(w, `{"location":"test-region"}`)
							return
						}
						if r.Method == http.MethodPost {
							posts++
							_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
							if err != nil {
								t.Error("invalid multipart upload")
								w.WriteHeader(400)
								return
							}
							reader := multipart.NewReader(r.Body, params["boundary"])
							part, err := reader.NextPart()
							if err != nil {
								t.Error("missing job metadata")
								w.WriteHeader(400)
								return
							}
							var job bqapi.Job
							if json.NewDecoder(part).Decode(&job) != nil || job.JobReference == nil || job.Configuration == nil || job.Configuration.Load == nil || job.Configuration.Load.Schema == nil {
								t.Error("invalid job metadata")
								return
							}
							destination := job.Configuration.Load.DestinationTable
							if destination == nil || destination.ProjectId != target.project || destination.DatasetId != "test_dataset" {
								t.Error("load used the wrong destination")
							}
							if job.JobReference.ProjectId != "test-project" {
								t.Error("load changed the job project")
							}
							if job.JobReference.Location != "" {
								t.Error("load location should be inferred")
							}
							if job.JobReference.JobId == "" || job.Configuration.Load.WriteDisposition != "WRITE_APPEND" {
								t.Error("missing stable job identity or append mode")
							}
							if job.Configuration.Load.CreateDisposition != "CREATE_NEVER" {
								t.Error("load could recreate a table without defaults")
							}
							for _, field := range job.Configuration.Load.Schema.Fields {
								if field.Mode != "REQUIRED" {
									t.Errorf("load field %s must be REQUIRED", field.Name)
								}
								if field.Name == "ingested_at" {
									t.Error("input schema overrides destination default")
								}
							}
							if id != "" && id != job.JobReference.JobId {
								t.Error("retry changed the job ID")
							}
							id = job.JobReference.JobId
							part, err = reader.NextPart()
							if err != nil {
								t.Error("missing row payload")
								w.WriteHeader(400)
								return
							}
							body, _ := io.ReadAll(part)
							if strings.Contains(string(body), `"ingested_at"`) {
								t.Error("payload overrides destination default")
							}
							if !strings.Contains(string(body), `"timestamp":0`) {
								t.Error("missing serialized row")
							}
							// Lose the first acknowledgement after accepting the upload.
							if loseAcknowledgement && posts == 1 {
								conn, _, err := w.(http.Hijacker).Hijack()
								if err != nil {
									t.Error("hijacking test connection failed")
									return
								}
								conn.Close()
								return
							}
							w.WriteHeader(409)
							fmt.Fprint(w, `{"error":{"code":409,"message":"synthetic conflict"}}`)
							return
						}
						gets++
						if r.URL.Query().Get("location") != "test-region" {
							t.Error("job lookup did not use dataset location")
						}
						if id == "" || !strings.HasSuffix(r.URL.Path, "/jobs/"+id) {
							t.Error("did not retrieve the original job")
						}
						fmt.Fprintf(w, `{"jobReference":{"projectId":"test-project","jobId":%q,"location":"test-region"},"status":{"state":"DONE"}}`, id)
					}))
					defer server.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					client := testWarehouseClient(t, ctx, server)
					store := Store{client: client, config: Options{Dataset: target.dataset, Tables: map[int64]string{300: "test_rows"}}}
					if err := store.Append(ctx, 300, []model.Row{{Symbol: "TEST", Time: 0}}); err != nil {
						t.Fatal(err)
					}
					wantPosts := 1
					if loseAcknowledgement {
						wantPosts = 2
					}
					if posts != wantPosts || gets == 0 || datasets != 1 {
						t.Fatal("load was resubmitted instead of resolving the existing job")
					}
				})
			}
		})
	}
}

func TestPrepareIngestionTimestamp(t *testing.T) {
	for _, target := range []struct{ dataset, project string }{
		{"test_dataset", "test-project"},
		{"destination-project.test_dataset", "destination-project"},
	} {
		t.Run(target.project, func(t *testing.T) {
			for _, mode := range []string{"new", "missing", "no default", "ready", "wrong type", "denied"} {
				t.Run(mode, func(t *testing.T) {
					ready := mode == "ready"
					creates, migrations := 0, 0
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.Header().Set("Content-Type", "application/json")
						if strings.Contains(r.URL.Path, "/jobs") {
							if r.Method == http.MethodPost {
								migrations++
								var request bqapi.Job
								if json.NewDecoder(r.Body).Decode(&request) != nil || request.Configuration == nil || request.Configuration.Query == nil {
									t.Error("invalid query job")
									return
								}
								sql := request.Configuration.Query.Query
								if !strings.Contains(sql, "`"+target.project+".test_dataset.test_rows`") {
									t.Error("migration used the wrong destination")
								}
								if !strings.Contains(sql, "ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMP;") || !strings.Contains(sql, "SET DEFAULT CURRENT_TIMESTAMP()") {
									t.Error("migration did not add column and set its default separately")
								}
								ready = true
							}
							fmt.Fprint(w, `{"jobReference":{"projectId":"test-project","jobId":"test-job","location":"test-region"},"status":{"state":"DONE"}}`)
							return
						}
						if !strings.Contains(r.URL.Path, "/projects/"+target.project+"/datasets/test_dataset/tables") {
							t.Error("table preparation used the wrong destination")
						}
						if mode == "denied" {
							w.WriteHeader(403)
							fmt.Fprint(w, `{"error":{"code":403}}`)
							return
						}
						if r.Method == http.MethodPost {
							creates++
							var request bqapi.Table
							if json.NewDecoder(r.Body).Decode(&request) != nil || request.Schema == nil {
								t.Error("invalid table creation")
								return
							}
							for _, field := range request.Schema.Fields {
								if field.Name == "ingested_at" && field.Type == "TIMESTAMP" && field.DefaultValueExpression == "CURRENT_TIMESTAMP()" {
									ready = true
								}
							}
							if !ready {
								t.Error("new table lacks timestamp default")
							}
						}
						if mode == "new" && !ready {
							w.WriteHeader(404)
							fmt.Fprint(w, `{"error":{"code":404}}`)
							return
						}
						fields := []map[string]string{{"name": "symbol", "type": "STRING"}}
						if ready || mode == "no default" || mode == "wrong type" {
							field := map[string]string{"name": "ingested_at", "type": "TIMESTAMP", "mode": "NULLABLE"}
							if ready {
								field["defaultValueExpression"] = "CURRENT_TIMESTAMP()"
							}
							if mode == "wrong type" {
								field["type"] = "STRING"
							}
							fields = append(fields, field)
						}
						_ = json.NewEncoder(w).Encode(map[string]any{"schema": map[string]any{"fields": fields}})
					}))
					defer server.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					defer cancel()
					client := testWarehouseClient(t, ctx, server)
					store := Store{client: client, config: Options{Project: "test-project", Dataset: target.dataset, Tables: map[int64]string{300: "test_rows"}}}
					err := store.prepareTable(ctx, 300)
					if mode == "wrong type" || mode == "denied" {
						if err == nil || creates != 0 || migrations != 0 {
							t.Fatal("unsafe schema or permission error ignored")
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					if err := store.prepareTable(ctx, 300); err != nil {
						t.Fatal(err)
					}
					wantCreate, wantMigration := 0, 0
					if mode == "new" {
						wantCreate = 1
					}
					if mode == "missing" || mode == "no default" {
						wantMigration = 1
					}
					if creates != wantCreate || migrations != wantMigration {
						t.Fatal("setup was not idempotent")
					}
				})
			}
		})
	}
}

func TestLoadJobPermanentFailuresDoNotWaitForDeadline(t *testing.T) {
	for _, stage := range []string{"submission", "polling", "job"} {
		t.Run(stage, func(t *testing.T) {
			posts, gets := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodPost {
					posts++
				} else {
					gets++
				}
				if stage == "submission" || (stage == "polling" && r.Method == http.MethodGet) {
					w.WriteHeader(403)
					fmt.Fprint(w, `{"error":{"code":403}}`)
					return
				}
				status := `{"state":"RUNNING"}`
				if stage == "job" && r.Method == http.MethodGet {
					status = `{"state":"DONE","errorResult":{"reason":"invalid"}}`
				}
				fmt.Fprintf(w, `{"jobReference":{"projectId":"test-project","jobId":"test-job","location":"test-region"},"status":%s}`, status)
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			client := testWarehouseClient(t, ctx, server)
			store := Store{client: client, config: Options{Dataset: "test_dataset", Tables: map[int64]string{300: "test_rows"}}}
			err := store.Append(ctx, 300, []model.Row{{Symbol: "TEST"}})
			if err == nil || ctx.Err() != nil || posts != 1 || gets > 1 {
				t.Fatalf("permanent load failure was retried: %v", err)
			}
		})
	}
}
func TestSchemaCompatibility(t *testing.T) {
	for _, interval := range []int64{300, 3600} {
		hasFive := false
		for _, field := range schema(interval) {
			if !field.Required {
				t.Fatalf("field %s must be REQUIRED", field.Name)
			}
			if field.Name == "twap_5m" {
				hasFive = true
			}
			if field.Name == "timestamp" && field.Type != "INTEGER" {
				t.Fatal("timestamp type changed")
			}
		}
		if hasFive != (interval == 3600) {
			t.Fatal("interval schema mismatch")
		}
	}
}
