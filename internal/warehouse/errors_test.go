package warehouse

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
)

func TestLoadErrorDetailsExcludeFreeFormValues(t *testing.T) {
	message := "Provided Schema does not match Table private-project:private_dataset.private_table. Field timestamp has changed type from INTEGER to TIMESTAMP. Authorization: Bearer synthetic-secret"
	got := loadErrorDetail(message)
	if got != "Provided Schema does not match; Field timestamp has changed type from INTEGER to TIMESTAMP" {
		t.Fatalf("unexpected detail: %s", got)
	}
	if got := loadErrorDetail("Field private_field has changed type from STRING to INTEGER"); got != "unrecognized detail omitted" {
		t.Fatalf("unknown field leaked: %s", got)
	}
	if got := loadErrorDetail("credential=synthetic-secret https://private.example"); got != "unrecognized detail omitted" {
		t.Fatalf("free-form text leaked: %s", got)
	}
}

func TestLoadErrorLoggingStages(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	store := &Store{}
	for _, stage := range []string{"submission", "polling"} {
		store.logLoadError(stage, 300, &googleapi.Error{Code: 400, Message: "Provided Schema does not match Table private-destination", Errors: []googleapi.ErrorItem{{Reason: "invalid"}}})
	}
	store.logLoadError("job", 3600, &bigquery.Error{Reason: "invalid", Message: "Invalid JSON private-data", Location: "private-location"})
	got := output.String()
	for _, expected := range []string{"stage=submission", "stage=polling", "stage=job", "status=400", "reason=invalid", "Provided Schema does not match", "Invalid JSON"} {
		if !strings.Contains(got, expected) {
			t.Fatalf("missing %q in %s", expected, got)
		}
	}
	if strings.Contains(got, "private-") {
		t.Fatalf("private value leaked: %s", got)
	}
}
