package warehouse

import (
	"errors"
	"log/slog"
	"regexp"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
)

// Only recognized diagnostic clauses are logged. Free-form API messages may
// contain configuration, credentials or input data, even after simple masking.
var loadDiagnostic = regexp.MustCompile(`(?i)Provided Schema does not match|Field ([a-z_][a-z_0-9]*) has changed (type|mode) from (INTEGER|INT64|FLOAT|FLOAT64|STRING|TIMESTAMP|DATE|DATETIME|NUMERIC|BIGNUMERIC|BOOLEAN|BOOL|BYTES|RECORD|JSON|NULLABLE|REQUIRED|REPEATED) to (INTEGER|INT64|FLOAT|FLOAT64|STRING|TIMESTAMP|DATE|DATETIME|NUMERIC|BIGNUMERIC|BOOLEAN|BOOL|BYTES|RECORD|JSON|NULLABLE|REQUIRED|REPEATED)\b|Cannot add fields|Missing required field|No such field|Too many errors|Invalid JSON|JSON parsing error|Invalid timestamp|Access Denied|Permission denied|Quota exceeded|Not found|Invalid job ID|Invalid schema|Empty schema`)

func loadErrorDetail(message string) string {
	var details []string
	for _, match := range loadDiagnostic.FindAllStringSubmatch(message, -1) {
		if match[1] != "" {
			known := false
			for _, field := range schema(3600) {
				if strings.EqualFold(field.Name, match[1]) {
					known = true
					break
				}
			}
			if !known {
				continue
			}
		}
		details = append(details, match[0])
		if len(details) == 8 {
			break
		}
	}
	if len(details) == 0 {
		return "unrecognized detail omitted"
	}
	return strings.Join(details, "; ")
}

func loadErrorReason(reason string) string {
	switch reason {
	case "invalid", "invalidQuery", "notFound", "duplicate", "accessDenied", "forbidden", "backendError", "internalError", "rateLimitExceeded", "jobRateLimitExceeded", "quotaExceeded", "resourcesExceeded", "billingNotEnabled", "stopped":
		return reason
	default:
		return "unknown"
	}
}

func (s *Store) logLoadError(stage string, interval int64, err error) {
	logDetail := func(reason, message string) {
		slog.Error("warehouse load failed", "stage", stage, "interval_seconds", interval, "status", apiStatus(err), "reason", loadErrorReason(reason), "detail", loadErrorDetail(message))
	}
	var api *googleapi.Error
	if errors.As(err, &api) {
		if len(api.Errors) == 0 {
			logDetail("", api.Message)
			return
		}
		for i, item := range api.Errors {
			if i == 8 {
				break
			}
			logDetail(item.Reason, api.Message+"; "+item.Message)
		}
		return
	}
	var job *bigquery.Error
	if errors.As(err, &job) {
		logDetail(job.Reason, job.Message)
		return
	}
	logDetail("", "")
}
