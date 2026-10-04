package mediadelivery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeInfrai struct {
	cronExpr string
	taskURL  string
	created  int
	captured int
}

func (f *fakeInfrai) CreateCron(_ context.Context, expr, task, _ string) (string, error) {
	f.created++
	f.cronExpr = expr
	f.taskURL = task
	return "job-42", nil
}

func (f *fakeInfrai) DeleteCron(context.Context, string, string) error { return nil }

func (f *fakeInfrai) CaptureError(context.Context, ExceptionPayload, string) error {
	f.captured++
	return nil
}

func TestProcessedAssetSchedulesCreatorDelivery(t *testing.T) {
	tests := []struct {
		name       string
		delayHours int
		wantCron   string
	}{
		{name: "two-hour review window", delayHours: 2, wantCron: "34 11 14 9 *"},
		{name: "cross-midnight delay", delayHours: 16, wantCron: "34 1 15 9 *"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeInfrai{}
			service := NewService(fake, "https://worker.example", nil)
			service.now = func() time.Time {
				return time.Date(2026, time.September, 14, 9, 34, 45, 0, time.UTC)
			}
			service.assets["asset-7"] = &Asset{
				ID: "asset-7", CreatorDeliveryURL: "https://creator.example/hook",
				DelayHours: tt.delayHours, State: StateProcessing,
			}

			req := httptest.NewRequest(http.MethodPost, "/assets/asset-7/processed", nil)
			req.SetPathValue("asset_id", "asset-7")
			res := httptest.NewRecorder()
			service.processed(res, req)

			if res.Code != http.StatusAccepted {
				t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
			}
			if fake.cronExpr != tt.wantCron {
				t.Fatalf("cron expression = %q, want %q", fake.cronExpr, tt.wantCron)
			}
			if fake.taskURL != "https://worker.example/callbacks/deliver/asset-7" {
				t.Fatalf("task URL = %q", fake.taskURL)
			}
			if service.assets["asset-7"].State != StateReady || service.assets["asset-7"].JobID != "job-42" {
				t.Fatalf("asset was not moved to ready with its job ID")
			}
		})
	}
}

func TestCallbackCapturesNotReadyFailure(t *testing.T) {
	fake := &fakeInfrai{}
	service := NewService(fake, "https://worker.example", nil)
	service.assets["asset-8"] = &Asset{ID: "asset-8", State: StateProcessing}
	req := httptest.NewRequest(http.MethodPost, "/callbacks/deliver/asset-8", nil)
	req.SetPathValue("asset_id", "asset-8")
	res := httptest.NewRecorder()

	service.deliver(res, req)

	if res.Code != http.StatusConflict || fake.captured != 1 {
		t.Fatalf("status = %d, captures = %d", res.Code, fake.captured)
	}
}

func TestClientDecodesBusinessErrorBeforeStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":    false,
			"error": map[string]string{"code": "BAD_INPUT", "message": "invalid schedule"},
		})
	}))
	defer server.Close()

	client := NewInfraiClient(server.URL, "test-key", server.Client())
	_, err := client.CreateCron(context.Background(), "0 12 1 1 *", "https://worker.example/callback", "test-create")
	apiErr, ok := err.(*APIError)
	if !ok || apiErr.HTTPStatus != http.StatusBadRequest || !strings.Contains(err.Error(), "invalid schedule") {
		t.Fatalf("error = %#v", err)
	}
}
