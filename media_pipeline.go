package mediadelivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type AssetState string

const (
	StateProcessing AssetState = "processing"
	StateReady      AssetState = "ready"
	StateDelivered  AssetState = "delivered"
	StateFailed     AssetState = "failed"
)

type Asset struct {
	ID                 string     `json:"asset_id"`
	CreatorDeliveryURL string     `json:"creator_delivery_url"`
	DelayHours         int        `json:"delay_hours"`
	State              AssetState `json:"state"`
	JobID              string     `json:"job_id,omitempty"`
}

type Scheduler interface {
	CreateCron(context.Context, string, string, string) (string, error)
	DeleteCron(context.Context, string, string) error
	CaptureError(context.Context, ExceptionPayload, string) error
}

type Service struct {
	infrai     Scheduler
	publicURL  string
	deliveries *http.Client
	now        func() time.Time
	mu         sync.Mutex
	assets     map[string]*Asset
}

func NewService(infrai Scheduler, publicURL string, deliveries *http.Client) *Service {
	if deliveries == nil {
		deliveries = &http.Client{Timeout: 15 * time.Second}
	}
	return &Service{
		infrai:     infrai,
		publicURL:  strings.TrimRight(publicURL, "/"),
		deliveries: deliveries,
		now:        time.Now,
		assets:     make(map[string]*Asset),
	}
}

func (s *Service) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /assets/ingest", s.ingest)
	mux.HandleFunc("POST /assets/{asset_id}/processed", s.processed)
	mux.HandleFunc("POST /callbacks/deliver/{asset_id}", s.deliver)
	return mux
}

func (s *Service) ingest(w http.ResponseWriter, r *http.Request) {
	var input struct {
		AssetID            string `json:"asset_id"`
		CreatorDeliveryURL string `json:"creator_delivery_url"`
		DelayHours         int    `json:"delay_hours"`
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil || input.AssetID == "" || input.DelayHours < 1 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "asset_id and a positive delay_hours are required"})
		return
	}
	if parsed, err := url.ParseRequestURI(input.CreatorDeliveryURL); err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "creator_delivery_url must be an HTTP URL"})
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.assets[input.AssetID]; exists {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "asset already ingested"})
		return
	}
	asset := &Asset{ID: input.AssetID, CreatorDeliveryURL: input.CreatorDeliveryURL, DelayHours: input.DelayHours, State: StateProcessing}
	s.assets[input.AssetID] = asset
	writeJSON(w, http.StatusAccepted, asset)
}

func (s *Service) processed(w http.ResponseWriter, r *http.Request) {
	assetID := r.PathValue("asset_id")
	s.mu.Lock()
	asset, exists := s.assets[assetID]
	if !exists {
		s.mu.Unlock()
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "asset not found"})
		return
	}
	if asset.State != StateProcessing {
		s.mu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]string{"error": "asset is not processing"})
		return
	}
	copy := *asset
	s.mu.Unlock()

	runAt := s.now().UTC().Add(time.Duration(copy.DelayHours) * time.Hour).Truncate(time.Minute)
	taskURL := s.publicURL + "/callbacks/deliver/" + url.PathEscape(copy.ID)
	jobID, err := s.infrai.CreateCron(r.Context(), cronExpression(runAt), taskURL, "media-delivery:create:"+copy.ID)
	if err != nil {
		writeInfraiError(w, err)
		return
	}

	s.mu.Lock()
	asset.State = StateReady
	asset.JobID = jobID
	result := *asset
	s.mu.Unlock()
	writeJSON(w, http.StatusAccepted, map[string]any{"asset": result, "deliver_at": runAt.Format(time.RFC3339)})
}

func (s *Service) deliver(w http.ResponseWriter, r *http.Request) {
	assetID := r.PathValue("asset_id")
	s.mu.Lock()
	asset, exists := s.assets[assetID]
	if !exists || asset.State != StateReady {
		s.mu.Unlock()
		s.captureDeliveryFailure(r.Context(), assetID, "asset is not ready for creator delivery")
		writeJSON(w, http.StatusConflict, map[string]string{"error": "asset is not ready"})
		return
	}
	copy := *asset
	s.mu.Unlock()

	body, _ := json.Marshal(map[string]string{"asset_id": copy.ID, "status": "ready"})
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, copy.CreatorDeliveryURL, strings.NewReader(string(body)))
	if err == nil {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "media-delivery:creator:"+copy.ID)
		var response *http.Response
		response, err = s.deliveries.Do(req)
		if response != nil {
			response.Body.Close()
			if response.StatusCode < 200 || response.StatusCode >= 300 {
				err = fmt.Errorf("creator endpoint returned HTTP %d", response.StatusCode)
			}
		}
	}
	if err != nil {
		s.mu.Lock()
		asset.State = StateFailed
		s.mu.Unlock()
		s.captureDeliveryFailure(r.Context(), assetID, err.Error())
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "creator delivery failed"})
		return
	}

	if err := s.infrai.DeleteCron(r.Context(), copy.JobID, "media-delivery:delete:"+copy.ID); err != nil {
		writeInfraiError(w, err)
		return
	}
	s.mu.Lock()
	asset.State = StateDelivered
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]string{"asset_id": copy.ID, "state": string(StateDelivered)})
}

func (s *Service) captureDeliveryFailure(ctx context.Context, assetID, message string) {
	err := s.infrai.CaptureError(ctx, ExceptionPayload{
		Message:     message,
		Level:       "error",
		Fingerprint: []string{"media-delivery", assetID},
		Context:     map[string]any{"asset_id": assetID, "stage": "creator_delivery"},
	}, "media-delivery:error:"+assetID)
	if err != nil {
		log.Printf("capture delivery failure: %v", err)
	}
}

func cronExpression(at time.Time) string {
	return strings.Join([]string{
		strconv.Itoa(at.Minute()),
		strconv.Itoa(at.Hour()),
		strconv.Itoa(at.Day()),
		strconv.Itoa(int(at.Month())),
		"*",
	}, " ")
}

func writeInfraiError(w http.ResponseWriter, err error) {
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.HTTPStatus >= 400 && apiErr.HTTPStatus < 500 {
		writeJSON(w, apiErr.HTTPStatus, map[string]string{"error": apiErr.Message})
		return
	}
	writeJSON(w, http.StatusBadGateway, map[string]string{"error": "upstream request failed"})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
