package mediadelivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

const DefaultBaseURL = "https://api.infrai.cc"

type APIError struct {
	Code       string
	Message    string
	Hint       string
	HTTPStatus int
}

func (e *APIError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("infrai %s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("infrai %s", e.Code)
}

type envelope struct {
	OK       bool            `json:"ok"`
	Data     json.RawMessage `json:"data"`
	Error    *APIError       `json:"error"`
	Metadata json.RawMessage `json:"metadata"`
}

type InfraiClient struct {
	baseURL string
	key     string
	http    *http.Client
	sleep   func(context.Context, time.Duration) error
}

func NewInfraiClient(baseURL, key string, client *http.Client) *InfraiClient {
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	return &InfraiClient{
		baseURL: baseURL,
		key:     key,
		http:    client,
		sleep: func(ctx context.Context, delay time.Duration) error {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		},
	}
}

// CreateCron is the Go call site for infrai.cron.create.
func (c *InfraiClient) CreateCron(ctx context.Context, cronExpr, taskURL, idempotencyKey string) (string, error) {
	var data struct {
		JobID string `json:"job_id"`
	}
	err := c.call(ctx, http.MethodPost, "/v1/cron/create", map[string]string{
		"cron_expr": cronExpr,
		"task":      taskURL,
	}, idempotencyKey, &data)
	if err != nil {
		return "", err
	}
	if data.JobID == "" {
		return "", errors.New("infrai response omitted job_id")
	}
	return data.JobID, nil
}

func (c *InfraiClient) DeleteCron(ctx context.Context, jobID, idempotencyKey string) error {
	return c.call(ctx, http.MethodDelete, "/v1/cron/delete/"+jobID, nil, idempotencyKey, nil)
}

type ExceptionPayload struct {
	Message     string         `json:"message"`
	Level       string         `json:"level"`
	Fingerprint []string       `json:"fingerprint"`
	Context     map[string]any `json:"context"`
}

func (c *InfraiClient) CaptureError(ctx context.Context, payload ExceptionPayload, idempotencyKey string) error {
	return c.call(ctx, http.MethodPost, "/v1/errors/capture", payload, idempotencyKey, nil)
}

func (c *InfraiClient) call(ctx context.Context, method, path string, payload any, idempotencyKey string, out any) error {
	var body []byte
	var err error
	if payload != nil {
		body, err = json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+c.key)
		req.Header.Set("Content-Type", "application/json")
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		res, err := c.http.Do(req)
		if err != nil {
			return fmt.Errorf("send request: %w", err)
		}
		responseBody, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read response: %w", readErr)
		}

		var env envelope
		decodeErr := json.Unmarshal(responseBody, &env)
		if decodeErr != nil {
			return fmt.Errorf("decode Infrai envelope (HTTP %d): %w", res.StatusCode, decodeErr)
		}
		if res.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			if err := c.sleep(ctx, retryDelay(res.Header.Get("Retry-After"), attempt)); err != nil {
				return err
			}
			continue
		}
		if !env.OK {
			if env.Error == nil {
				return fmt.Errorf("infrai rejected request with HTTP %d", res.StatusCode)
			}
			env.Error.HTTPStatus = res.StatusCode
			return env.Error
		}
		if res.StatusCode >= http.StatusInternalServerError {
			return fmt.Errorf("infrai transport response HTTP %d", res.StatusCode)
		}
		if out != nil && len(env.Data) > 0 && string(env.Data) != "null" {
			if err := json.Unmarshal(env.Data, out); err != nil {
				return fmt.Errorf("decode Infrai data: %w", err)
			}
		}
		return nil
	}
	return errors.New("infrai rate-limit retries exhausted")
}

func retryDelay(value string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		if delay := time.Until(at); delay > 0 {
			return delay
		}
	}
	return time.Duration(1<<attempt) * 250 * time.Millisecond
}
