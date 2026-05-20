// Package discovery provides a client for VictoriaMetrics APIs
// used during the metric discovery and cardinality analysis phase.
package discovery

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/yilmazo/victoriametrics-data-migrator/internal/config"
	"github.com/yilmazo/victoriametrics-data-migrator/internal/types"
)

// Client communicates with VictoriaMetrics HTTP APIs for metric discovery
// and cardinality analysis.
type Client struct {
	baseURL    string
	httpClient *http.Client
	headers    map[string]string
	bearerToken string
	basicAuth  *config.BasicAuthConfig
	logger     *zap.Logger
}

// NewClient creates a new VictoriaMetrics API client.
func NewClient(srcCfg config.SourceConfig, logger *zap.Logger) *Client {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
	}

	if srcCfg.TLS.InsecureSkipVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	return &Client{
		baseURL: strings.TrimRight(srcCfg.VmselectURL, "/"),
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   5 * time.Minute,
		},
		headers:     srcCfg.Headers,
		bearerToken: srcCfg.BearerToken,
		basicAuth:   &srcCfg.BasicAuth,
		logger:      logger,
	}
}

// apiResponse is the generic wrapper for Prometheus-compatible API responses.
type apiResponse struct {
	Status string          `json:"status"`
	Data   json.RawMessage `json:"data"`
	Error  string          `json:"error,omitempty"`
}

// doRequest performs an HTTP GET request with configured auth and headers.
func (c *Client) doRequest(ctx context.Context, path string, params url.Values) ([]byte, error) {
	reqURL := fmt.Sprintf("%s%s", c.baseURL, path)
	if len(params) > 0 {
		reqURL += "?" + params.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	// Apply auth
	if c.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	}
	if c.basicAuth != nil && c.basicAuth.Username != "" {
		req.SetBasicAuth(c.basicAuth.Username, c.basicAuth.Password)
	}
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}

	c.logger.Debug("API request", zap.String("url", reqURL))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request to %s: %w", reqURL, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d from %s: %s", resp.StatusCode, reqURL, string(body))
	}

	return body, nil
}

// DiscoverMetrics fetches metric names matching the given selector within
// the specified time range using the /api/v1/label/__name__/values endpoint.
func (c *Client) DiscoverMetrics(ctx context.Context, match string, start, end time.Time) ([]string, error) {
	params := url.Values{}
	if match != "" {
		params.Set("match[]", match)
	}
	if !start.IsZero() {
		params.Set("start", strconv.FormatInt(start.Unix(), 10))
	}
	if !end.IsZero() {
		params.Set("end", strconv.FormatInt(end.Unix(), 10))
	}

	body, err := c.doRequest(ctx, "/api/v1/label/__name__/values", params)
	if err != nil {
		return nil, fmt.Errorf("discovering metrics: %w", err)
	}

	var resp apiResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parsing metrics response: %w", err)
	}
	if resp.Status != "success" {
		return nil, fmt.Errorf("API error discovering metrics: %s", resp.Error)
	}

	var metrics []string
	if err := json.Unmarshal(resp.Data, &metrics); err != nil {
		return nil, fmt.Errorf("parsing metrics data: %w", err)
	}

	c.logger.Info("Discovered metrics", zap.Int("count", len(metrics)))
	return metrics, nil
}

// DiscoverLabels fetches label names for a given metric and time range
// using the /api/v1/labels endpoint.
func (c *Client) DiscoverLabels(ctx context.Context, metric string, baseSelector string, start, end time.Time) ([]string, error) {
	match := buildMatchSelector(metric, baseSelector)

	params := url.Values{}
	params.Set("match[]", match)
	if !start.IsZero() {
		params.Set("start", strconv.FormatInt(start.Unix(), 10))
	}
	if !end.IsZero() {
		params.Set("end", strconv.FormatInt(end.Unix(), 10))
	}

	body, err := c.doRequest(ctx, "/api/v1/labels", params)
	if err != nil {
		return nil, fmt.Errorf("discovering labels for %s: %w", metric, err)
	}

	var resp apiResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parsing labels response: %w", err)
	}
	if resp.Status != "success" {
		return nil, fmt.Errorf("API error discovering labels: %s", resp.Error)
	}

	var labels []string
	if err := json.Unmarshal(resp.Data, &labels); err != nil {
		return nil, fmt.Errorf("parsing labels data: %w", err)
	}

	return labels, nil
}

// GetLabelValues fetches all values for a given label name, metric, and time range
// using the /api/v1/label/<name>/values endpoint.
func (c *Client) GetLabelValues(ctx context.Context, labelName string, metric string, baseSelector string, start, end time.Time) ([]string, error) {
	match := buildMatchSelector(metric, baseSelector)

	params := url.Values{}
	params.Set("match[]", match)
	if !start.IsZero() {
		params.Set("start", strconv.FormatInt(start.Unix(), 10))
	}
	if !end.IsZero() {
		params.Set("end", strconv.FormatInt(end.Unix(), 10))
	}

	path := fmt.Sprintf("/api/v1/label/%s/values", url.PathEscape(labelName))
	body, err := c.doRequest(ctx, path, params)
	if err != nil {
		return nil, fmt.Errorf("getting label values for %s/%s: %w", metric, labelName, err)
	}

	var resp apiResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parsing label values response: %w", err)
	}
	if resp.Status != "success" {
		return nil, fmt.Errorf("API error getting label values: %s", resp.Error)
	}

	var values []string
	if err := json.Unmarshal(resp.Data, &values); err != nil {
		return nil, fmt.Errorf("parsing label values data: %w", err)
	}

	return values, nil
}

// GetTSDBStatus fetches cardinality statistics from /api/v1/status/tsdb.
// If focusLabel is non-empty, the response will include seriesCountByFocusLabelValue.
func (c *Client) GetTSDBStatus(ctx context.Context, match string, focusLabel string, topN int, start, end time.Time) (*types.TSDBStatus, error) {
	params := url.Values{}
	if match != "" {
		params.Set("match[]", match)
	}
	if focusLabel != "" {
		params.Set("focusLabel", focusLabel)
	}
	if topN > 0 {
		params.Set("topN", strconv.Itoa(topN))
	}
	if !start.IsZero() {
		params.Set("start", strconv.FormatInt(start.Unix(), 10))
	}
	if !end.IsZero() {
		params.Set("end", strconv.FormatInt(end.Unix(), 10))
	}

	body, err := c.doRequest(ctx, "/api/v1/status/tsdb", params)
	if err != nil {
		return nil, fmt.Errorf("getting TSDB status: %w", err)
	}

	// The tsdb status endpoint wraps data differently
	var resp apiResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parsing TSDB status response: %w", err)
	}

	var status types.TSDBStatus
	if err := json.Unmarshal(resp.Data, &status); err != nil {
		return nil, fmt.Errorf("parsing TSDB status data: %w", err)
	}

	return &status, nil
}

// GetSeriesCount returns the total number of series matching a selector
// for the given time range. It uses the TSDB status endpoint.
// Note: This is expensive on large datasets. Prefer GetSeriesCountFast for quick checks.
func (c *Client) GetSeriesCount(ctx context.Context, match string, start, end time.Time) (int, error) {
	status, err := c.GetTSDBStatus(ctx, match, "", 1, start, end)
	if err != nil {
		return 0, err
	}
	return status.TotalSeries, nil
}

// GetSeriesCountFast returns an approximate series count using a lightweight
// /api/v1/query?query=count({selector}) call. This is orders of magnitude
// faster than the TSDB status endpoint (~100ms vs ~30s) and sufficient for
// deciding whether a metric needs splitting.
func (c *Client) GetSeriesCountFast(ctx context.Context, match string, end time.Time) (int, error) {
	query := fmt.Sprintf("count(%s)", match)

	params := url.Values{}
	params.Set("query", query)
	if !end.IsZero() {
		params.Set("time", strconv.FormatInt(end.Unix(), 10))
	}
	// Use a large step to avoid unnecessary processing
	params.Set("step", "86400")

	body, err := c.doRequest(ctx, "/api/v1/query", params)
	if err != nil {
		return 0, fmt.Errorf("fast series count for %s: %w", match, err)
	}

	var resp apiResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, fmt.Errorf("parsing fast count response: %w", err)
	}
	if resp.Status != "success" {
		return 0, fmt.Errorf("API error in fast count: %s", resp.Error)
	}

	// Parse the instant query result: {"resultType":"vector","result":[{"value":[ts,"count"]}]}
	var queryResult struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Value []json.RawMessage `json:"value"`
		} `json:"result"`
	}
	if err := json.Unmarshal(resp.Data, &queryResult); err != nil {
		return 0, fmt.Errorf("parsing fast count data: %w", err)
	}

	if len(queryResult.Result) == 0 || len(queryResult.Result[0].Value) < 2 {
		return 0, nil // No series found
	}

	var countStr string
	if err := json.Unmarshal(queryResult.Result[0].Value[1], &countStr); err != nil {
		return 0, fmt.Errorf("parsing count value: %w", err)
	}

	count, err := strconv.Atoi(countStr)
	if err != nil {
		return 0, fmt.Errorf("converting count %q to int: %w", countStr, err)
	}

	return count, nil
}

// GetSeriesDistribution returns the series count per value of a given label,
// for series matching the given selector. Uses focusLabel with TSDB status.
func (c *Client) GetSeriesDistribution(ctx context.Context, match string, focusLabel string, start, end time.Time) ([]types.LabelValueCount, error) {
	status, err := c.GetTSDBStatus(ctx, match, focusLabel, 100000, start, end)
	if err != nil {
		return nil, err
	}
	return status.SeriesCountByFocusLabelValue, nil
}

// buildMatchSelector constructs a PromQL match[] selector from a metric name
// and an optional base selector string.
func buildMatchSelector(metric string, baseSelector string) string {
	parts := []string{fmt.Sprintf(`__name__="%s"`, metric)}

	// Parse base selector and append its matchers
	if baseSelector != "" {
		// Strip outer braces if present
		sel := strings.TrimSpace(baseSelector)
		sel = strings.TrimPrefix(sel, "{")
		sel = strings.TrimSuffix(sel, "}")
		sel = strings.TrimSpace(sel)
		if sel != "" {
			parts = append(parts, sel)
		}
	}

	return "{" + strings.Join(parts, ",") + "}"
}
