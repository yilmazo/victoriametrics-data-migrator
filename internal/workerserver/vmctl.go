// Package workerserver implements the gRPC worker process that receives
// task assignments from the coordinator and executes vmctl as a subprocess.
package workerserver

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"

	"go.uber.org/zap"
)

// VmctlResult holds the result of a vmctl subprocess execution.
type VmctlResult struct {
	ExitCode         int
	Logs             string
	ErrorMessage     string
	BytesTransferred int64
}

// RunVmctl executes vmctl as a subprocess with the given arguments.
// It captures stdout and stderr, parses the output for bytes transferred,
// and returns a structured result.
func RunVmctl(ctx context.Context, vmctlPath string, args []string, logger *zap.Logger) *VmctlResult {
	logger.Info("Starting vmctl subprocess",
		zap.String("vmctl_path", vmctlPath),
		zap.Int("arg_count", len(args)),
	)

	cmd := exec.CommandContext(ctx, vmctlPath, args...)

	// Capture both stdout and stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return &VmctlResult{
			ExitCode:     -1,
			ErrorMessage: fmt.Sprintf("failed to create stdout pipe: %v", err),
		}
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return &VmctlResult{
			ExitCode:     -1,
			ErrorMessage: fmt.Sprintf("failed to create stderr pipe: %v", err),
		}
	}

	if err := cmd.Start(); err != nil {
		return &VmctlResult{
			ExitCode:     -1,
			ErrorMessage: fmt.Sprintf("failed to start vmctl: %v", err),
		}
	}

	// Read stdout and stderr concurrently
	var logLines []string
	var mu sync.Mutex

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			mu.Lock()
			logLines = append(logLines, line)
			mu.Unlock()
			logger.Debug("vmctl stdout", zap.String("line", line))
		}
	}()

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			mu.Lock()
			logLines = append(logLines, line)
			mu.Unlock()
			logger.Debug("vmctl stderr", zap.String("line", line))
		}
	}()

	wg.Wait()

	// Wait for the process to finish
	exitCode := 0
	errMsg := ""
	if err := cmd.Wait(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = -1
		}
		errMsg = err.Error()
	}

	logs := strings.Join(logLines, "\n")

	// Parse bytes transferred from vmctl output
	bytesTransferred := parseVmctlBytes(logs)

	result := &VmctlResult{
		ExitCode:         exitCode,
		Logs:             logs,
		ErrorMessage:     errMsg,
		BytesTransferred: bytesTransferred,
	}

	if exitCode == 0 {
		logger.Info("vmctl completed successfully",
			zap.Int64("bytes_transferred", bytesTransferred),
		)
	} else {
		// Extract meaningful error from the last non-empty log line
		for i := len(logLines) - 1; i >= 0; i-- {
			line := strings.TrimSpace(logLines[i])
			if line != "" && !strings.HasPrefix(line, "VictoriaMetrics") {
				result.ErrorMessage = line
				break
			}
		}
		logger.Warn("vmctl failed",
			zap.Int("exit_code", exitCode),
			zap.String("error", result.ErrorMessage),
		)
	}

	return result
}

// parseVmctlBytes extracts bytes transferred from vmctl output.
// vmctl logs lines like: "total bytes: 7.8 MB"
func parseVmctlBytes(logs string) int64 {
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, "total bytes:") {
			parts := strings.Split(line, "total bytes:")
			if len(parts) > 1 {
				bytesStr := strings.TrimSpace(parts[1])
				bytesStr = strings.Split(bytesStr, ";")[0]
				return parseHumanBytes(bytesStr)
			}
		}
	}
	return 0
}

// parseHumanBytes converts a human-readable bytes string to int64.
func parseHumanBytes(s string) int64 {
	s = strings.TrimSpace(s)
	s = strings.ToUpper(s)

	multipliers := map[string]int64{
		"TB": 1024 * 1024 * 1024 * 1024,
		"GB": 1024 * 1024 * 1024,
		"MB": 1024 * 1024,
		"KB": 1024,
		"B":  1,
	}

	for suffix, mult := range multipliers {
		if strings.HasSuffix(s, suffix) {
			numStr := strings.TrimSpace(strings.TrimSuffix(s, suffix))
			var val float64
			if _, err := fmt.Sscanf(numStr, "%f", &val); err == nil {
				return int64(val * float64(mult))
			}
		}
	}

	return 0
}
