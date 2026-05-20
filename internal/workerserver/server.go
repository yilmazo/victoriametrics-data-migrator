package workerserver

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	pb "github.com/yilmazo/victoriametrics-data-migrator/proto"
)

// Server implements the gRPC WorkerService.
// Each worker pod runs one Server instance that receives tasks from the coordinator,
// executes vmctl as a subprocess, and returns the result.
type Server struct {
	pb.UnimplementedWorkerServiceServer
	vmctlPath string
	workerID  string
	logger    *zap.Logger

	// Ensure only one task runs at a time
	mu   sync.Mutex
	busy bool
}

// NewServer creates a new worker gRPC server.
func NewServer(vmctlPath string, logger *zap.Logger) *Server {
	hostname, _ := os.Hostname()
	return &Server{
		vmctlPath: vmctlPath,
		workerID:  hostname,
		logger:    logger,
	}
}

// ExecuteTask handles a task execution request from the coordinator.
// It spawns vmctl as a subprocess and returns the result.
func (s *Server) ExecuteTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	s.mu.Lock()
	if s.busy {
		s.mu.Unlock()
		return nil, fmt.Errorf("worker %s is busy", s.workerID)
	}
	s.busy = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.busy = false
		s.mu.Unlock()
	}()

	s.logger.Info("Received task",
		zap.String("task_id", req.TaskId),
		zap.Int("arg_count", len(req.VmctlArgs)),
	)

	result := RunVmctl(ctx, s.vmctlPath, req.VmctlArgs, s.logger)

	return &pb.TaskResponse{
		ExitCode:         int32(result.ExitCode),
		Logs:             result.Logs,
		ErrorMessage:     result.ErrorMessage,
		BytesTransferred: result.BytesTransferred,
	}, nil
}

// Ping responds to health check requests.
func (s *Server) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{
		WorkerId: s.workerID,
	}, nil
}

// Run starts the gRPC server on the given port and blocks until the context is cancelled.
func (s *Server) Run(ctx context.Context, port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", port, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterWorkerServiceServer(grpcServer, s)

	s.logger.Info("Worker gRPC server starting",
		zap.String("worker_id", s.workerID),
		zap.Int("port", port),
		zap.String("vmctl_path", s.vmctlPath),
	)

	// Graceful shutdown on context cancellation
	go func() {
		<-ctx.Done()
		s.logger.Info("Shutting down worker gRPC server")
		grpcServer.GracefulStop()
	}()

	if err := grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("gRPC server error: %w", err)
	}

	return nil
}
