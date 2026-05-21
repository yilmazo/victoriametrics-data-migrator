// Package worker manages the lifecycle of static worker pods and gRPC communication.
package worker

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/yilmazo/victoriametrics-data-migrator/internal/config"
	"github.com/yilmazo/victoriametrics-data-migrator/internal/types"
	pb "github.com/yilmazo/victoriametrics-data-migrator/proto"
)

const (
	defaultGRPCPort = 9091
	grpcDialTimeout = 10 * time.Second
)

// TaskResult holds the result of a task executed by a worker.
type TaskResult struct {
	ExitCode         int
	Logs             string
	ErrorMessage     string
	BytesTransferred int64
}

// workerConn tracks a gRPC connection to a worker pod.
type workerConn struct {
	podName string
	addr    string
	conn    *grpc.ClientConn
	client  pb.WorkerServiceClient
	busy    bool
}

// Manager manages the lifecycle of worker pods and gRPC communication.
type Manager struct {
	clientset   kubernetes.Interface
	config      *config.Config
	namespace   string
	migrationID string
	logger      *zap.Logger

	mu      sync.Mutex
	workers []*workerConn
}

// NewManager creates a new worker Manager.
// It tries in-cluster config first, then falls back to kubeconfig.
func NewManager(cfg *config.Config, migrationID string, logger *zap.Logger) (*Manager, error) {
	var restConfig *rest.Config
	var err error

	// Try in-cluster config first
	restConfig, err = rest.InClusterConfig()
	if err != nil {
		// Fall back to kubeconfig
		loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
		configOverrides := &clientcmd.ConfigOverrides{}
		kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
		restConfig, err = kubeConfig.ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("unable to get kubernetes config: %w", err)
		}
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("creating kubernetes clientset: %w", err)
	}

	return &Manager{
		clientset:   clientset,
		config:      cfg,
		namespace:   cfg.Workers.Namespace,
		migrationID: migrationID,
		logger:      logger,
	}, nil
}

// DeployWorkers creates a Deployment of worker pods and waits for them to become Ready.
// It then establishes gRPC connections to all worker pods.
func (m *Manager) DeployWorkers(ctx context.Context) error {
	deploy := m.buildDeployment()

	m.logger.Info("Creating worker Deployment",
		zap.String("name", deploy.Name),
		zap.Int32("replicas", *deploy.Spec.Replicas),
	)

	_, err := m.clientset.AppsV1().Deployments(m.namespace).Create(ctx, deploy, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("creating worker deployment: %w", err)
	}

	// Wait for all pods to be Ready
	if err := m.waitForWorkers(ctx); err != nil {
		return fmt.Errorf("waiting for workers: %w", err)
	}

	// Discover pod IPs and establish gRPC connections
	if err := m.connectToWorkers(ctx); err != nil {
		return fmt.Errorf("connecting to workers: %w", err)
	}

	m.logger.Info("All workers ready and connected",
		zap.Int("count", len(m.workers)),
	)

	return nil
}

// waitForWorkers polls until the Deployment has the desired number of ready replicas.
func (m *Manager) waitForWorkers(ctx context.Context) error {
	deployName := m.deploymentName()
	desired := int32(m.config.Workers.Count)
	pollInterval := 2 * time.Second
	timeout := 5 * time.Minute

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		deploy, err := m.clientset.AppsV1().Deployments(m.namespace).Get(ctx, deployName, metav1.GetOptions{})
		if err != nil {
			m.logger.Warn("Failed to check deployment status", zap.Error(err))
			time.Sleep(pollInterval)
			continue
		}

		if deploy.Status.ReadyReplicas >= desired {
			m.logger.Info("Worker deployment ready",
				zap.Int32("ready", deploy.Status.ReadyReplicas),
				zap.Int32("desired", desired),
			)
			return nil
		}

		m.logger.Debug("Waiting for workers to be ready",
			zap.Int32("ready", deploy.Status.ReadyReplicas),
			zap.Int32("desired", desired),
		)
		time.Sleep(pollInterval)
	}

	return fmt.Errorf("timed out waiting for %d workers to be ready (timeout: %s)", desired, timeout)
}

// connectToWorkers discovers worker pod IPs and establishes gRPC connections.
func (m *Manager) connectToWorkers(ctx context.Context) error {
	labelSelector := fmt.Sprintf("app=vm-migrator,component=worker,migration-id=%s", m.migrationID)
	pods, err := m.clientset.CoreV1().Pods(m.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return fmt.Errorf("listing worker pods: %w", err)
	}

	port := m.grpcPort()

	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		if pod.Status.PodIP == "" {
			continue
		}

		addr := fmt.Sprintf("%s:%d", pod.Status.PodIP, port)
		dialCtx, cancel := context.WithTimeout(ctx, grpcDialTimeout)
		conn, err := grpc.DialContext(dialCtx, addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		cancel()

		if err != nil {
			m.logger.Warn("Failed to connect to worker, skipping",
				zap.String("pod", pod.Name),
				zap.String("addr", addr),
				zap.Error(err),
			)
			continue
		}

		client := pb.NewWorkerServiceClient(conn)

		// Verify the worker is responsive
		pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := client.Ping(pingCtx, &pb.PingRequest{})
		pingCancel()

		if err != nil {
			m.logger.Warn("Worker ping failed, skipping",
				zap.String("pod", pod.Name),
				zap.Error(err),
			)
			conn.Close()
			continue
		}

		m.workers = append(m.workers, &workerConn{
			podName: pod.Name,
			addr:    addr,
			conn:    conn,
			client:  client,
		})

		m.logger.Info("Connected to worker",
			zap.String("pod", pod.Name),
			zap.String("worker_id", resp.WorkerId),
			zap.String("addr", addr),
		)
	}

	if len(m.workers) == 0 {
		return fmt.Errorf("no workers available")
	}

	return nil
}

// AcquireWorker returns an idle worker connection, or nil if all are busy.
func (m *Manager) AcquireWorker() *workerConn {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, w := range m.workers {
		if !w.busy {
			w.busy = true
			return w
		}
	}
	return nil
}

// ReleaseWorker marks a worker as idle.
func (m *Manager) ReleaseWorker(w *workerConn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w.busy = false
}

// IdleWorkerCount returns the number of idle workers.
func (m *Manager) IdleWorkerCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	count := 0
	for _, w := range m.workers {
		if !w.busy {
			count++
		}
	}
	return count
}

// WorkerCount returns the total number of connected workers.
func (m *Manager) WorkerCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.workers)
}

// DispatchTask sends a task to a specific worker and waits for the result.
func (m *Manager) DispatchTask(ctx context.Context, w *workerConn, task *types.Task) (*TaskResult, error) {
	args := m.buildVmctlArgs(task)

	m.logger.Info("Dispatching task to worker",
		zap.String("task_id", task.ID),
		zap.String("worker", w.podName),
		zap.String("selector", task.Selector),
	)

	resp, err := w.client.ExecuteTask(ctx, &pb.TaskRequest{
		TaskId:    task.ID,
		VmctlArgs: args,
	})
	if err != nil {
		return nil, fmt.Errorf("executing task %s on worker %s: %w", task.ID, w.podName, err)
	}

	return &TaskResult{
		ExitCode:         int(resp.ExitCode),
		Logs:             resp.Logs,
		ErrorMessage:     resp.ErrorMessage,
		BytesTransferred: resp.BytesTransferred,
	}, nil
}

// Cleanup deletes the worker Deployment and closes all gRPC connections.
func (m *Manager) Cleanup(ctx context.Context) error {
	// Close gRPC connections
	for _, w := range m.workers {
		if err := w.conn.Close(); err != nil {
			m.logger.Warn("Failed to close gRPC connection",
				zap.String("pod", w.podName),
				zap.Error(err),
			)
		}
	}

	// Delete the Deployment
	deployName := m.deploymentName()
	propagation := metav1.DeletePropagationForeground
	err := m.clientset.AppsV1().Deployments(m.namespace).Delete(ctx, deployName, metav1.DeleteOptions{
		PropagationPolicy: &propagation,
	})
	if err != nil {
		return fmt.Errorf("deleting worker deployment: %w", err)
	}

	m.logger.Info("Deleted worker deployment", zap.String("name", deployName))
	return nil
}

// buildDeployment constructs the K8s Deployment spec for worker pods.
func (m *Manager) buildDeployment() *appsv1.Deployment {
	cfg := m.config
	deployName := m.deploymentName()
	replicas := int32(cfg.Workers.Count)
	port := int32(m.grpcPort())

	labels := map[string]string{
		"app":          "vm-migrator",
		"component":    "worker",
		"migration-id": m.migrationID,
	}

	// Build resource requirements
	resources := corev1.ResourceRequirements{}
	if cfg.Workers.Pod.Resources.Requests.CPU != "" || cfg.Workers.Pod.Resources.Requests.Memory != "" {
		resources.Requests = corev1.ResourceList{}
		if cfg.Workers.Pod.Resources.Requests.CPU != "" {
			resources.Requests[corev1.ResourceCPU] = resource.MustParse(cfg.Workers.Pod.Resources.Requests.CPU)
		}
		if cfg.Workers.Pod.Resources.Requests.Memory != "" {
			resources.Requests[corev1.ResourceMemory] = resource.MustParse(cfg.Workers.Pod.Resources.Requests.Memory)
		}
	}
	if cfg.Workers.Pod.Resources.Limits.CPU != "" || cfg.Workers.Pod.Resources.Limits.Memory != "" {
		resources.Limits = corev1.ResourceList{}
		if cfg.Workers.Pod.Resources.Limits.CPU != "" {
			resources.Limits[corev1.ResourceCPU] = resource.MustParse(cfg.Workers.Pod.Resources.Limits.CPU)
		}
		if cfg.Workers.Pod.Resources.Limits.Memory != "" {
			resources.Limits[corev1.ResourceMemory] = resource.MustParse(cfg.Workers.Pod.Resources.Limits.Memory)
		}
	}

	workerArgs := []string{
		"worker",
		"--port=" + strconv.Itoa(int(port)),
		"--vmctl-path=" + cfg.Workers.Pod.VmctlPath,
	}

	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deployName,
			Namespace: m.namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: cfg.Workers.Pod.ServiceAccount,
					Containers: []corev1.Container{
						{
							Name:      "worker",
							Image:     cfg.Workers.Pod.Image,
							Command:   []string{"vm-migrator"},
							Args:      workerArgs,
							Resources: resources,
							Ports: []corev1.ContainerPort{
								{
									Name:          "grpc",
									ContainerPort: port,
									Protocol:      corev1.ProtocolTCP,
								},
							},
						},
					},
				},
			},
		},
	}

	// Apply image pull policy
	if cfg.Workers.Pod.ImagePullPolicy != "" {
		deploy.Spec.Template.Spec.Containers[0].ImagePullPolicy = corev1.PullPolicy(cfg.Workers.Pod.ImagePullPolicy)
	}

	// Apply node selector
	if len(cfg.Workers.Pod.NodeSelector) > 0 {
		deploy.Spec.Template.Spec.NodeSelector = cfg.Workers.Pod.NodeSelector
	}

	// Apply tolerations
	for _, tol := range cfg.Workers.Pod.Tolerations {
		deploy.Spec.Template.Spec.Tolerations = append(deploy.Spec.Template.Spec.Tolerations,
			corev1.Toleration{
				Key:      tol.Key,
				Operator: corev1.TolerationOperator(tol.Operator),
				Value:    tol.Value,
				Effect:   corev1.TaintEffect(tol.Effect),
			})
	}

	// Apply image pull secret
	if cfg.Workers.Pod.ImagePullSecret != "" {
		deploy.Spec.Template.Spec.ImagePullSecrets = []corev1.LocalObjectReference{
			{Name: cfg.Workers.Pod.ImagePullSecret},
		}
	}

	return deploy
}

// buildVmctlArgs constructs command-line arguments for vmctl vm-native.
func (m *Manager) buildVmctlArgs(task *types.Task) []string {
	cfg := m.config

	args := []string{
		"vm-native",
		"-s", // silent mode (no interactive prompts)
		"--vm-native-src-addr=" + cfg.Source.VmselectURL,
		"--vm-native-dst-addr=" + cfg.Destination.VminsertURL,
		"--vm-native-filter-match=" + task.Selector,
		"--vm-native-filter-time-start=" + task.TimeStart.Format("2006-01-02T15:04:05Z"),
		"--vm-native-filter-time-end=" + task.TimeEnd.Format("2006-01-02T15:04:05Z"),
		"--vm-concurrency=" + strconv.Itoa(cfg.Workers.Vmctl.Concurrency),
		"--vm-native-disable-per-metric-migration",
	}

	// Source auth
	if cfg.Source.BearerToken != "" {
		args = append(args, "--vm-native-src-bearer-token="+cfg.Source.BearerToken)
	}
	if cfg.Source.BasicAuth.Username != "" {
		args = append(args, "--vm-native-src-user="+cfg.Source.BasicAuth.Username)
		args = append(args, "--vm-native-src-password="+cfg.Source.BasicAuth.Password)
	}
	if len(cfg.Source.Headers) > 0 {
		var headers []string
		for k, v := range cfg.Source.Headers {
			headers = append(headers, k+":"+v)
		}
		args = append(args, "--vm-native-src-headers="+strings.Join(headers, "^^"))
	}
	if cfg.Source.TLS.InsecureSkipVerify {
		args = append(args, "--vm-native-src-insecure-skip-verify")
	}
	if cfg.Source.TLS.CAFile != "" {
		args = append(args, "--vm-native-src-ca-file="+cfg.Source.TLS.CAFile)
	}
	if cfg.Source.TLS.CertFile != "" {
		args = append(args, "--vm-native-src-cert-file="+cfg.Source.TLS.CertFile)
	}
	if cfg.Source.TLS.KeyFile != "" {
		args = append(args, "--vm-native-src-key-file="+cfg.Source.TLS.KeyFile)
	}

	// Destination auth
	if cfg.Destination.BearerToken != "" {
		args = append(args, "--vm-native-dst-bearer-token="+cfg.Destination.BearerToken)
	}
	if cfg.Destination.BasicAuth.Username != "" {
		args = append(args, "--vm-native-dst-user="+cfg.Destination.BasicAuth.Username)
		args = append(args, "--vm-native-dst-password="+cfg.Destination.BasicAuth.Password)
	}
	if len(cfg.Destination.Headers) > 0 {
		var headers []string
		for k, v := range cfg.Destination.Headers {
			headers = append(headers, k+":"+v)
		}
		args = append(args, "--vm-native-dst-headers="+strings.Join(headers, "^^"))
	}
	if cfg.Destination.TLS.InsecureSkipVerify {
		args = append(args, "--vm-native-dst-insecure-skip-verify")
	}

	// vmctl options
	if cfg.Workers.Vmctl.RateLimit > 0 {
		args = append(args, "--vm-rate-limit="+strconv.Itoa(cfg.Workers.Vmctl.RateLimit))
	}
	if cfg.Workers.Vmctl.DisableBinaryProtocol {
		args = append(args, "--vm-native-disable-binary-protocol")
	}
	if cfg.Workers.Vmctl.DisableHTTPKeepAlive {
		args = append(args, "--vm-native-disable-http-keep-alive")
	}
	if cfg.Workers.Vmctl.BackoffRetries > 0 {
		args = append(args, "--vm-native-backoff-retries="+strconv.Itoa(cfg.Workers.Vmctl.BackoffRetries))
	}
	for _, label := range cfg.Workers.Vmctl.ExtraLabels {
		args = append(args, "--vm-extra-label="+label)
	}

	return args
}

// deploymentName returns the Deployment name for this migration.
func (m *Manager) deploymentName() string {
	name := fmt.Sprintf("vm-migrator-workers-%s", m.migrationID)
	if len(name) > 63 {
		name = name[:63]
	}
	return name
}

// grpcPort returns the configured gRPC port or the default.
func (m *Manager) grpcPort() int {
	if m.config.Workers.GRPCPort > 0 {
		return m.config.Workers.GRPCPort
	}
	return defaultGRPCPort
}
