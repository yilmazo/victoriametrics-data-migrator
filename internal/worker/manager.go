// Package worker manages Kubernetes Jobs for executing vmctl migration tasks.
package worker

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"go.uber.org/zap"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/yilmazo/victoriametrics-data-migrator/internal/config"
	"github.com/yilmazo/victoriametrics-data-migrator/internal/types"
)

// Manager manages the lifecycle of Kubernetes Jobs that run vmctl.
type Manager struct {
	clientset   kubernetes.Interface
	config      *config.Config
	namespace   string
	migrationID string
	logger      *zap.Logger
}

// NewManager creates a new Kubernetes Job manager.
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

// CreateJob creates a Kubernetes Job for a migration task.
func (m *Manager) CreateJob(ctx context.Context, task *types.Task) (*batchv1.Job, error) {
	job := m.buildJobSpec(task)

	created, err := m.clientset.BatchV1().Jobs(m.namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("creating job for task %s: %w", task.ID, err)
	}

	m.logger.Info("Created K8s Job",
		zap.String("job", created.Name),
		zap.String("task_id", task.ID),
		zap.String("selector", task.Selector),
	)

	return created, nil
}

// WatchJobs watches for Job completions/failures in the migration namespace.
func (m *Manager) WatchJobs(ctx context.Context) (watch.Interface, error) {
	labelSelector := fmt.Sprintf("app=vm-migrator,migration-id=%s", m.migrationID)
	watcher, err := m.clientset.BatchV1().Jobs(m.namespace).Watch(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return nil, fmt.Errorf("watching jobs: %w", err)
	}
	return watcher, nil
}

// GetJobLogs retrieves logs from a completed Job's pod.
func (m *Manager) GetJobLogs(ctx context.Context, jobName string) (string, error) {
	// Find pods for this job
	pods, err := m.clientset.CoreV1().Pods(m.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", jobName),
	})
	if err != nil {
		return "", fmt.Errorf("listing pods for job %s: %w", jobName, err)
	}

	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no pods found for job %s", jobName)
	}

	// Get logs from the first pod
	req := m.clientset.CoreV1().Pods(m.namespace).GetLogs(pods.Items[0].Name, &corev1.PodLogOptions{})
	result := req.Do(ctx)
	raw, err := result.Raw()
	if err != nil {
		return "", fmt.Errorf("getting logs for job %s: %w", jobName, err)
	}

	return string(raw), nil
}

// DeleteJob removes a completed Job and its pods.
func (m *Manager) DeleteJob(ctx context.Context, jobName string) error {
	propagation := metav1.DeletePropagationBackground
	err := m.clientset.BatchV1().Jobs(m.namespace).Delete(ctx, jobName, metav1.DeleteOptions{
		PropagationPolicy: &propagation,
	})
	if err != nil {
		return fmt.Errorf("deleting job %s: %w", jobName, err)
	}
	return nil
}

// CleanupAll removes all Jobs created by this migration.
func (m *Manager) CleanupAll(ctx context.Context) error {
	labelSelector := fmt.Sprintf("app=vm-migrator,migration-id=%s", m.migrationID)
	propagation := metav1.DeletePropagationBackground

	err := m.clientset.BatchV1().Jobs(m.namespace).DeleteCollection(ctx,
		metav1.DeleteOptions{PropagationPolicy: &propagation},
		metav1.ListOptions{LabelSelector: labelSelector},
	)
	if err != nil {
		return fmt.Errorf("cleaning up jobs: %w", err)
	}

	m.logger.Info("Cleaned up all migration jobs", zap.String("migration_id", m.migrationID))
	return nil
}

// CountRunningJobs returns the number of currently running Jobs for this migration.
func (m *Manager) CountRunningJobs(ctx context.Context) (int, error) {
	labelSelector := fmt.Sprintf("app=vm-migrator,migration-id=%s", m.migrationID)
	jobs, err := m.clientset.BatchV1().Jobs(m.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return 0, fmt.Errorf("listing jobs: %w", err)
	}

	running := 0
	for _, job := range jobs.Items {
		if job.Status.Active > 0 {
			running++
		}
	}
	return running, nil
}

// buildJobSpec constructs a Kubernetes Job specification for a vmctl task.
func (m *Manager) buildJobSpec(task *types.Task) *batchv1.Job {
	cfg := m.config
	jobName := fmt.Sprintf("vm-migrator-%s", task.ID)
	if len(jobName) > 63 {
		jobName = jobName[:63]
	}

	// Build vmctl command arguments
	args := m.buildVmctlArgs(task)

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

	var backoffLimit int32 = 0 // Coordinator handles retries
	var ttl int32 = 600         // Clean up finished jobs after 10 min

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: m.namespace,
			Labels: map[string]string{
				"app":          "vm-migrator",
				"component":    "worker",
				"task-id":      task.ID,
				"migration-id": m.migrationID,
			},
			Annotations: map[string]string{
				"vm-migrator/metric":    task.MetricName,
				"vm-migrator/selector":  task.Selector,
				"vm-migrator/time-start": task.TimeStart.Format("2006-01-02T15:04:05Z"),
				"vm-migrator/time-end":   task.TimeEnd.Format("2006-01-02T15:04:05Z"),
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":          "vm-migrator",
						"component":    "worker",
						"task-id":      task.ID,
						"migration-id": m.migrationID,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyNever,
					ServiceAccountName: cfg.Workers.Pod.ServiceAccount,
					Containers: []corev1.Container{
						{
							Name:      "vmctl",
							Image:     cfg.Workers.Pod.Image,
							Args:      args,
							Resources: resources,
						},
					},
				},
			},
		},
	}

	// Apply node selector
	if len(cfg.Workers.Pod.NodeSelector) > 0 {
		job.Spec.Template.Spec.NodeSelector = cfg.Workers.Pod.NodeSelector
	}

	// Apply tolerations
	for _, tol := range cfg.Workers.Pod.Tolerations {
		job.Spec.Template.Spec.Tolerations = append(job.Spec.Template.Spec.Tolerations,
			corev1.Toleration{
				Key:      tol.Key,
				Operator: corev1.TolerationOperator(tol.Operator),
				Value:    tol.Value,
				Effect:   corev1.TaintEffect(tol.Effect),
			})
	}

	return job
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

// JobTaskID extracts the task ID from a Job's labels.
func JobTaskID(job *batchv1.Job) string {
	return job.Labels["task-id"]
}

// IsJobComplete checks if a Job has completed (succeeded or failed).
func IsJobComplete(job *batchv1.Job) bool {
	return IsJobSucceeded(job) || IsJobFailed(job)
}

// IsJobSucceeded checks if a Job completed successfully.
func IsJobSucceeded(job *batchv1.Job) bool {
	return job.Status.Succeeded > 0
}

// IsJobFailed checks if a Job has failed.
func IsJobFailed(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// JobFailureReason extracts the failure reason from a failed Job.
func JobFailureReason(job *batchv1.Job) string {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return c.Message
		}
	}
	return "unknown failure"
}
