package transfer_pvc

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	indirectLabel     = "app.kubernetes.io/component"
	indirectLabelVal  = "indirect-transfer"
	rcloneImage       = "quay.io/konveyor/rsync-transfer:latest"
	gcpSAKeyMountPath = "/etc/rclone"
)

func (t *TransferPVCCommand) runIndirect() error {
	storageCfg, err := LoadStorageConfig(t.Flags.StorageConfigPath)
	if err != nil {
		return err
	}

	srcClient, err := t.getClientFromContext(t.Flags.SourceContext)
	if err != nil {
		return fmt.Errorf("unable to get source client: %w", err)
	}
	destClient, err := t.getClientFromContext(t.Flags.DestinationContext)
	if err != nil {
		return fmt.Errorf("unable to get destination client: %w", err)
	}

	srcRestCfg, err := t.getRestConfigFromContext(t.Flags.SourceContext)
	if err != nil {
		return fmt.Errorf("unable to get source REST config: %w", err)
	}
	destRestCfg, err := t.getRestConfigFromContext(t.Flags.DestinationContext)
	if err != nil {
		return fmt.Errorf("unable to get destination REST config: %w", err)
	}

	// Step 1: Backup from source
	log.Printf("Phase 1: Backing up PVC %s/%s to %s %s",
		t.PVC.Namespace.source, t.PVC.Name.source,
		storageCfg.Provider, storageCfg.BucketOrContainer())

	if err := t.runRclonePhase(srcClient, srcRestCfg, storageCfg,
		t.PVC.Namespace.source, t.PVC.Name.source, "backup"); err != nil {
		return fmt.Errorf("backup failed: %w", err)
	}
	log.Printf("Backup complete")

	// Step 2: Create destination PVC
	srcPVC := &corev1.PersistentVolumeClaim{}
	if err := srcClient.Get(context.TODO(), client.ObjectKey{
		Namespace: t.PVC.Namespace.source, Name: t.PVC.Name.source,
	}, srcPVC); err != nil {
		return fmt.Errorf("unable to get source PVC: %w", err)
	}

	destPVC := t.buildDestinationPVC(srcPVC)
	if err := destClient.Create(context.TODO(), destPVC); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("unable to create destination PVC: %w", err)
	}
	log.Printf("Destination PVC %s/%s created (%s)",
		destPVC.Namespace, destPVC.Name,
		func() string {
			if destPVC.Spec.StorageClassName != nil {
				return *destPVC.Spec.StorageClassName
			}
			return "default"
		}())

	// Step 3: Restore to destination
	log.Printf("Phase 2: Restoring PVC %s/%s from %s %s",
		t.PVC.Namespace.destination, t.PVC.Name.destination,
		storageCfg.Provider, storageCfg.BucketOrContainer())

	if err := t.runRclonePhase(destClient, destRestCfg, storageCfg,
		t.PVC.Namespace.destination, t.PVC.Name.destination, "restore"); err != nil {
		return fmt.Errorf("restore failed: %w", err)
	}
	log.Printf("Restore complete")

	log.Printf("Indirect transfer complete: %s/%s -> %s/%s via %s",
		t.PVC.Namespace.source, t.PVC.Name.source,
		t.PVC.Namespace.destination, t.PVC.Name.destination,
		storageCfg.Provider)
	return nil
}

func (t *TransferPVCCommand) runRclonePhase(
	k8sClient client.Client,
	restCfg *rest.Config,
	storageCfg *StorageConfig,
	namespace, pvcName, direction string,
) error {
	podSecCtx, err := getIDsForNamespace(k8sClient, namespace, pvcName)
	if err != nil {
		log.Printf("WARN: UID detection failed for %s/%s: %v", namespace, pvcName, err)
		podSecCtx = &corev1.PodSecurityContext{}
	}

	// Create temporary credentials Secret
	secretName := fmt.Sprintf("crane-indirect-%s-%s", direction, getValidatedResourceName(pvcName))
	credSecret := storageCfg.ToCredentialsSecret(secretName, namespace)
	if err := k8sClient.Create(context.TODO(), credSecret); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create credentials secret: %w", err)
	}
	defer func() {
		if err := k8sClient.Delete(context.TODO(), credSecret); err != nil {
			log.Printf("WARN: failed to delete credentials secret %s: %v", secretName, err)
		}
	}()

	// Build rclone command
	remotePath := storageCfg.RemotePath(pvcName)
	var rcloneCmd string
	if direction == "backup" {
		rcloneCmd = fmt.Sprintf("rclone sync /data %s -v --progress --stats 5s", remotePath)
	} else {
		rcloneCmd = fmt.Sprintf("rclone sync %s /data -v --progress --stats 5s", remotePath)
	}

	if t.Flags.BandwidthLimit != "" {
		rcloneCmd += fmt.Sprintf(" --bwlimit %s", t.Flags.BandwidthLimit)
	}

	// Build pod
	podName := fmt.Sprintf("crane-rclone-%s-%s", direction, getValidatedResourceName(pvcName))
	if len(podName) > 63 {
		podName = podName[:63]
	}

	image := rcloneImage
	if direction == "backup" && t.Flags.SourceImage != "" {
		image = t.Flags.SourceImage
	} else if direction == "restore" && t.Flags.DestinationImage != "" {
		image = t.Flags.DestinationImage
	}

	trueBool := true
	falseBool := false
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "crane",
				indirectLabel:                 indirectLabelVal,
				"app.konveyor.io/direction":   direction,
				"app.konveyor.io/pvc":         getValidatedResourceName(pvcName),
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			SecurityContext: &corev1.PodSecurityContext{
				RunAsUser:  podSecCtx.RunAsUser,
				RunAsGroup: podSecCtx.RunAsGroup,
				FSGroup:    podSecCtx.FSGroup,
			},
			Containers: []corev1.Container{{
				Name:    "rclone",
				Image:   image,
				Command: []string{"sh", "-c", rcloneCmd},
				Env:     storageCfg.ToRcloneEnvVars(),
				SecurityContext: &corev1.SecurityContext{
					RunAsNonRoot:             &trueBool,
					AllowPrivilegeEscalation: &falseBool,
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
					SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
				},
				VolumeMounts: []corev1.VolumeMount{{
					Name:      "data",
					MountPath: "/data",
				}},
			}},
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: pvcName,
					},
				},
			}},
		},
	}

	// For GCP, mount the service account key file
	if strings.ToLower(storageCfg.Provider) == "gcp" && storageCfg.ServiceAccountKey != "" {
		pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{
			Name:  "RCLONE_CONFIG_REMOTE_SERVICE_ACCOUNT_FILE",
			Value: gcpSAKeyMountPath + "/sa-key.json",
		})
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
			Name:      "gcp-sa-key",
			MountPath: gcpSAKeyMountPath,
			ReadOnly:  true,
		})
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: "gcp-sa-key",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: secretName,
					Items: []corev1.KeyToPath{{
						Key:  "sa-key.json",
						Path: "sa-key.json",
					}},
				},
			},
		})
	}

	// Create pod
	if err := k8sClient.Create(context.TODO(), pod); err != nil {
		return fmt.Errorf("failed to create rclone %s pod: %w", direction, err)
	}
	defer func() {
		if err := k8sClient.Delete(context.TODO(), pod); err != nil {
			log.Printf("WARN: failed to delete rclone pod %s: %v", podName, err)
		}
	}()

	// Wait for pod to complete
	log.Printf("Waiting for rclone %s pod %s to complete...", direction, podName)
	if err := t.followRclonePodLogs(restCfg, namespace, podName); err != nil {
		return fmt.Errorf("rclone %s failed: %w", direction, err)
	}

	// Check exit code
	completedPod := &corev1.Pod{}
	if err := k8sClient.Get(context.TODO(), client.ObjectKey{
		Namespace: namespace, Name: podName,
	}, completedPod); err != nil {
		return fmt.Errorf("failed to get completed pod status: %w", err)
	}

	if completedPod.Status.Phase == corev1.PodFailed {
		return fmt.Errorf("rclone %s pod failed", direction)
	}

	if len(completedPod.Status.ContainerStatuses) > 0 {
		cs := completedPod.Status.ContainerStatuses[0]
		if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
			return fmt.Errorf("rclone %s exited with code %d", direction, cs.State.Terminated.ExitCode)
		}
	}

	return nil
}

func (t *TransferPVCCommand) followRclonePodLogs(restCfg *rest.Config, namespace, podName string) error {
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("failed to create clientset: %w", err)
	}

	// Wait for pod to be running or completed
	if err := wait.PollUntil(time.Second*3, func() (bool, error) {
		pod, err := clientset.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		switch pod.Status.Phase {
		case corev1.PodRunning, corev1.PodSucceeded, corev1.PodFailed:
			return true, nil
		}
		return false, nil
	}, make(<-chan struct{})); err != nil {
		return fmt.Errorf("timeout waiting for pod to start: %w", err)
	}

	req := clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: "rclone",
		Follow:    true,
	})

	stream, err := req.Stream(context.TODO())
	if err != nil {
		return fmt.Errorf("failed to stream logs: %w", err)
	}
	defer stream.Close()

	buf := make([]byte, 4096)
	for {
		n, err := stream.Read(buf)
		if n > 0 {
			fmt.Print(string(buf[:n]))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
	}

	// Wait for pod to actually complete
	return wait.PollUntil(time.Second*2, func() (bool, error) {
		pod, err := clientset.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed, nil
	}, make(<-chan struct{}))
}
