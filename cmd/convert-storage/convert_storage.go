package convert_storage

import (
	"context"
	"crypto/md5"
	"fmt"
	"log"
	"time"

	logrusr "github.com/bombsimon/logrusr/v3"
	"github.com/go-logr/logr"
	configv1 "github.com/openshift/api/config/v1"
	routev1 "github.com/openshift/api/route/v1"
	securityv1 "github.com/openshift/api/security/v1"
	openshiftuid "github.com/openshift/library-go/pkg/security/uid"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	errorsutil "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/backube/pvc-transfer/endpoint"
	ingressendpoint "github.com/backube/pvc-transfer/endpoint/ingress"
	routeendpoint "github.com/backube/pvc-transfer/endpoint/route"
	"github.com/backube/pvc-transfer/transfer"
	rsynctransfer "github.com/backube/pvc-transfer/transfer/rsync"
	"github.com/backube/pvc-transfer/transport"
	stunneltransport "github.com/backube/pvc-transfer/transport/stunnel"

	transfer_pvc "github.com/konveyor/crane/cmd/transfer-pvc"
)

type endpointType string

const (
	endpointNginx endpointType = "nginx-ingress"
	endpointRoute endpointType = "route"
)

type ConvertStorageCommand struct {
	configFlags *genericclioptions.ConfigFlags
	genericclioptions.IOStreams

	context        *clientcmdapi.Context
	contextName    string
	resolvedClient client.Client
	restConfig     *rest.Config

	// Single-PVC flags
	PVCName            string
	PVCNamespace       string
	TargetStorageClass string
	TargetPVCName      string
	TargetAccessMode   string
	TargetCapacity     string

	// Networking
	Endpoint     endpointType
	Subdomain    string
	IngressClass string

	// Batch
	PlanFile string

	// Options
	SkipSwap       bool
	Verify         bool
	Image          string
	ProgressOutput string
}

func NewConvertStorageCommand(streams genericclioptions.IOStreams) *cobra.Command {
	c := &ConvertStorageCommand{
		configFlags: genericclioptions.NewConfigFlags(false),
		IOStreams:    streams,
	}

	cmd := &cobra.Command{
		Use:   "convert-storage",
		Short: "Convert PVC storage class within a cluster",
		Long: `Convert PVCs from one StorageClass to another within the same cluster.

Creates a new PVC with the target StorageClass, transfers data via rsync,
and swaps workload references to point to the new PVC. The old PVC is
preserved (labeled, not deleted).

Single PVC:
  crane convert-storage --context mycluster --pvc-name mysql-data \
    --pvc-namespace myapp --target-storage-class gp3 --endpoint route

Batch (plan file):
  crane convert-storage plan --context mycluster --namespace myapp --output plan.yaml
  crane convert-storage --plan plan.yaml --endpoint route`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := c.Complete(cmd, args); err != nil {
				return err
			}
			if err := c.Validate(); err != nil {
				return err
			}
			return c.Run()
		},
	}

	cmd.Flags().StringVar(&c.contextName, "context", "", "Cluster kubeconfig context")
	cmd.Flags().StringVar(&c.PVCName, "pvc-name", "", "Source PVC name")
	cmd.Flags().StringVar(&c.PVCNamespace, "pvc-namespace", "", "PVC namespace")
	cmd.Flags().StringVar(&c.TargetStorageClass, "target-storage-class", "", "Target StorageClass name")
	cmd.Flags().StringVar(&c.TargetPVCName, "target-pvc-name", "", "Override auto-generated target PVC name")
	cmd.Flags().StringVar(&c.TargetAccessMode, "target-access-mode", "", "Override access mode (ReadWriteOnce, ReadWriteMany)")
	cmd.Flags().StringVar(&c.TargetCapacity, "target-capacity", "", "Override storage capacity")
	cmd.Flags().StringVar((*string)(&c.Endpoint), "endpoint", "", "Endpoint type: route or nginx-ingress (auto-detected if omitted)")
	cmd.Flags().StringVar(&c.Subdomain, "subdomain", "", "Subdomain for nginx-ingress endpoint")
	cmd.Flags().StringVar(&c.IngressClass, "ingress-class", "", "IngressClass for nginx-ingress")
	cmd.Flags().StringVar(&c.PlanFile, "plan", "", "Path to plan YAML (batch mode)")
	cmd.Flags().BoolVar(&c.SkipSwap, "skip-swap", false, "Skip workload reference swap")
	cmd.Flags().BoolVar(&c.Verify, "verify", false, "Enable checksum verification after transfer")
	cmd.Flags().StringVar(&c.Image, "image", "quay.io/konveyor/rsync-transfer:latest", "Container image for rsync pods")
	cmd.Flags().StringVar(&c.ProgressOutput, "output", "", "Write transfer stats to file")

	cmd.AddCommand(newPlanCommand(streams))

	return cmd
}

func (c *ConvertStorageCommand) Complete(cmd *cobra.Command, args []string) error {
	config := c.configFlags.ToRawKubeConfigLoader()
	rawConfig, err := config.RawConfig()
	if err != nil {
		return err
	}

	if c.contextName == "" && c.PlanFile != "" {
		plan, err := LoadPlan(c.PlanFile)
		if err != nil {
			return err
		}
		c.contextName = plan.Context
	}

	if c.contextName == "" {
		return fmt.Errorf("--context is required")
	}

	for name, ctx := range rawConfig.Contexts {
		if name == c.contextName {
			c.context = ctx
			break
		}
	}

	if c.PVCNamespace == "" && c.context != nil {
		c.PVCNamespace = c.context.Namespace
	}

	return nil
}

func (c *ConvertStorageCommand) Validate() error {
	if c.context == nil {
		return fmt.Errorf("cannot evaluate context %q", c.contextName)
	}

	if c.PlanFile != "" {
		return nil
	}

	if c.PVCName == "" {
		return fmt.Errorf("--pvc-name is required in single-PVC mode")
	}
	if c.PVCNamespace == "" {
		return fmt.Errorf("--pvc-namespace is required")
	}
	if c.TargetStorageClass == "" {
		return fmt.Errorf("--target-storage-class is required in single-PVC mode")
	}

	return nil
}

func (c *ConvertStorageCommand) Run() error {
	k8sClient, restCfg, err := c.getClient()
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}
	c.resolvedClient = k8sClient
	c.restConfig = restCfg

	if c.PlanFile != "" {
		return c.runBatch(k8sClient)
	}
	return c.runSingle(k8sClient)
}

func (c *ConvertStorageCommand) runSingle(k8sClient client.Client) error {
	suffix, err := GenerateSuffix()
	if err != nil {
		return err
	}

	targetName := c.TargetPVCName
	if targetName == "" {
		targetName = GenerateTargetName(c.PVCName, suffix)
	}

	entry := PVCEntry{
		Name:               c.PVCName,
		TargetStorageClass: c.TargetStorageClass,
		TargetName:         targetName,
		Action:             "convert",
	}

	result := c.convertPVC(k8sClient, c.PVCNamespace, entry)
	if result.err != nil {
		return fmt.Errorf("conversion failed for PVC %s: %w", c.PVCName, result.err)
	}

	if !c.SkipSwap {
		log.Printf("Swapping workload references in namespace %s", c.PVCNamespace)
		pvcMapping := map[string]string{c.PVCName: targetName}

		swapped, err := SwapWorkloadPVCReferences(k8sClient, c.PVCNamespace, c.PVCName, targetName, c.TargetStorageClass)
		if err != nil {
			return fmt.Errorf("workload reference swap failed: %w", err)
		}
		for _, s := range swapped {
			log.Printf("  Updated %s: %s -> %s", s, c.PVCName, targetName)
		}

		ssSwapped, err := DetectAndSwapStatefulSetTemplates(k8sClient, c.PVCNamespace, pvcMapping, c.TargetStorageClass)
		if err != nil {
			return fmt.Errorf("StatefulSet template swap failed: %w", err)
		}
		for _, s := range ssSwapped {
			log.Printf("  Updated %s", s)
		}

		if err := RestoreWorkloadReplicas(k8sClient, c.PVCNamespace, result.origReplicas); err != nil {
			log.Printf("WARN: failed to restore replicas: %v", err)
		}
	}

	log.Printf("Done. Old PVC %s preserved (delete manually after verification)", c.PVCName)
	return nil
}

func (c *ConvertStorageCommand) runBatch(k8sClient client.Client) error {
	plan, err := LoadPlan(c.PlanFile)
	if err != nil {
		return err
	}

	if c.PVCNamespace == "" {
		c.PVCNamespace = plan.Namespace
	}

	if c.Endpoint == "" && plan.Endpoint != "" {
		c.Endpoint = endpointType(plan.Endpoint)
	}
	if c.Subdomain == "" && plan.Subdomain != "" {
		c.Subdomain = plan.Subdomain
	}

	type pvcSwapEntry struct {
		oldName  string
		newName  string
		targetSC string
	}

	convertCount := 0
	skipCount := 0
	var failedPVCs []string
	var swapEntries []pvcSwapEntry
	allOrigReplicas := make(map[string]int32)

	for i, entry := range plan.PVCs {
		if entry.Action == "skip" {
			skipCount++
			log.Printf("[%d/%d] Skipping PVC %s", i+1, len(plan.PVCs), entry.Name)
			continue
		}

		log.Printf("[%d/%d] %s -> %s (%s)", i+1, len(plan.PVCs), entry.Name, entry.TargetName, entry.TargetStorageClass)
		result := c.convertPVC(k8sClient, plan.Namespace, entry)
		if result.err != nil {
			log.Printf("  FAILED: %v", result.err)
			failedPVCs = append(failedPVCs, entry.Name)
			continue
		}
		convertCount++
		swapEntries = append(swapEntries, pvcSwapEntry{
			oldName: entry.Name, newName: entry.TargetName, targetSC: entry.TargetStorageClass,
		})
		for k, v := range result.origReplicas {
			allOrigReplicas[k] = v
		}
		log.Printf("  OK")
	}

	if !c.SkipSwap && len(swapEntries) > 0 {
		log.Printf("Swapping workload references in namespace %s", c.PVCNamespace)
		pvcMapping := make(map[string]string)
		lastTargetSC := ""
		for _, se := range swapEntries {
			pvcMapping[se.oldName] = se.newName
			lastTargetSC = se.targetSC

			swapped, err := SwapWorkloadPVCReferences(k8sClient, c.PVCNamespace, se.oldName, se.newName, se.targetSC)
			if err != nil {
				log.Printf("  WARN: swap failed for %s -> %s: %v", se.oldName, se.newName, err)
				continue
			}
			for _, s := range swapped {
				log.Printf("  Updated %s: %s -> %s", s, se.oldName, se.newName)
			}
		}

		ssSwapped, err := DetectAndSwapStatefulSetTemplates(k8sClient, c.PVCNamespace, pvcMapping, lastTargetSC)
		if err != nil {
			log.Printf("  WARN: StatefulSet template swap failed: %v", err)
		} else {
			for _, s := range ssSwapped {
				log.Printf("  Updated %s", s)
			}
		}

		if err := RestoreWorkloadReplicas(k8sClient, c.PVCNamespace, allOrigReplicas); err != nil {
			log.Printf("WARN: failed to restore replicas: %v", err)
		}
	}

	log.Printf("Done. %d PVC(s) converted, %d skipped. Old PVCs preserved.", convertCount, skipCount)
	if len(failedPVCs) > 0 {
		return fmt.Errorf("%d PVC(s) failed: %v", len(failedPVCs), failedPVCs)
	}
	return nil
}

type conversionResult struct {
	err          error
	origReplicas map[string]int32
}

func (c *ConvertStorageCommand) convertPVC(k8sClient client.Client, namespace string, entry PVCEntry) conversionResult {
	// Step 1: Pre-flight validation
	srcPVC := &corev1.PersistentVolumeClaim{}
	if err := k8sClient.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: entry.Name}, srcPVC); err != nil {
		return conversionResult{err: fmt.Errorf("source PVC %s/%s not found: %w", namespace, entry.Name, err)}
	}
	if srcPVC.Status.Phase != corev1.ClaimBound {
		return conversionResult{err: fmt.Errorf("source PVC %s/%s is not Bound (phase: %s)", namespace, entry.Name, srcPVC.Status.Phase)}
	}

	targetSC := &unstructured.Unstructured{}
	targetSC.SetGroupVersionKind(schema.GroupVersionKind{Group: "storage.k8s.io", Version: "v1", Kind: "StorageClass"})
	if err := k8sClient.Get(context.TODO(), client.ObjectKey{Name: entry.TargetStorageClass}, targetSC); err != nil {
		if errors.IsForbidden(err) {
			log.Printf("WARN: cannot verify target StorageClass %q (insufficient permissions), proceeding anyway", entry.TargetStorageClass)
		} else if errors.IsNotFound(err) {
			return conversionResult{err: fmt.Errorf("target StorageClass %q does not exist on the cluster", entry.TargetStorageClass)}
		} else {
			return conversionResult{err: fmt.Errorf("failed to check target StorageClass %q: %w", entry.TargetStorageClass, err)}
		}
	}

	existingPVC := &corev1.PersistentVolumeClaim{}
	if err := k8sClient.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: entry.TargetName}, existingPVC); err == nil {
		return conversionResult{err: fmt.Errorf("target PVC %s/%s already exists (collision)", namespace, entry.TargetName)}
	}

	// Step 2: Quiesce workloads that reference this PVC
	log.Printf("Quiescing workloads referencing PVC %s", entry.Name)
	origReplicas, err := QuiesceWorkloadsForPVC(k8sClient, namespace, entry.Name)
	if err != nil {
		return conversionResult{err: fmt.Errorf("failed to quiesce workloads: %w", err)}
	}

	// Step 3: Build and create destination PVC
	destPVC := c.buildDestinationPVC(srcPVC, entry)
	log.Printf("Creating PVC %s (%s, %s) in namespace %s",
		destPVC.Name, entry.TargetStorageClass,
		destPVC.Spec.Resources.Requests.Storage().String(), namespace)
	if err := k8sClient.Create(context.TODO(), destPVC); err != nil {
		return conversionResult{err: fmt.Errorf("failed to create destination PVC: %w", err)}
	}

	// Step 4: Transfer data via rsync
	log.Printf("Transferring data: %s -> %s", entry.Name, entry.TargetName)
	if err := c.transferData(k8sClient, namespace, srcPVC, destPVC); err != nil {
		return conversionResult{err: fmt.Errorf("data transfer failed: %w", err)}
	}
	log.Printf("Transfer complete")

	// Step 5: Label old PVC
	if err := c.labelOldPVC(k8sClient, srcPVC, entry.TargetName); err != nil {
		log.Printf("WARN: failed to label old PVC %s: %v", entry.Name, err)
	}

	return conversionResult{origReplicas: origReplicas}
}

func (c *ConvertStorageCommand) buildDestinationPVC(srcPVC *corev1.PersistentVolumeClaim, entry PVCEntry) *corev1.PersistentVolumeClaim {
	pvc := &corev1.PersistentVolumeClaim{}
	pvc.Namespace = srcPVC.Namespace
	pvc.Name = entry.TargetName
	pvc.Labels = srcPVC.Labels
	pvc.Spec = *srcPVC.Spec.DeepCopy()
	pvc.Spec.StorageClassName = &entry.TargetStorageClass
	pvc.Spec.VolumeMode = nil
	pvc.Spec.VolumeName = ""

	if c.TargetAccessMode != "" {
		pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{
			corev1.PersistentVolumeAccessMode(c.TargetAccessMode),
		}
	}

	if c.TargetCapacity != "" {
		q, err := resource.ParseQuantity(c.TargetCapacity)
		if err == nil {
			pvc.Spec.Resources.Requests[corev1.ResourceStorage] = q
		}
	}

	return pvc
}

func (c *ConvertStorageCommand) labelOldPVC(k8sClient client.Client, pvc *corev1.PersistentVolumeClaim, targetName string) error {
	if pvc.Labels == nil {
		pvc.Labels = make(map[string]string)
	}
	pvc.Labels[labelMigratedTo] = targetName

	suffix, _ := GenerateSuffix()
	pvc.Labels[labelMigratedBy] = suffix

	return k8sClient.Update(context.TODO(), pvc)
}

func (c *ConvertStorageCommand) transferData(k8sClient client.Client, namespace string, srcPVC, destPVC *corev1.PersistentVolumeClaim) error {
	logrusLog := logrus.New()
	logrusLog.SetFormatter(&logrus.JSONFormatter{})
	logger := logrusr.New(logrusLog).WithName("convert-storage")

	// Intra-cluster: server and client pods are in the SAME namespace.
	// Use distinct label sets so the log reader can find the right pod.
	serverLabels := map[string]string{
		"app.kubernetes.io/name":          "crane",
		"app.kubernetes.io/component":     "convert-storage",
		"app.konveyor.io/role":            "server",
		"app.konveyor.io/created-for-pvc": getValidatedResourceName(destPVC.Name),
	}
	clientLabels := map[string]string{
		"app.kubernetes.io/name":          "crane",
		"app.kubernetes.io/component":     "convert-storage",
		"app.konveyor.io/role":            "client",
		"app.konveyor.io/created-for-pvc": getValidatedResourceName(srcPVC.Name),
	}

	epType := c.Endpoint
	if epType == "" {
		epType = c.detectEndpoint(k8sClient)
	}

	e, err := c.createEndpoint(epType, destPVC, serverLabels, logger, k8sClient)
	if err != nil {
		return fmt.Errorf("failed creating endpoint: %w", err)
	}

	if err := waitForEndpoint(e, k8sClient); err != nil {
		return fmt.Errorf("endpoint not healthy: %w", err)
	}

	stunnelServer, err := stunneltransport.NewServer(
		context.TODO(), k8sClient, logger,
		types.NamespacedName{
			Name:      getValidatedResourceName(destPVC.Name),
			Namespace: destPVC.Namespace,
		}, e, &transport.Options{
			Labels: serverLabels,
			Image:  c.Image,
		})
	if err != nil {
		return fmt.Errorf("error creating stunnel server: %w", err)
	}

	// Intra-cluster: server and client are in the same namespace but the
	// pvc-transfer library derives the cert secret name from the NamespacedName
	// passed to NewServer/NewClient. Server uses destPVC name, client uses
	// srcPVC name — so the secret names differ. Copy the server's cert secret
	// under the name the client will look for (same approach as cross-cluster
	// transfer-pvc, which copies certs from dest cluster to source cluster).
	serverSecretName := fmt.Sprintf("stunnel-creds-certs-%s", getValidatedResourceName(destPVC.Name))
	clientSecretName := fmt.Sprintf("stunnel-creds-certs-%s", getValidatedResourceName(srcPVC.Name))

	serverSecret := &corev1.Secret{}
	if err := k8sClient.Get(context.TODO(), client.ObjectKey{
		Namespace: namespace, Name: serverSecretName,
	}, serverSecret); err != nil {
		return fmt.Errorf("failed to get server cert secret %s: %w", serverSecretName, err)
	}

	clientCertSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clientSecretName,
			Namespace: namespace,
			Labels:    clientLabels,
		},
		Data: serverSecret.Data,
	}
	if err := k8sClient.Create(context.TODO(), clientCertSecret); err != nil {
		if !errors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create client cert secret: %w", err)
		}
	}

	destPVCList := transfer.NewSingletonPVC(destPVC)
	srcPVCList := transfer.NewSingletonPVC(srcPVC)

	clientPodSecCtx, err := getIDsForNamespace(k8sClient, srcPVC.Namespace, srcPVC.Name)
	if err != nil {
		return fmt.Errorf("error creating security context for rsync client: %w", err)
	}

	serverPodSecCtx := clientPodSecCtx

	trueBool := true
	falseBool := false
	rsyncServer, err := rsynctransfer.NewServer(
		context.TODO(), k8sClient, logger, destPVCList, stunnelServer, e, serverLabels, nil,
		transfer.PodOptions{
			ContainerSecurityContext: corev1.SecurityContext{
				Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				RunAsNonRoot:             &trueBool,
				AllowPrivilegeEscalation: &falseBool,
				SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			PodSecurityContext: corev1.PodSecurityContext{
				RunAsUser:  serverPodSecCtx.RunAsUser,
				RunAsGroup: serverPodSecCtx.RunAsGroup,
				FSGroup:    serverPodSecCtx.FSGroup,
			},
			Image: c.Image,
		},
	)
	if err != nil {
		return fmt.Errorf("error creating rsync server: %w", err)
	}

	_ = wait.PollUntil(time.Second*5, func() (done bool, err error) {
		ready, err := rsyncServer.IsHealthy(context.TODO(), k8sClient)
		if err != nil {
			log.Println(err, "unable to check rsync server health, retrying...")
			return false, nil
		}
		return ready, nil
	}, make(<-chan struct{}))

	nodeName, err := getNodeNameForPVC(k8sClient, srcPVC.Namespace, srcPVC.Name)
	if err != nil {
		return fmt.Errorf("failed to find node name: %w", err)
	}

	stunnelClient, err := stunneltransport.NewClient(
		context.TODO(), k8sClient, logger,
		types.NamespacedName{
			Name:      getValidatedResourceName(srcPVC.Name),
			Namespace: srcPVC.Namespace,
		}, e.Hostname(), e.IngressPort(), &transport.Options{
			Labels: clientLabels,
			Image:  c.Image,
		},
	)
	if err != nil {
		return fmt.Errorf("error creating stunnel client: %w", err)
	}

	_, err = rsynctransfer.NewClient(
		context.TODO(), k8sClient, srcPVCList, stunnelClient, logger, "rsync-client", clientLabels, nil,
		transfer.PodOptions{
			NodeName: nodeName,
			CommandOptions: rsynctransfer.NewDefaultOptionsFrom(
				transfer_pvc.Verify(c.Verify),
				transfer_pvc.RestrictedContainers(true),
				transfer_pvc.Verbose(true),
			),
			ContainerSecurityContext: corev1.SecurityContext{
				Privileged:               &falseBool,
				Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				RunAsNonRoot:             &trueBool,
				AllowPrivilegeEscalation: &falseBool,
			},
			PodSecurityContext: corev1.PodSecurityContext{
				RunAsUser:  clientPodSecCtx.RunAsUser,
				RunAsGroup: clientPodSecCtx.RunAsGroup,
				FSGroup:    clientPodSecCtx.FSGroup,
			},
			Image: c.Image,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create rsync client: %w", err)
	}

	err = transfer_pvc.FollowClientLogs(
		c.restConfig, types.NamespacedName{Name: srcPVC.Name, Namespace: srcPVC.Namespace}, clientLabels, c.ProgressOutput)
	if err != nil {
		return fmt.Errorf("error following rsync client logs: %w", err)
	}

	// Garbage collect both server and client resources
	if err := c.garbageCollect(k8sClient, serverLabels, namespace); err != nil {
		log.Printf("WARN: server-side cleanup: %v", err)
	}
	return c.garbageCollect(k8sClient, clientLabels, namespace)
}

func (c *ConvertStorageCommand) detectEndpoint(k8sClient client.Client) endpointType {
	apiList := &metav1.APIResourceList{}
	apiList.GroupVersion = "route.openshift.io/v1"
	// Check if Route API exists on the cluster
	routeList := &unstructured.UnstructuredList{}
	routeList.SetGroupVersionKind(routeGVK())
	if err := k8sClient.List(context.TODO(), routeList, client.InNamespace("default"), client.Limit(1)); err == nil {
		return endpointRoute
	}
	return endpointNginx
}

func (c *ConvertStorageCommand) createEndpoint(
	epType endpointType, pvc *corev1.PersistentVolumeClaim,
	labels map[string]string, logger logr.Logger, k8sClient client.Client) (endpoint.Endpoint, error) {
	switch epType {
	case endpointNginx:
		annotations := map[string]string{
			ingressendpoint.NginxIngressPassthroughAnnotation: "true",
		}
		if err := ingressendpoint.AddToScheme(scheme.Scheme); err != nil {
			return nil, err
		}
		ingressClass := c.IngressClass
		if ingressClass == "" {
			ingressClass = "nginx"
		}
		return ingressendpoint.New(
			context.TODO(), k8sClient, logger,
			types.NamespacedName{Namespace: pvc.Namespace, Name: getValidatedResourceName(pvc.Name)},
			&ingressClass, c.Subdomain, labels, annotations, nil)
	case endpointRoute:
		if err := routeendpoint.AddToScheme(scheme.Scheme); err != nil {
			return nil, err
		}
		resourceName := types.NamespacedName{Namespace: pvc.Namespace, Name: getValidatedResourceName(pvc.Name)}
		hostname, err := getRouteHostName(k8sClient, resourceName)
		if err != nil {
			return nil, err
		}
		return routeendpoint.New(
			context.TODO(), k8sClient, logger,
			resourceName, routeendpoint.EndpointTypePassthrough, hostname, labels, nil)
	default:
		return nil, fmt.Errorf("unrecognized endpoint type %q", epType)
	}
}

func (c *ConvertStorageCommand) garbageCollect(k8sClient client.Client, labels map[string]string, namespace string) error {
	gvk := []client.Object{
		&corev1.Pod{},
		&corev1.ConfigMap{},
		&corev1.Secret{},
	}
	switch c.Endpoint {
	case endpointRoute:
		gvk = append(gvk, &routev1.Route{})
	case endpointNginx:
		gvk = append(gvk, &networkingv1.Ingress{})
	}

	if err := deleteResourcesForGVK(k8sClient, gvk, labels, namespace); err != nil {
		return err
	}
	return deleteResourcesIteratively(k8sClient, []client.Object{
		&corev1.Service{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Service",
				APIVersion: corev1.SchemeGroupVersion.Version,
			},
		},
	}, labels, namespace)
}

func (c *ConvertStorageCommand) getClient() (client.Client, *rest.Config, error) {
	ctx := c.contextName
	c.configFlags.Context = &ctx
	restCfg, err := c.configFlags.ToRESTConfig()
	if err != nil {
		return nil, nil, err
	}

	if err := routev1.Install(scheme.Scheme); err != nil {
		return nil, nil, err
	}
	if err := configv1.AddToScheme(scheme.Scheme); err != nil {
		return nil, nil, err
	}

	k8sClient, err := client.New(restCfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		return nil, nil, err
	}
	return k8sClient, restCfg, nil
}

// --- Helper functions reused from transfer-pvc patterns ---

func getValidatedResourceName(name string) string {
	if len(name) < 63 {
		return name
	}
	return fmt.Sprintf("crane-%x", md5.Sum([]byte(name)))
}

func getNodeNameForPVC(c client.Client, namespace, pvcName string) (string, error) {
	podList := corev1.PodList{}
	if err := c.List(context.TODO(), &podList, client.InNamespace(namespace)); err != nil {
		return "", err
	}
	for _, pod := range podList.Items {
		if pod.Status.Phase == corev1.PodRunning {
			for _, vol := range pod.Spec.Volumes {
				if vol.PersistentVolumeClaim != nil && vol.PersistentVolumeClaim.ClaimName == pvcName {
					return pod.Spec.NodeName, nil
				}
			}
		}
	}
	return "", nil
}

func getIDsForNamespace(c client.Client, namespace, pvcName string) (*corev1.PodSecurityContext, error) {
	ps := &corev1.PodSecurityContext{}
	ns := &corev1.Namespace{}
	if err := c.Get(context.TODO(), types.NamespacedName{Name: namespace}, ns); err != nil {
		return nil, err
	}
	if annotationVal, found := ns.Annotations[securityv1.UIDRangeAnnotation]; found {
		uidBlock, err := openshiftuid.ParseBlock(annotationVal)
		if err != nil {
			log.Printf("malformed UID range annotation %q in namespace %s: %v, falling back", annotationVal, namespace, err)
		} else {
			min := int64(uidBlock.Start)
			ps.RunAsUser = &min
		}
	}
	if annotationVal, found := ns.Annotations[securityv1.SupplementalGroupsAnnotation]; found {
		uidBlock, err := openshiftuid.ParseBlock(annotationVal)
		if err != nil {
			log.Printf("malformed supplemental groups annotation %q in namespace %s: %v", annotationVal, namespace, err)
		} else {
			min := int64(uidBlock.Start)
			ps.RunAsGroup = &min
			ps.FSGroup = &min
		}
	}
	if ps.RunAsUser != nil {
		return ps, nil
	}
	return ps, nil
}

func getRouteHostName(c client.Client, namespacedName types.NamespacedName) (*string, error) {
	routeNamePrefix := fmt.Sprintf("%s-%s", namespacedName.Name, namespacedName.Namespace)
	if len(routeNamePrefix) <= 62 {
		return nil, nil
	}
	ingressConfig := &configv1.Ingress{}
	if err := c.Get(context.TODO(), types.NamespacedName{Name: "cluster"}, ingressConfig); err != nil {
		return nil, err
	}
	hash := fmt.Sprintf("%x", md5.Sum([]byte(routeNamePrefix)))[:8]
	truncated := routeNamePrefix[:62-9] + "-" + hash
	hostname := fmt.Sprintf("%s.%s", truncated, ingressConfig.Spec.Domain)
	return &hostname, nil
}

func waitForEndpoint(e endpoint.Endpoint, k8sClient client.Client) error {
	return wait.PollUntil(time.Second*5, func() (done bool, err error) {
		ready, err := e.IsHealthy(context.TODO(), k8sClient)
		if err != nil {
			log.Println(err, "unable to check endpoint health, retrying...")
			return false, nil
		}
		return ready, nil
	}, make(<-chan struct{}))
}

func deleteResourcesForGVK(c client.Client, gvk []client.Object, labels map[string]string, namespace string) error {
	for _, obj := range gvk {
		if err := c.DeleteAllOf(context.TODO(), obj, client.InNamespace(namespace), client.MatchingLabels(labels)); err != nil {
			return err
		}
	}
	return nil
}

func deleteResourcesIteratively(c client.Client, iterativeTypes []client.Object, labels map[string]string, namespace string) error {
	listOptions := []client.ListOption{
		client.MatchingLabels(labels),
		client.InNamespace(namespace),
	}
	var errs []error
	for _, objList := range iterativeTypes {
		ulist := &unstructured.UnstructuredList{}
		ulist.SetGroupVersionKind(objList.GetObjectKind().GroupVersionKind())
		if err := c.List(context.TODO(), ulist, listOptions...); err != nil {
			errs = append(errs, err)
			continue
		}
		for _, item := range ulist.Items {
			if err := c.Delete(context.TODO(), &item, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errorsutil.NewAggregate(errs)
}

func storageClassGVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: "storage.k8s.io", Version: "v1", Kind: "StorageClassList"}
}

func routeGVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: "route.openshift.io", Version: "v1", Kind: "RouteList"}
}

