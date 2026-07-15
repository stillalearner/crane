package convert_storage

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strings"

	configv1 "github.com/openshift/api/config/v1"
	routev1 "github.com/openshift/api/route/v1"
	"github.com/spf13/cobra"
	appsv1 "k8s.io/api/apps/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes/scheme"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type planOptions struct {
	configFlags   *genericclioptions.ConfigFlags
	Context       string
	Namespace     string
	Output        string
	LabelSelector string
}

func newPlanCommand(streams genericclioptions.IOStreams) *cobra.Command {
	o := &planOptions{
		configFlags: genericclioptions.NewConfigFlags(false),
	}

	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Generate a storage conversion plan for a namespace",
		Long: `Discovers PVCs and StorageClasses in a namespace and generates an editable
YAML plan file. Review and edit the plan, then run 'crane convert-storage --plan <file>'.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Validate(); err != nil {
				return err
			}
			return o.Run()
		},
	}

	cmd.Flags().StringVar(&o.Context, "context", "", "Cluster kubeconfig context")
	cmd.Flags().StringVar(&o.Namespace, "namespace", "", "Namespace to discover PVCs in")
	cmd.Flags().StringVar(&o.Output, "output", "", "Path to write plan YAML")
	cmd.Flags().StringVar(&o.LabelSelector, "label-selector", "", "Filter PVCs by label selector")
	cmd.MarkFlagRequired("context")
	cmd.MarkFlagRequired("namespace")
	cmd.MarkFlagRequired("output")

	return cmd
}

func (o *planOptions) Validate() error {
	if o.Context == "" {
		return fmt.Errorf("--context is required")
	}
	if o.Namespace == "" {
		return fmt.Errorf("--namespace is required")
	}
	if o.Output == "" {
		return fmt.Errorf("--output is required")
	}
	return nil
}

func (o *planOptions) Run() error {
	ctx := o.Context
	o.configFlags.Context = &ctx
	restCfg, err := o.configFlags.ToRESTConfig()
	if err != nil {
		return fmt.Errorf("failed to get REST config: %w", err)
	}

	if err := routev1.Install(scheme.Scheme); err != nil {
		return err
	}
	if err := configv1.AddToScheme(scheme.Scheme); err != nil {
		return err
	}

	k8sClient, err := client.New(restCfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	// List PVCs in namespace
	pvcList := &corev1.PersistentVolumeClaimList{}
	listOpts := []client.ListOption{client.InNamespace(o.Namespace)}
	if o.LabelSelector != "" {
		labels, err := parseLabelSelector(o.LabelSelector)
		if err != nil {
			return fmt.Errorf("invalid label selector: %w", err)
		}
		listOpts = append(listOpts, client.MatchingLabels(labels))
	}
	if err := k8sClient.List(context.TODO(), pvcList, listOpts...); err != nil {
		return fmt.Errorf("failed to list PVCs in namespace %s: %w", o.Namespace, err)
	}

	if len(pvcList.Items) == 0 {
		return fmt.Errorf("no PVCs found in namespace %s", o.Namespace)
	}

	// List StorageClasses on cluster (best-effort — non-admin may not have access)
	scList := &storagev1.StorageClassList{}
	scListAvailable := true
	if err := k8sClient.List(context.TODO(), scList); err != nil {
		log.Printf("WARN: cannot list StorageClasses (%v). Target StorageClass must be set manually in the plan file.", err)
		scListAvailable = false
	}

	if scListAvailable && len(scList.Items) < 2 {
		log.Printf("WARN: only %d StorageClass(es) found on the cluster. At least 2 are needed for conversion.", len(scList.Items))
	}

	// Build provisioner and default SC maps
	scByProvisioner := make(map[string][]string)
	var defaultSC string
	if scListAvailable {
		for _, sc := range scList.Items {
			scByProvisioner[sc.Provisioner] = append(scByProvisioner[sc.Provisioner], sc.Name)
			if sc.Annotations["storageclass.kubernetes.io/is-default-class"] == "true" {
				defaultSC = sc.Name
			}
		}
	}

	suffix, err := GenerateSuffix()
	if err != nil {
		return fmt.Errorf("failed to generate suffix: %w", err)
	}

	plan := BuildPlanFromPVCs(pvcList.Items, o.Context, o.Namespace, suffix)

	// Auto-suggest target StorageClass for each PVC (only if SC list is available)
	if scListAvailable {
		for i := range plan.PVCs {
			pvc := &plan.PVCs[i]
			suggested := suggestTargetSC(pvc.SourceStorageClass, scList.Items, scByProvisioner, defaultSC)
			pvc.TargetStorageClass = suggested
		}
	}

	// Fix target names for StatefulSet PVCs to follow K8s naming convention.
	// PVC pattern: <templateName>-<stsName>-<ordinal>
	// Correct target: <templateName>-mig-<suffix>-<stsName>-<ordinal>
	var stsList appsv1.StatefulSetList
	if err := k8sClient.List(context.TODO(), &stsList, client.InNamespace(o.Namespace)); err == nil {
		for i := range plan.PVCs {
			pvc := &plan.PVCs[i]
			for _, ss := range stsList.Items {
				for _, vct := range ss.Spec.VolumeClaimTemplates {
					pattern := regexp.MustCompile(
						fmt.Sprintf(`^%s-%s-(\d+)$`, regexp.QuoteMeta(vct.Name), regexp.QuoteMeta(ss.Name)))
					matches := pattern.FindStringSubmatch(pvc.Name)
					if matches != nil {
						newTemplateName := fmt.Sprintf("%s%s%s", vct.Name, migInfix, suffix)
						pvc.TargetName = fmt.Sprintf("%s-%s-%s", newTemplateName, ss.Name, matches[1])
						break
					}
				}
			}
		}
	}

	if err := WritePlan(plan, o.Output); err != nil {
		return err
	}

	// Print summary
	log.Printf("Plan written to %s", o.Output)
	log.Printf("Namespace: %s, PVCs: %d, Suffix: %s", o.Namespace, len(plan.PVCs), suffix)
	log.Println()
	if scListAvailable {
		log.Println("StorageClasses on cluster:")
		for _, sc := range scList.Items {
			marker := ""
			if sc.Annotations["storageclass.kubernetes.io/is-default-class"] == "true" {
				marker = " [default]"
			}
			log.Printf("  %-30s %s%s", sc.Name, sc.Provisioner, marker)
		}
	} else {
		log.Println("StorageClasses: (unavailable — set targetStorageClass manually in the plan)")
	}
	log.Println()
	log.Println("PVCs in plan:")
	for _, pvc := range plan.PVCs {
		target := pvc.TargetStorageClass
		if target == "" {
			target = "(needs manual selection)"
		}
		log.Printf("  %-30s %s -> %s", pvc.Name, pvc.SourceStorageClass, target)
	}
	log.Println()
	log.Println("Review and edit the plan file, then run:")
	log.Printf("  crane convert-storage --plan %s --endpoint <route|nginx-ingress>", o.Output)

	return nil
}

// suggestTargetSC picks a target StorageClass for a given source SC.
// Priority:
//  1. Another SC with the same provisioner (but different name)
//  2. GlusterFS/NFS -> Ceph mapping
//  3. Cluster default SC (if different from source)
//  4. Empty (user must pick manually)
func suggestTargetSC(sourceSC string, allSCs []storagev1.StorageClass, byProvisioner map[string][]string, defaultSC string) string {
	// Find source provisioner
	var sourceProvisioner string
	for _, sc := range allSCs {
		if sc.Name == sourceSC {
			sourceProvisioner = sc.Provisioner
			break
		}
	}

	// 1. Same provisioner, different name
	if sourceProvisioner != "" {
		for _, name := range byProvisioner[sourceProvisioner] {
			if name != sourceSC {
				return name
			}
		}
	}

	// 2. GlusterFS/NFS -> Ceph
	if isGlusterOrNFS(sourceProvisioner) {
		for provisioner, names := range byProvisioner {
			if isCeph(provisioner) && len(names) > 0 {
				return names[0]
			}
		}
	}

	// 3. Cluster default (if different)
	if defaultSC != "" && defaultSC != sourceSC {
		return defaultSC
	}

	// 4. No suggestion
	return ""
}

func isGlusterOrNFS(provisioner string) bool {
	p := strings.ToLower(provisioner)
	return strings.Contains(p, "gluster") ||
		strings.Contains(p, "nfs")
}

func isCeph(provisioner string) bool {
	p := strings.ToLower(provisioner)
	return strings.Contains(p, "ceph") ||
		strings.Contains(p, "rbd")
}

func parseLabelSelector(selector string) (map[string]string, error) {
	labels := make(map[string]string)
	pairs := strings.Split(selector, ",")
	for _, pair := range pairs {
		kv := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid label pair %q, expected key=value", pair)
		}
		labels[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
	}
	return labels, nil
}
