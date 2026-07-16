package transfer_pvc

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
)

type setupStorageOptions struct {
	Provider string
	Output   string
}

func NewSetupStorageCommand() *cobra.Command {
	o := &setupStorageOptions{}
	cmd := &cobra.Command{
		Use:   "setup-storage",
		Short: "Generate a storage config template for indirect PVC transfer",
		Long: `Generates a YAML template for configuring object storage used in indirect PVC transfer.
Fill in the template with your credentials and bucket details, then pass it to
'crane transfer-pvc --indirect --storage-config <file>'.

Supported providers: aws, minio, noobaa, mcg, ceph, gcp, azure`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if o.Provider == "" {
				return fmt.Errorf("--provider is required")
			}
			if o.Output == "" {
				return fmt.Errorf("--output is required")
			}
			if err := WriteStorageTemplate(o.Provider, o.Output); err != nil {
				return err
			}
			log.Printf("Storage config template written to %s", o.Output)
			log.Printf("Edit the file with your credentials, then run:")
			log.Printf("  crane transfer-pvc --indirect --storage-config %s \\", o.Output)
			log.Printf("    --source-context <src> --destination-context <tgt> \\")
			log.Printf("    --pvc-name <name> --pvc-namespace <ns>")
			return nil
		},
	}
	cmd.Flags().StringVar(&o.Provider, "provider", "", "Storage provider (aws, minio, noobaa, mcg, ceph, gcp, azure)")
	cmd.Flags().StringVar(&o.Output, "output", "", "Output YAML file path")
	cmd.MarkFlagRequired("provider")
	cmd.MarkFlagRequired("output")
	return cmd
}
