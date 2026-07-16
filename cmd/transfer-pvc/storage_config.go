package transfer_pvc

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type StorageConfig struct {
	Provider          string `yaml:"provider"`
	Bucket            string `yaml:"bucket,omitempty"`
	Container         string `yaml:"container,omitempty"`
	Region            string `yaml:"region,omitempty"`
	Endpoint          string `yaml:"endpoint,omitempty"`
	AccessKey         string `yaml:"accessKey,omitempty"`
	SecretKey         string `yaml:"secretKey,omitempty"`
	Account           string `yaml:"account,omitempty"`
	Key               string `yaml:"key,omitempty"`
	ServiceAccountKey string `yaml:"serviceAccountKey,omitempty"`
	Prefix            string `yaml:"prefix,omitempty"`
}

func LoadStorageConfig(path string) (*StorageConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read storage config %q: %w", path, err)
	}
	cfg := &StorageConfig{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse storage config %q: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid storage config %q: %w", path, err)
	}
	return cfg, nil
}

func (c *StorageConfig) Validate() error {
	if c.Provider == "" {
		return fmt.Errorf("provider is required")
	}
	switch strings.ToLower(c.Provider) {
	case "aws":
		if c.Bucket == "" {
			return fmt.Errorf("bucket is required for provider %q", c.Provider)
		}
		if c.AccessKey == "" || c.SecretKey == "" {
			return fmt.Errorf("accessKey and secretKey are required for provider %q", c.Provider)
		}
		if c.Region == "" {
			return fmt.Errorf("region is required for provider %q", c.Provider)
		}
	case "minio", "noobaa", "mcg", "ceph":
		if c.Bucket == "" {
			return fmt.Errorf("bucket is required for provider %q", c.Provider)
		}
		if c.Endpoint == "" {
			return fmt.Errorf("endpoint is required for provider %q", c.Provider)
		}
		if c.AccessKey == "" || c.SecretKey == "" {
			return fmt.Errorf("accessKey and secretKey are required for provider %q", c.Provider)
		}
	case "gcp":
		if c.Bucket == "" {
			return fmt.Errorf("bucket is required for provider %q", c.Provider)
		}
		if c.ServiceAccountKey == "" {
			return fmt.Errorf("serviceAccountKey is required for provider %q", c.Provider)
		}
	case "azure":
		if c.Container == "" {
			return fmt.Errorf("container is required for provider %q", c.Provider)
		}
		if c.Account == "" || c.Key == "" {
			return fmt.Errorf("account and key are required for provider %q", c.Provider)
		}
	default:
		return fmt.Errorf("unsupported provider %q (supported: aws, minio, noobaa, mcg, ceph, gcp, azure)", c.Provider)
	}
	return nil
}

// BucketOrContainer returns the bucket name (or container for Azure).
func (c *StorageConfig) BucketOrContainer() string {
	if c.Container != "" {
		return c.Container
	}
	return c.Bucket
}

// RemotePath returns the rclone remote path for a given PVC name.
func (c *StorageConfig) RemotePath(pvcName string) string {
	prefix := c.Prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return fmt.Sprintf("remote:%s/%s%s/", c.BucketOrContainer(), prefix, pvcName)
}

// ToRcloneEnvVars converts the storage config to rclone environment variables.
func (c *StorageConfig) ToRcloneEnvVars() []corev1.EnvVar {
	var envs []corev1.EnvVar
	add := func(key, value string) {
		if value != "" {
			envs = append(envs, corev1.EnvVar{
				Name:  "RCLONE_CONFIG_REMOTE_" + key,
				Value: value,
			})
		}
	}

	switch strings.ToLower(c.Provider) {
	case "aws":
		add("TYPE", "s3")
		add("PROVIDER", "AWS")
		add("REGION", c.Region)
		add("ACCESS_KEY_ID", c.AccessKey)
		add("SECRET_ACCESS_KEY", c.SecretKey)
	case "minio":
		add("TYPE", "s3")
		add("PROVIDER", "Minio")
		add("ENDPOINT", c.Endpoint)
		add("ACCESS_KEY_ID", c.AccessKey)
		add("SECRET_ACCESS_KEY", c.SecretKey)
	case "noobaa", "mcg":
		add("TYPE", "s3")
		add("PROVIDER", "Other")
		add("ENDPOINT", c.Endpoint)
		add("ACCESS_KEY_ID", c.AccessKey)
		add("SECRET_ACCESS_KEY", c.SecretKey)
	case "ceph":
		add("TYPE", "s3")
		add("PROVIDER", "Ceph")
		add("ENDPOINT", c.Endpoint)
		add("ACCESS_KEY_ID", c.AccessKey)
		add("SECRET_ACCESS_KEY", c.SecretKey)
	case "gcp":
		add("TYPE", "google cloud storage")
	case "azure":
		add("TYPE", "azureblob")
		add("ACCOUNT", c.Account)
		add("KEY", c.Key)
	}
	return envs
}

// ToCredentialsSecret creates a temporary K8s Secret containing the storage
// credentials. For GCP, the service account key is stored as a file.
func (c *StorageConfig) ToCredentialsSecret(name, namespace string) *corev1.Secret {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "crane",
				"app.kubernetes.io/component": "indirect-transfer",
			},
		},
		Data: make(map[string][]byte),
	}

	if c.ServiceAccountKey != "" {
		secret.Data["sa-key.json"] = []byte(c.ServiceAccountKey)
	}

	return secret
}

var storageTemplates = map[string]string{
	"aws": `provider: aws
bucket: <your-bucket-name>
region: <aws-region>
accessKey: <your-access-key-id>
secretKey: <your-secret-access-key>
`,
	"minio": `provider: minio
bucket: <your-bucket-name>
endpoint: <http://minio:9000>
accessKey: <your-access-key>
secretKey: <your-secret-key>
`,
	"noobaa": `provider: noobaa
bucket: <your-bucket-name>
endpoint: <https://s3.noobaa.svc:443>
accessKey: <your-access-key>
secretKey: <your-secret-key>
`,
	"mcg": `provider: mcg
bucket: <your-bucket-name>
endpoint: <https://s3.openshift-storage.svc:443>
accessKey: <your-access-key>
secretKey: <your-secret-key>
`,
	"ceph": `provider: ceph
bucket: <your-bucket-name>
endpoint: <http://rgw:8080>
accessKey: <your-access-key>
secretKey: <your-secret-key>
`,
	"gcp": `provider: gcp
bucket: <your-bucket-name>
serviceAccountKey: |
  <paste your service account JSON key here>
`,
	"azure": `provider: azure
container: <your-container-name>
account: <storage-account-name>
key: <storage-account-key>
`,
}

func WriteStorageTemplate(provider, outputPath string) error {
	tmpl, ok := storageTemplates[strings.ToLower(provider)]
	if !ok {
		providers := make([]string, 0, len(storageTemplates))
		for k := range storageTemplates {
			providers = append(providers, k)
		}
		return fmt.Errorf("unsupported provider %q (supported: %s)", provider, strings.Join(providers, ", "))
	}
	if err := os.WriteFile(outputPath, []byte(tmpl), 0600); err != nil {
		return fmt.Errorf("failed to write template to %q: %w", outputPath, err)
	}
	return nil
}
