package transfer_pvc

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStorageConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     StorageConfig
		wantErr string
	}{
		{name: "valid aws", cfg: StorageConfig{Provider: "aws", Bucket: "b", Region: "us-east-1", AccessKey: "ak", SecretKey: "sk"}},
		{name: "valid minio", cfg: StorageConfig{Provider: "minio", Bucket: "b", Endpoint: "http://m:9000", AccessKey: "ak", SecretKey: "sk"}},
		{name: "valid noobaa", cfg: StorageConfig{Provider: "noobaa", Bucket: "b", Endpoint: "http://n:443", AccessKey: "ak", SecretKey: "sk"}},
		{name: "valid mcg", cfg: StorageConfig{Provider: "mcg", Bucket: "b", Endpoint: "http://m:443", AccessKey: "ak", SecretKey: "sk"}},
		{name: "valid ceph", cfg: StorageConfig{Provider: "ceph", Bucket: "b", Endpoint: "http://rgw:8080", AccessKey: "ak", SecretKey: "sk"}},
		{name: "valid gcp", cfg: StorageConfig{Provider: "gcp", Bucket: "b", ServiceAccountKey: "{}"}},
		{name: "valid azure", cfg: StorageConfig{Provider: "azure", Container: "c", Account: "acc", Key: "key"}},
		{name: "missing provider", cfg: StorageConfig{}, wantErr: "provider is required"},
		{name: "unsupported provider", cfg: StorageConfig{Provider: "dropbox"}, wantErr: "unsupported provider"},
		{name: "aws missing bucket", cfg: StorageConfig{Provider: "aws", Region: "us-east-1", AccessKey: "ak", SecretKey: "sk"}, wantErr: "bucket is required"},
		{name: "aws missing region", cfg: StorageConfig{Provider: "aws", Bucket: "b", AccessKey: "ak", SecretKey: "sk"}, wantErr: "region is required"},
		{name: "aws missing keys", cfg: StorageConfig{Provider: "aws", Bucket: "b", Region: "us-east-1"}, wantErr: "accessKey and secretKey are required"},
		{name: "minio missing endpoint", cfg: StorageConfig{Provider: "minio", Bucket: "b", AccessKey: "ak", SecretKey: "sk"}, wantErr: "endpoint is required"},
		{name: "gcp missing sa key", cfg: StorageConfig{Provider: "gcp", Bucket: "b"}, wantErr: "serviceAccountKey is required"},
		{name: "azure missing account", cfg: StorageConfig{Provider: "azure", Container: "c", Key: "k"}, wantErr: "account and key are required"},
		{name: "azure missing container", cfg: StorageConfig{Provider: "azure", Account: "a", Key: "k"}, wantErr: "container is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			} else {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error = %v, want containing %q", err, tt.wantErr)
				}
			}
		})
	}
}

func TestStorageConfigToRcloneEnvVars(t *testing.T) {
	tests := []struct {
		name     string
		cfg      StorageConfig
		wantType string
		wantProv string
	}{
		{
			name:     "aws",
			cfg:      StorageConfig{Provider: "aws", Region: "us-east-1", AccessKey: "AK", SecretKey: "SK"},
			wantType: "s3",
			wantProv: "AWS",
		},
		{
			name:     "minio",
			cfg:      StorageConfig{Provider: "minio", Endpoint: "http://m:9000", AccessKey: "AK", SecretKey: "SK"},
			wantType: "s3",
			wantProv: "Minio",
		},
		{
			name:     "gcp",
			cfg:      StorageConfig{Provider: "gcp"},
			wantType: "google cloud storage",
		},
		{
			name:     "azure",
			cfg:      StorageConfig{Provider: "azure", Account: "acc", Key: "key"},
			wantType: "azureblob",
		},
		{
			name:     "ceph",
			cfg:      StorageConfig{Provider: "ceph", Endpoint: "http://rgw:8080", AccessKey: "AK", SecretKey: "SK"},
			wantType: "s3",
			wantProv: "Ceph",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			envs := tt.cfg.ToRcloneEnvVars()
			envMap := make(map[string]string)
			for _, e := range envs {
				envMap[e.Name] = e.Value
			}

			if got := envMap["RCLONE_CONFIG_REMOTE_TYPE"]; got != tt.wantType {
				t.Errorf("TYPE = %q, want %q", got, tt.wantType)
			}
			if tt.wantProv != "" {
				if got := envMap["RCLONE_CONFIG_REMOTE_PROVIDER"]; got != tt.wantProv {
					t.Errorf("PROVIDER = %q, want %q", got, tt.wantProv)
				}
			}
		})
	}
}

func TestStorageConfigRemotePath(t *testing.T) {
	tests := []struct {
		name     string
		cfg      StorageConfig
		pvcName  string
		expected string
	}{
		{
			name:     "simple",
			cfg:      StorageConfig{Bucket: "mybucket"},
			pvcName:  "mysql-data",
			expected: "remote:mybucket/mysql-data/",
		},
		{
			name:     "with prefix",
			cfg:      StorageConfig{Bucket: "mybucket", Prefix: "migrations/app1"},
			pvcName:  "mysql-data",
			expected: "remote:mybucket/migrations/app1/mysql-data/",
		},
		{
			name:     "prefix with trailing slash",
			cfg:      StorageConfig{Bucket: "mybucket", Prefix: "mig/"},
			pvcName:  "pvc1",
			expected: "remote:mybucket/mig/pvc1/",
		},
		{
			name:     "azure container",
			cfg:      StorageConfig{Container: "mycontainer"},
			pvcName:  "data",
			expected: "remote:mycontainer/data/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.RemotePath(tt.pvcName)
			if got != tt.expected {
				t.Errorf("RemotePath(%q) = %q, want %q", tt.pvcName, got, tt.expected)
			}
		})
	}
}

func TestStorageConfigRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "storage.yaml")

	// Write template
	if err := WriteStorageTemplate("aws", path); err != nil {
		t.Fatalf("WriteStorageTemplate() error: %v", err)
	}

	// Template has placeholder values — structurally valid but not real credentials
	tmplCfg, err := LoadStorageConfig(path)
	if err != nil {
		t.Fatalf("LoadStorageConfig() template error: %v", err)
	}
	if tmplCfg.Provider != "aws" {
		t.Errorf("template Provider = %q, want %q", tmplCfg.Provider, "aws")
	}

	// Write config with real-looking values
	validYaml := `provider: aws
bucket: test-bucket
region: us-east-1
accessKey: AKIATEST
secretKey: secret123
`
	if err := os.WriteFile(path, []byte(validYaml), 0600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadStorageConfig(path)
	if err != nil {
		t.Fatalf("LoadStorageConfig() error: %v", err)
	}
	if cfg.Provider != "aws" {
		t.Errorf("Provider = %q, want %q", cfg.Provider, "aws")
	}
	if cfg.Bucket != "test-bucket" {
		t.Errorf("Bucket = %q, want %q", cfg.Bucket, "test-bucket")
	}
}

func TestWriteStorageTemplate_AllProviders(t *testing.T) {
	providers := []string{"aws", "minio", "noobaa", "mcg", "ceph", "gcp", "azure"}
	dir := t.TempDir()
	for _, p := range providers {
		t.Run(p, func(t *testing.T) {
			path := filepath.Join(dir, p+".yaml")
			if err := WriteStorageTemplate(p, path); err != nil {
				t.Fatalf("WriteStorageTemplate(%q) error: %v", p, err)
			}
			data, _ := os.ReadFile(path)
			if len(data) == 0 {
				t.Error("template is empty")
			}
			if !strings.Contains(string(data), "provider: "+p) {
				t.Errorf("template missing 'provider: %s'", p)
			}
		})
	}
}

func TestWriteStorageTemplate_UnsupportedProvider(t *testing.T) {
	err := WriteStorageTemplate("dropbox", "/tmp/nope.yaml")
	if err == nil || !strings.Contains(err.Error(), "unsupported provider") {
		t.Errorf("error = %v, want 'unsupported provider'", err)
	}
}
