package config

type CKManServerConfig struct {
	CertFile  string `yaml:"certfile"`
	KeyFile   string `yaml:"keyfile"`
	PublicKey string `yaml:"public_key"`
}
