package config

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestConfig(t *testing.T) {
	fmt.Println(RootPath)
	configBytes, err := json.Marshal(ConfigMap)
	if err != nil {
		t.Fatalf("Error marshalling config: %v", err)
	}
	fmt.Println(string(configBytes))
}
