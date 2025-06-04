package config

import (
	"os"
	"path"
	"runtime"

	"gopkg.in/yaml.v3"
)

var (
	RootPath  string
	ConfigMap map[string]any
)

// 获取项目的根路径
func init() {
	RootPath = path.Dir(GetCurrentPath())

	initConf := RootPath + "/config/init.yml"
	yamlFile, err := os.ReadFile(initConf)
	if err != nil {
		panic(err)
	}

	ConfigMap = make(map[string]any)
	err = yaml.Unmarshal(yamlFile, &ConfigMap)
	if err != nil {
		panic(err)
	}
}

func GetCurrentPath() string {
	_, filename, _, _ := runtime.Caller(1)
	return path.Dir(filename)
}
