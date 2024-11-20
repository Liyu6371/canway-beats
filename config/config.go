package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type PeriodTask map[string]interface{}

func (p PeriodTask) ToMap() map[string]interface{} {
	return map[string]interface{}(p)
}

func (p PeriodTask) TaskNames() []string {
	names := []string{}
	for k := range p {
		names = append(names, k)
	}
	return names
}

func (p PeriodTask) GetConfByName(name string) interface{} {
	if v, ok := p[name]; ok {
		return v
	}
	return nil
}

// NonPeriodTask
type NonPeriodTask map[string]interface{}

func (n NonPeriodTask) ToMap() map[string]interface{} {
	return map[string]interface{}(n)
}

func (n NonPeriodTask) TaskNames() []string {
	names := []string{}
	for k := range n {
		names = append(names, k)
	}
	return names
}

func (n NonPeriodTask) GetConfByName(name string) interface{} {
	if v, ok := n[name]; ok {
		return v
	}
	return nil
}

var g *Config

type Config struct {
	TestModel      bool                   `json:"test_model"`
	Logger         Logger                 `json:"logger"`
	Sender         map[string]interface{} `json:"sender"`
	Source         map[string]interface{} `json:"source"`
	PeriodTasks    PeriodTask             `json:"period_tasks"`
	NonPeriodTasks NonPeriodTask          `json:"non_period_tasks"`
}

type Logger struct {
	Level string `json:"level"`
	Path  string `json:"path"`
}

func InitConfig(p string) error {
	if p == "" {
		return errors.New("config path shouldn't be empty!")
	}
	dir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("unable to get wd, error: %s", err)
	}
	cPath := filepath.Join(dir, p)
	content, err := os.ReadFile(cPath)
	if err != nil {
		return fmt.Errorf("cannot read config file: %s, error: %v", cPath, err)
	}
	if err = yaml.Unmarshal(content, &g); err != nil {
		return fmt.Errorf("yaml unmarshal error: %v", err)
	}
	return nil
}

func GetConf() *Config {
	return g
}

func GetPeriodConf() PeriodTask {
	return g.PeriodTasks
}

func GetNonPeriodConf() NonPeriodTask {
	return g.NonPeriodTasks
}
