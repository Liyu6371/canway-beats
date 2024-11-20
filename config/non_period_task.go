package config

// NonPeriodTask
type NonPeriodTask map[string]Task

func (n NonPeriodTask) TaskNames() []string {
	names := []string{}
	for k := range n {
		names = append(names, k)
	}
	return names
}

func (n NonPeriodTask) GetTaskConfByName(name string) Task {
	if v, ok := n[name]; ok {
		return v
	}
	return nil
}
