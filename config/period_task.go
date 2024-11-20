package config

type PeriodTask map[string]Task

func (p PeriodTask) TaskNames() []string {
	names := []string{}
	for k := range p {
		names = append(names, k)
	}
	return names
}

func (p PeriodTask) GetTaskByName(name string) Task {
	if v, ok := p[name]; ok {
		return v
	}
	return nil
}
