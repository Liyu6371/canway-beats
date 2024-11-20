package config

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
