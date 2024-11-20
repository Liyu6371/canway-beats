package config

type Task map[string]interface{}

func (t Task) ToMap() map[string]interface{} {
	return map[string]interface{}(t)
}
