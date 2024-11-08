package config

import "github.com/mitchellh/mapstructure"

type MapConf map[string]interface{}

func (m MapConf) To(v any) (any, error) {
	if err := mapstructure.Decode(m, v); err != nil {
		return nil, err
	}
	return v, nil
}
