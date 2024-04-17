package utils

type DeviceConfig struct {
	ID       int    `json:"id"`
	DeviceID int    `json:"device_id"`
	Data     string `json:"data"`
	Image    string `json:"image"`
	Control  string `json:"control"`
	Version  string `json:"version"`
}

type DataConfig struct {
	ID     int    `json:"id"`
	Port   string `json:"port"`
	Params struct {
		Command  string `json:"command"`
		Contents []struct {
			Key  string `json:"key"`
			Info struct {
				Name string `json:"name"`
				Unit string `json:"unit"`
			} `json:"info"`
		} `json:"contents"`
	} `json:"params"`
}
