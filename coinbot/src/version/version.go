package version

import (
	_ "embed" // for importing version
	"runtime/debug"
)

var (
	Commit = "unknown"
	Version = "unknown"
	BuildTimestamp = "unknown"
)

func GetBuildInfo() map[string]string {
	data := make(map[string]string, 0)

	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			data[s.Key] = s.Value
		}
	}

	data["commit"] = Commit
	data["version"] = Version
	data["build_timestamp"] = BuildTimestamp

	return data
}
