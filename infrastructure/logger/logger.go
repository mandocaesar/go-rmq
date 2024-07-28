package logger

import "context"

// SetStandardLog init catalystdk log globally for standard logging with fields
func SetStandardLog(logConfig *log.Config) {
	if err := log.SetStdLog(logConfig); err != nil {
		// when got error on setting config, it will use the default config.
		log.StdError(context.Background(), nil, err, "init log got error")
	}
}
