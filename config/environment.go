package config

import (
	"os"
	"strings"
)

const (
	appEnvVar              = "APP_ENV"
	environmentDevelopment = "development"
	environmentProduction  = "production"
	environmentStaging     = "staging"
)

// getAppEnvironment reads the application environment from APP_ENV and
// defaults to development when no value is provided.
func getAppEnvironment() string {
	env := strings.ToLower(strings.TrimSpace(os.Getenv(appEnvVar)))
	if env == "" {
		return environmentDevelopment
	}
	return env
}

// resolveEnvSpecificPath selects an environment specific configuration file
// when one is available for the current environment.
func resolveEnvSpecificPath(path, defaultPath string, envPaths map[string]string) string {
	if path == "" {
		path = defaultPath
	}

	env := getAppEnvironment()
	if envPath, ok := envPaths[env]; ok {
		if path == defaultPath || path == envPath {
			return envPath
		}
	}

	return path
}
