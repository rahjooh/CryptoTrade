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

const (
	// EnvironmentDevelopment exposes the canonical development environment
	// identifier. It can be used by callers outside the config package when
	// environment specific behaviour is required.
	EnvironmentDevelopment = environmentDevelopment
	// EnvironmentProduction exposes the canonical production environment
	// identifier.
	EnvironmentProduction = environmentProduction
	// EnvironmentStaging exposes the canonical staging environment
	// identifier.
	EnvironmentStaging = environmentStaging
)

var environmentAliases = map[string]string{
	"prod":        environmentProduction,
	"producation": environmentProduction,
	"stag":        environmentStaging,
	"stagging":    environmentStaging,
}

// getAppEnvironment reads the application environment from APP_ENV and
// defaults to development when no value is provided.
func getAppEnvironment() string {
	env := strings.ToLower(strings.TrimSpace(os.Getenv(appEnvVar)))
	if env == "" {
		return environmentDevelopment
	}
	if canonical, ok := environmentAliases[env]; ok {
		return canonical
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

// AppEnvironment exposes the current application environment as configured
// through the APP_ENV environment variable. The value is normalised using the
// same alias rules that resolve environment specific files so callers can rely
// on a consistent identifier.
func AppEnvironment() string {
	return getAppEnvironment()
}

// IsProductionLike reports whether the provided environment should behave like
// a production deployment. Production-like environments (production and
// staging) are typically stricter about configuration errors such as missing IP
// shard assignments.
func IsProductionLike(env string) bool {
	switch env {
	case environmentProduction, environmentStaging:
		return true
	default:
		return false
	}
}
