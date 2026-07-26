package agentdist

import (
	"fmt"
	"os"
	"regexp"
	"strings"
)

const (
	DefaultRepository  = "cazi-cc/komari-agent"
	DefaultScriptRef   = "main"
	DefaultDockerImage = "ghcr.io/cazi-cc/komari-agent:snapshot"
)

var (
	repositoryPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)
	refPattern        = regexp.MustCompile(`^[A-Za-z0-9._/-]+$`)
)

// Config is the backend-owned source of truth used by the admin UI to install
// the agent build that matches this Komari fork.
type Config struct {
	Repository       string   `json:"repository"`
	ScriptRef        string   `json:"script_ref"`
	LinuxScriptURL   string   `json:"linux_script_url"`
	WindowsScriptURL string   `json:"windows_script_url"`
	DockerImage      string   `json:"docker_image"`
	RequiredArgs     []string `json:"required_args"`
}

func Current() Config {
	repository := envOrDefault("KOMARI_AGENT_REPOSITORY", DefaultRepository)
	if !repositoryPattern.MatchString(repository) {
		repository = DefaultRepository
	}

	ref := envOrDefault("KOMARI_AGENT_SCRIPT_REF", DefaultScriptRef)
	if !refPattern.MatchString(ref) || strings.Contains(ref, "..") {
		ref = DefaultScriptRef
	}

	dockerImage := envOrDefault("KOMARI_AGENT_DOCKER_IMAGE", DefaultDockerImage)
	if strings.ContainsAny(dockerImage, " \t\r\n") {
		dockerImage = DefaultDockerImage
	}

	rawBase := fmt.Sprintf("https://raw.githubusercontent.com/%s/refs/heads/%s", repository, ref)
	return Config{
		Repository:       repository,
		ScriptRef:        ref,
		LinuxScriptURL:   rawBase + "/install.sh",
		WindowsScriptURL: rawBase + "/install.ps1",
		DockerImage:      dockerImage,
		RequiredArgs: []string{
			"--disable-web-ssh",
			"--interval", "5",
			"--info-report-interval", "15",
		},
	}
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
