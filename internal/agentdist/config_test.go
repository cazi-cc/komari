package agentdist

import (
	"strings"
	"testing"
)

func TestCurrentUsesForkDistribution(t *testing.T) {
	t.Setenv("KOMARI_AGENT_REPOSITORY", "")
	t.Setenv("KOMARI_AGENT_SCRIPT_REF", "")
	t.Setenv("KOMARI_AGENT_DOCKER_IMAGE", "")

	got := Current()
	if got.Repository != DefaultRepository {
		t.Fatalf("repository = %q, want %q", got.Repository, DefaultRepository)
	}
	if !strings.Contains(got.LinuxScriptURL, "cazi-cc/komari-agent") ||
		!strings.Contains(got.WindowsScriptURL, "cazi-cc/komari-agent") {
		t.Fatalf("script URLs do not point to the fork: %#v", got)
	}
	if got.DockerImage != DefaultDockerImage {
		t.Fatalf("docker image = %q, want %q", got.DockerImage, DefaultDockerImage)
	}
	if strings.Contains(got.LinuxScriptURL, "komari-monitor/komari-agent") {
		t.Fatalf("official agent repository leaked into distribution: %s", got.LinuxScriptURL)
	}
}

func TestCurrentRejectsInvalidOverrides(t *testing.T) {
	t.Setenv("KOMARI_AGENT_REPOSITORY", "https://example.invalid/repo")
	t.Setenv("KOMARI_AGENT_SCRIPT_REF", "../../main")
	t.Setenv("KOMARI_AGENT_DOCKER_IMAGE", "bad image")

	got := Current()
	if got.Repository != DefaultRepository ||
		got.ScriptRef != DefaultScriptRef ||
		got.DockerImage != DefaultDockerImage {
		t.Fatalf("invalid overrides were not rejected: %#v", got)
	}
}

func TestCurrentAcceptsControlledOverrides(t *testing.T) {
	t.Setenv("KOMARI_AGENT_REPOSITORY", "owner/agent")
	t.Setenv("KOMARI_AGENT_SCRIPT_REF", "release/test")
	t.Setenv("KOMARI_AGENT_DOCKER_IMAGE", "registry.example/agent:test")

	got := Current()
	if got.Repository != "owner/agent" ||
		got.ScriptRef != "release/test" ||
		got.DockerImage != "registry.example/agent:test" {
		t.Fatalf("controlled overrides were not applied: %#v", got)
	}
}
