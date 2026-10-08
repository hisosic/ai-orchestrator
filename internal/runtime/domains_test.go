package runtime

import (
	"strings"
	"testing"
)

func TestMultipleDomains(t *testing.T) {
	t.Setenv("ORCHESTRATOR_BASE_DOMAIN", "24x365.online")
	t.Setenv("ORCHESTRATOR_PATH_HOST", "")
	t.Setenv("ORCHESTRATOR_EXTRA_BASE_DOMAINS", " rgrg.im, 24x365.online ,")

	if got := strings.Join(PathHosts(), ","); got != "itda.24x365.online,itda.rgrg.im" {
		t.Fatalf("PathHosts = %s", got)
	}
	for host, want := range map[string]string{
		"itda.rgrg.im":       "itda.rgrg.im",
		"ITDA.RGRG.IM:443":   "itda.rgrg.im",
		"web.rgrg.im":        "itda.rgrg.im",
		"itda.24x365.online": "itda.24x365.online",
		"20.20.6.248:8000":   "itda.24x365.online",
	} {
		if _, ph := DomainForHost(host); ph != want {
			t.Errorf("DomainForHost(%s) = %s, want %s", host, ph, want)
		}
	}

	path := TraefikLabels("api", 80, false)["traefik.http.routers.api-host.rule"]
	if path != "(Host(`itda.24x365.online`) || Host(`itda.rgrg.im`)) && PathPrefix(`/api/`)" {
		t.Errorf("path rule = %s", path)
	}
	sub := TraefikLabels("api", 80, true)["traefik.http.routers.api.rule"]
	if sub != "(Host(`api.24x365.online`) || Host(`api.rgrg.im`))" {
		t.Errorf("subdomain rule = %s", sub)
	}

	// Without extra domains the rules are unchanged from before.
	t.Setenv("ORCHESTRATOR_EXTRA_BASE_DOMAINS", "")
	if r := TraefikLabels("api", 80, true)["traefik.http.routers.api.rule"]; r != "Host(`api.24x365.online`)" {
		t.Errorf("single-domain rule = %s", r)
	}
}
