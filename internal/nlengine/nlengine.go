package nlengine

import (
	"regexp"
	"strconv"
	"strings"

	"ai-container-go/internal/models"
)

type patternEntry struct {
	re     *regexp.Regexp
	action models.IntentAction
	groups []string
}

var patterns = []patternEntry{
	// Scale
	{regexp.MustCompile(`(?i)(?:스케일|스케일링|늘려|줄여).*?(\w+).*?(\d+)\s*개`), models.IntentScale, []string{"service", "replicas"}},
	{regexp.MustCompile(`(?i)(\w+)\s*(?:를|을)\s*(\d+)\s*개(?:로)?\s*(?:스케일|실행)`), models.IntentScale, []string{"service", "replicas"}},
	{regexp.MustCompile(`(?i)(?:scale|replicate)\s+(\w+)\s+(?:to|up to)\s*(\d+)`), models.IntentScale, []string{"service", "replicas"}},
	{regexp.MustCompile(`(?i)(\w+)\s*(\d+)\s*개\s*(?:로)?\s*스케일`), models.IntentScale, []string{"service", "replicas"}},
	// Migrate
	{regexp.MustCompile(`(?i)(\w[\w-]*)\s*(?:를|을)\s*(\w[\w-]*)\s*(?:로|으로)\s*(?:마이그레이션|이동|옮겨|이전)`), models.IntentMigrate, []string{"service", "target_node"}},
	{regexp.MustCompile(`(?i)(?:마이그레이션|이동|옮겨)\s+(\w[\w-]*)\s+(\w[\w-]*)`), models.IntentMigrate, []string{"service", "target_node"}},
	{regexp.MustCompile(`(?i)(?:migrate|move)\s+(\w[\w-]*)\s+(?:to|from\s+\w+\s+to)\s+(\w[\w-]*)`), models.IntentMigrate, []string{"service", "target_node"}},
	// Drain
	{regexp.MustCompile(`(?i)(\w[\w-]*)\s*(?:를|을)?\s*(?:드레인|비워|배출)`), models.IntentDrain, []string{"target_node"}},
	{regexp.MustCompile(`(?i)(?:drain|evacuate)\s+(\w[\w-]*)`), models.IntentDrain, []string{"target_node"}},
	// Deploy
	{regexp.MustCompile(`(?i)(\w+)\s*배포.*?이미지\s+(\S+)`), models.IntentDeploy, []string{"name", "image"}},
	{regexp.MustCompile(`(?i)(\w+)\s*(?:서비스\s*)?(?:를\s*)?배포(?:\s*해줘)?`), models.IntentDeploy, []string{"name"}},
	{regexp.MustCompile(`(?i)deploy\s+(\w+)(?:\s+(\S+))?`), models.IntentDeploy, []string{"name", "image"}},
	// Resource
	{regexp.MustCompile(`(?i)([a-zA-Z0-9_-]+).*?메모리\s*(\d+)\s*(m|mb|g|gb)?`), models.IntentResource, []string{"service", "memory"}},
	{regexp.MustCompile(`(?i)([a-zA-Z0-9_-]+)\s*에\s*메모리\s*(\d+)\s*(m|mb|g|gb)?`), models.IntentResource, []string{"service", "memory"}},
	{regexp.MustCompile(`(?i)(?:memory|mem)\s+(?:of\s+)?(\w+)\s+(?:to\s+)?(\d+)\s*(m|mb|g|gb)?`), models.IntentResource, []string{"service", "memory"}},
	{regexp.MustCompile(`(?i)(\w+).*?cpu\s*(\d*\.?\d+)`), models.IntentResource, []string{"service", "cpu"}},
	// Stop
	{regexp.MustCompile(`(?i)([a-zA-Z0-9_-]+)\s*컨테이너\s*(?:종료|중지)(?:\s*해줘)?`), models.IntentStop, []string{"service"}},
	{regexp.MustCompile(`(?i)(\w+)\s*(?:를|을)?\s*중지`), models.IntentStop, []string{"service"}},
	{regexp.MustCompile(`(?i)stop\s+(\w+)`), models.IntentStop, []string{"service"}},
	// Cluster status
	{regexp.MustCompile(`(?i)클러스터\s*(?:상태|상황|현황|정보)|cluster\s*(?:status|info)`), models.IntentClusterStatus, []string{}},
	// Node list
	{regexp.MustCompile(`(?i)노드\s*(?:목록|리스트|상태)|node[s]?\s*(?:list|status)`), models.IntentNodeList, []string{}},
	// List
	{regexp.MustCompile(`(?i)목록|리스트|서비스\s*목록|list\s*services?`), models.IntentList, []string{}},
}

// normMemory normalizes memory value and unit into a short form.
// e.g. "512" + "mb" -> "512m", "1" + "gb" -> "1g", "256" + "" -> "256m"
func normMemory(value, unit string) string {
	unit = strings.ToLower(strings.TrimSpace(unit))
	switch unit {
	case "g", "gb":
		return value + "g"
	case "m", "mb":
		return value + "m"
	default:
		return value + "m"
	}
}

// Parse parses a natural language command (Korean or English) and returns
// a ParsedIntent describing the detected action and parameters.
func Parse(command string) models.ParsedIntent {
	intent := models.ParsedIntent{
		Action: models.IntentUnknown,
		Raw:    command,
	}

	trimmed := strings.TrimSpace(command)

	for _, p := range patterns {
		match := p.re.FindStringSubmatch(trimmed)
		if match == nil {
			continue
		}

		intent.Action = p.action

		for i, groupName := range p.groups {
			captureIdx := i + 1
			if captureIdx >= len(match) {
				continue
			}
			val := match[captureIdx]
			if val == "" {
				continue
			}

			switch groupName {
			case "service":
				intent.ServiceName = val
			case "replicas":
				if n, err := strconv.Atoi(val); err == nil {
					intent.Replicas = &n
				}
			case "name":
				intent.ServiceName = val
			case "image":
				intent.Image = val
			case "memory":
				// For resource action, the memory value is this group
				// and the unit is the next capture group
				unit := ""
				if captureIdx+1 < len(match) {
					unit = match[captureIdx+1]
				}
				intent.Memory = normMemory(val, unit)
			case "cpu":
				intent.CPU = val
			case "target_node":
				intent.TargetNode = val
			}
		}

		return intent
	}

	// Fallback: single-word handling
	lower := strings.ToLower(trimmed)
	if lower == "list" || lower == "목록" || lower == "리스트" {
		intent.Action = models.IntentList
		return intent
	}

	// Single word -> treat as deploy with that word as service name
	if len(strings.Fields(trimmed)) == 1 && trimmed != "" {
		intent.Action = models.IntentDeploy
		intent.ServiceName = trimmed
		return intent
	}

	return intent
}
