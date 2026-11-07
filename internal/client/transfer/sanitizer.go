package transfer

import (
	"path/filepath"
	"strings"
)

// CleanInputPath normalizes a path entered through a command-line prompt.
// It removes PowerShell's call operator and repeatedly unwraps surrounding
// quotes so drag-and-drop paths can be passed directly to os.Stat and Open.
func CleanInputPath(input string) string {
	cleaned := strings.TrimSpace(input)
	if cleaned == "" {
		return ""
	}

	for {
		previous := cleaned
		cleaned = strings.Trim(cleaned, "\"' \t\r\n")
		if strings.HasPrefix(cleaned, "&") {
			cleaned = strings.TrimSpace(strings.TrimPrefix(cleaned, "&"))
		}
		if cleaned == previous {
			return cleaned
		}
	}
}

// SanitizeFileName reduces an untrusted path to a safe local base name.
func SanitizeFileName(input string) string {
	input = strings.ReplaceAll(input, "\\", "/")
	base := strings.TrimSpace(filepath.Base(filepath.Clean(input)))
	if base == "" || base == "." || base == "/" || base == "\\" {
		return "unnamed_file.bin"
	}
	return base
}
