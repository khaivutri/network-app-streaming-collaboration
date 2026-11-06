package transfer

import (
	"path/filepath"
	"strings"
)

// SanitizeFileName reduces an untrusted path to a safe local base name.
func SanitizeFileName(input string) string {
	input = strings.ReplaceAll(input, "\\", "/")
	base := strings.TrimSpace(filepath.Base(filepath.Clean(input)))
	if base == "" || base == "." || base == "/" || base == "\\" {
		return "unnamed_file.bin"
	}
	return base
}
