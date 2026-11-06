package transfer

import "testing"

func TestSanitizeFileNamePathTraversal(t *testing.T) {
	for input, expected := range map[string]string{"../../evil.exe": "evil.exe", `C:\Windows\system32\cmd.exe`: "cmd.exe", "folder/sub/doc.pdf": "doc.pdf"} {
		if got := SanitizeFileName(input); got != expected {
			t.Fatalf("%q -> %q, want %q", input, got, expected)
		}
	}
}
