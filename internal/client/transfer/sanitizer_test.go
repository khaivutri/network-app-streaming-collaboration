package transfer

import "testing"

func TestSanitizeFileNamePathTraversal(t *testing.T) {
	for input, expected := range map[string]string{"../../evil.exe": "evil.exe", `C:\Windows\system32\cmd.exe`: "cmd.exe", "folder/sub/doc.pdf": "doc.pdf"} {
		if got := SanitizeFileName(input); got != expected {
			t.Fatalf("%q -> %q, want %q", input, got, expected)
		}
	}
}

func TestCleanInputPath_WindowsPowerShellDragDrop(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "powershell call operator with single quotes",
			input:    "& 'c:\\Users\\kelve\\Downloads\\test.pdf'",
			expected: "c:\\Users\\kelve\\Downloads\\test.pdf",
		},
		{
			name:     "wrapped in double quotes with powershell call operator",
			input:    "\" & 'c:\\Users\\kelve\\Downloads\\GIAO TRINH.pdf' \"",
			expected: "c:\\Users\\kelve\\Downloads\\GIAO TRINH.pdf",
		},
		{
			name:     "single quotes with space",
			input:    " 'D:\\Data\\My Document.docx' ",
			expected: "D:\\Data\\My Document.docx",
		},
		{
			name:     "double quotes simple",
			input:    "\"C:\\Program Files\\App\\file.bin\"",
			expected: "C:\\Program Files\\App\\file.bin",
		},
		{
			name:     "bare path with leading whitespace",
			input:    "   C:\\plain_path.txt   ",
			expected: "C:\\plain_path.txt",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CleanInputPath(tc.input); got != tc.expected {
				t.Errorf("CleanInputPath(%q) = %q; want %q", tc.input, got, tc.expected)
			}
		})
	}
}
