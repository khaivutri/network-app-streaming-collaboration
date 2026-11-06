package main

import (
	"reflect"
	"testing"
)

func TestParseCommandLine(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "simple sendfile",
			input: `/sendfile Bob sample.txt "File tài liệu"`,
			want:  []string{"/sendfile", "Bob", "sample.txt", "File tài liệu"},
		},
		{
			name:  "path with spaces in double quotes",
			input: `/sendfile Bob "C:\My Documents\Báo cáo đồ án.docx" "Báo cáo cuối kỳ"`,
			want:  []string{"/sendfile", "Bob", `C:\My Documents\Báo cáo đồ án.docx`, "Báo cáo cuối kỳ"},
		},
		{
			name:  "path with spaces in single quotes",
			input: `/sendfile Bob 'D:\Folder Name\my file.bin' Kèm ghi chú`,
			want:  []string{"/sendfile", "Bob", `D:\Folder Name\my file.bin`, "Kèm", "ghi", "chú"},
		},
		{
			name:  "drag and drop path with spaces",
			input: `/sendfile Bob "C:\Users\Admin\Desktop\Test File.txt"`,
			want:  []string{"/sendfile", "Bob", `C:\Users\Admin\Desktop\Test File.txt`},
		},
		{
			name:  "extra whitespace between tokens",
			input: `  /sendfile    Bob    "file with spaces.txt"   "caption"  `,
			want:  []string{"/sendfile", "Bob", "file with spaces.txt", "caption"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseCommandLine(tt.input)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

