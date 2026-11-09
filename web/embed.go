package web

import (
	"embed"
	"io/fs"
)

// StaticFS contains the browser UI shipped inside the client executable.
// Embedding keeps the local agent usable without a network or package manager.
//
//go:embed static
var embeddedStatic embed.FS

// StaticFiles returns a read-only filesystem rooted at static/.
func StaticFiles() (fs.FS, error) { return fs.Sub(embeddedStatic, "static") }
