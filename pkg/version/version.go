package version

import "fmt"

var (
	Version   = "dev"
	GitCommit = "none"
	BuildDate = "unknown"
)

func Print() {
	fmt.Printf("Version: %s\nGit Commit: %s\nBuild Date: %s\n", Version, GitCommit, BuildDate)
}
