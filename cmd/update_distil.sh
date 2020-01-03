#/bin/bash
find . -maxdepth 1 -mindepth 1 -type d -exec sh -c "cd \"{}\" && pwd && go get github.com/uncharted-distil/distil@${1} && go mod tidy" \;
