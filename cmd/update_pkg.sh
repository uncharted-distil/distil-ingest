#/bin/bash
find . -maxdepth 1 -mindepth 1 -type d -exec sh -c "cd \"{}\" && pwd && go get -u github.com/uncharted-distil/distil-ingest/pkg@${1} && go mod tidy" \;
