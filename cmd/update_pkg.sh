#/bin/bash
find . -maxdepth 1 -mindepth 1 -type d -exec sh -c "cd \"{}\" && pwd && go get github.com/uncharted-distil/distil-ingest/pkg@${1}" \;
#find . -maxdepth 1 -mindepth 1 -type d -exec sh -c "cd \"{}\" && pwd && go get github.com/uncharted-distil/distil-compute@${1}" \;
