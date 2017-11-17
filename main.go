package main

import (
	"github.com/unchartedsoftware/distil-ingest/rest"
)

func main() {
	cl := rest.NewClient("http://localhost:5000")
	r := rest.NewRanker("pca", cl)

	r.RankFile("/home/phorne/data/d3m/o_196/data/merged.csv")
}
