package main

import (
	"context"
	"fmt"
	"regexp"

	"github.com/apache/beam/sdks/go/pkg/beam/runners/direct"

	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"

	_ "github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/gcs"
	_ "github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/local"
)

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

func main() {
	fmt.Println("Starting...")

	p := beam.NewPipeline()
	s := p.Root()

	lines := textio.Read(s, "gs://apache-beam-samples/shakespeare/*")

	words := beam.ParDo(s, func(line string, emit func(string)) {
		for _, word := range wordRE.FindAllString(line, -1) {
			emit(word)
		}
	}, lines)

	counted := stats.Count(s, words)

	formated := beam.ParDo(s, func(w string, c int) string {
		return fmt.Sprintf("%s: %v", w, c)
	}, counted)

	textio.Write(s, "final-results.txt", formated)

	direct.Execute(context.Background(), p)

	fmt.Println("Done...")

}
