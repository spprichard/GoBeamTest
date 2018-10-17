package main

import (
	"context"
	"flag"
	"fmt"
	"reflect"
	"regexp"

	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"

	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"

	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"

	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"

	"github.com/apache/beam/sdks/go/pkg/beam/log"

	"github.com/apache/beam/sdks/go/pkg/beam"
)

// Output seems to be broken? go run main.go --output="results.txt" doesn't work

var (
	input  = flag.String("input", "gs://apache-beam-samples/shakespeare/kinglear.txt", "File(s) to read.")
	filter = flag.String("filter", "Flourish|stomach", "Regex filter pattern to use. Only words matching this pattern will be included.")
	output = flag.String("output", "", "Output file (required).")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*filterFn)(nil)).Elem())
}

// filterFn is a DoFn for filtering out certain words.
type filterFn struct {
	Filter string `json:"filter"`
	re     *regexp.Regexp
}

func (f *filterFn) Setup() {
	f.re = regexp.MustCompile(f.Filter)
}

func (f *filterFn) ProcessElement(ctx context.Context, word string, count int, emit func(string, int)) {
	if f.re.MatchString(word) {
		log.Infof(ctx, "Matched: %v", word)
		emit(word, count)
	} else {
		log.Debugf(ctx, "Did not match: %v", word)
	}
}

// The below transforms are identical to the wordcount versions. If this was
// production code, common transforms would be placed in a separate package
// and shared directly rather than being copied.

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

// extractFn is a DoFn that emits the words in a given line.
func extractFn(line string, emit func(string)) {
	for _, word := range wordRE.FindAllString(line, -1) {
		emit(word)
	}
}

func formatFn(w string, c int) string {
	return fmt.Sprintf("%s: %v", w, c)
}

// CountWords is a composite transform that counts the words of an PCollection
// of lines. It expects a PCollection of type string and returns a PCollection
// of type KV<string,int>.
func CountWords(s beam.Scope, lines beam.PCollection) beam.PCollection {
	s = s.Scope("CountWords")
	col := beam.ParDo(s, extractFn, lines)
	return stats.Count(s, col)
}

func main() {
	flag.Parse()
	beam.Init()

	// Concept #2: the beam logging package works both during pipeline
	// construction and at runtime. It should always be used.
	ctx := context.Background()
	if *output == "" {
		log.Exit(ctx, "No output provided")
	}

	if _, err := regexp.Compile(*filter); err != nil {
		log.Exitf(ctx, "Invalid Filter %v", err)
	}

	p, s := beam.NewPipelineWithRoot()

	lines := textio.Read(s, *input)
	counted := CountWords(s, lines)
	filtered := beam.ParDo(s, &filterFn{Filter: *filter}, counted)
	formatted := beam.ParDo(s, formatFn, filtered)

	// Concept #3: passert is a set of convenient PTransforms that can be used
	// when writing Pipeline level tests to validate the contents of
	// PCollections. passert is best used in unit tests with small data sets
	// but is demonstrated here as a teaching tool.

	passert.Equals(s, formatted, "Flourish: 3", "stomach: 1")

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
