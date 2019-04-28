package wellwheel

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"testing"
)

// it shows how bufio impacts performance.
func BenchmarkBufWrite(b *testing.B) {
	bytesPerWrite := []int{64, 128, 256}
	b.Run("", benchBufWriteRun(benchBufWrite, bytesPerWrite))
}

func benchBufWriteRun(f func(*testing.B, int, bool), sizes []int) func(*testing.B) {
	return func(b *testing.B) {
		for _, s := range sizes {
			b.Run(fmt.Sprintf("with buf bytes per write:%d", s), func(b *testing.B) {
				f(b, s, true)
			})
			b.Run(fmt.Sprintf("no buf bytes per write:%d", s), func(b *testing.B) {
				f(b, s, false)
			})
		}
	}
}

func benchBufWrite(b *testing.B, size int, withBuf bool) {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		b.Fatal(err)
	}
	var w io.Writer
	w = f
	if withBuf {
		w = bufio.NewWriterSize(f, 32*1024)
	}

	l := log.New(w, "", log.Ldate)

	p := make([]byte, size)
	content := string(p)
	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Println(content)
	}
}
