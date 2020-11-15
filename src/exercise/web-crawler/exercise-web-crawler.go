package main

import (
	"fmt"
	"sync"
	"time"
)

//
// Serial crawler
//

func Serial(url string, fetcher Fetcher, fetched map[string]bool) {
	if ok := fetched[url]; ok {
		return
	}
	fetched[url] = true
	_, urls, err := fetcher.Fetch(url)
	if err != nil {
		return
	}
	for _, u := range urls {
		Serial(u, fetcher, fetched)
	}
	return
}

//
// Concurrent crawler with mutex
//

type fetchState struct {
	lock    sync.Mutex
	fetched map[string]bool
}

func ConcurrentMutex(url string, fetcher Fetcher, f *fetchState) {
	f.lock.Lock()
	already := f.fetched[url]
	f.fetched[url] = true
	f.lock.Unlock()

	if already {
		return
	}

	_, urls, err := fetcher.Fetch(url)
	if err != nil {
		return
	}

	var done sync.WaitGroup
	for _, u := range urls {
		done.Add(1)
		go func(u2 string) {
			defer done.Done()
			ConcurrentMutex(u2, fetcher, f)
		}(u)
	}
	done.Wait()
	return
}

func initState() *fetchState {
	f := &fetchState{}
	f.fetched = make(map[string]bool)
	return f
}

//
// Concurrent crawler with channels
//

func ConcurrentChannel(url string, fetcher Fetcher) {
	ch := make(chan []string, 10)
	ch <- []string{url}
	master(ch, fetcher)
}

func master(ch chan []string, fetcher Fetcher) {
	fetched := make(map[string]bool)
	for {
		select {
		case urls := <-ch:
			for _, u := range urls {
				if ok := fetched[u]; !ok {
					fetched[u] = true
					go worker(u, ch, fetcher)
				}
			}
		case <-time.After(time.Second * 2):
			return
		}
	}
}

func worker(url string, ch chan []string, fetcher Fetcher) {
	_, urls, err := fetcher.Fetch(url)
	if err == nil {
		ch <- urls
	}
}

func main() {
	fmt.Println("====Serial====")
	Serial("https://golang.org/", fetcher, make(map[string]bool))
	fmt.Println("====ConcurrentMutex====")
	ConcurrentMutex("https://golang.org/", fetcher, initState())
	fmt.Println("====ConcurrentChannel====")
	ConcurrentChannel("https://golang.org/", fetcher)
}

//
//	Global definition
//

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		fmt.Printf("found:   %v\n", url)
		return res.body, res.urls, nil
	}
	fmt.Printf("missing: %v\n", url)
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
	"https://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
		},
	},
	"https://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
}
