package main

import (
	"flag"
	"log"
	"os"
	"runtime/pprof"
	"sync"
)

func main() {
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	var wg sync.WaitGroup
	server := NewProxyServer("0.0.0.0:6479")
	server.Start()
	wg.Add(1)
	wg.Wait()
}

