/*
Copyright 2021 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	bs "github.com/inhies/go-bytesize"
)

type configSpec struct {
	PutCount       int
	WorkerCount    int
	KeyCount       int
	ReportInterval time.Duration
	DefragInterval time.Duration
	DefragTimeout  time.Duration
	DataFile       string
	EtcdEndpoints  string
	EtcdTimeout    time.Duration
	HumanReadable  bool

	// --- implementation
	endpoints []string
}

func main() {

	var config configSpec
	flag.IntVar(&config.PutCount, "puts", 1000,
		"total number of PUT requests to KV store")
	flag.IntVar(&config.WorkerCount, "workers", 10,
		"number of worker threads to used to make PUT requests")
	flag.IntVar(&config.KeyCount, "keys", 10,
		"number of keys that should be used for PUT requests")
	flag.DurationVar(&config.ReportInterval, "report", 5*time.Second,
		"interval at which statistics should be output")
	flag.DurationVar(&config.DefragInterval, "defrag", 5*time.Minute,
		"interval at which etcd should be defragmented")
	flag.DurationVar(&config.DefragTimeout, "defrag-timeout", 10*time.Second,
		"duration at which a call to defragment etcd will timeout")
	flag.StringVar(&config.DataFile, "data", "data.json",
		"file that contains the JSON object to be used in PUT requests")
	flag.StringVar(&config.EtcdEndpoints, "endpoints", "localhost:2379",
		"addresses of the etcd instances to which to make the PUT requests")
	flag.DurationVar(&config.EtcdTimeout, "etcd-timeout", 10*time.Second,
		"duration at which a call to etcd will timeout")
	flag.BoolVar(&config.HumanReadable, "human", false,
		"should output be human readable")
	flag.Parse()

	// convert string specifications of end points into string slice
	for _, ep := range strings.Split(config.EtcdEndpoints, ",") {
		config.endpoints = append(config.endpoints, strings.TrimSpace(ep))
	}
	if len(config.endpoints) == 0 {
		log.Fatal("ERROR: at least one etcd endpoint must be specified")
	}

	// read/parse JSON object to write to KV store
	dataFile, err := os.Open(config.DataFile)
	if err != nil {
		log.Fatalf("ERROR: unable to open data file '%s'", config.DataFile)
	}
	defer dataFile.Close()

	jsonDecoder := json.NewDecoder(dataFile)
	jsonData := make(map[string]interface{})
	jsonDecoder.Decode(&jsonData)
	jsonBytes, err := json.Marshal(&jsonData)
	if err != nil {
		log.Fatalf("ERROR: unable to parse file '%s' as JSON", config.DataFile)
	}

	// Create a wait group for the workers and one for the routine that
	// outputs status. This way we can make sure all processing is done
	// before we exit
	var workerWG, statsWG sync.WaitGroup
	workerWG.Add(config.WorkerCount)
	statsWG.Add(1)

	// Create a channel to use to communicate between workers and
	// the routine for stats output
	ch := make(chan time.Duration, 5000)

	// Kick off stats outputer
	go config.runStatus(ch, &statsWG)

	// Kick off workers
	for j := 0; j < config.WorkerCount; j++ {
		go config.runWorker(j, ch, &workerWG, string(jsonBytes))
	}

	// Wait for PUT threads to complete
	workerWG.Wait()

	// Append (stop) to channel so stats processor knows
	// not more samples will be added and then wait for
	// stats process to complete.
	ch <- time.Duration(-1)
	statsWG.Wait()
}

// runStatus periodically outputs telemetry information about the
// test run as well as periodically deframents the store
func (c *configSpec) runStatus(ch <-chan time.Duration, wg *sync.WaitGroup) {
	var min, max, avg, last time.Duration
	count := int64(0)
	// connect to etcd
	defragCli, err := clientv3.New(clientv3.Config{
		Endpoints:   c.endpoints,
		DialTimeout: c.DefragTimeout,
	})
	if err != nil {
		log.Fatalf("ERROR: unable to create connection to KV store for defragmentation using '%s': '%s'\n",
			c.endpoints, err.Error())
	}
	defer defragCli.Close()
	sizeCli, err := clientv3.New(clientv3.Config{
		Endpoints:   c.endpoints,
		DialTimeout: c.EtcdTimeout,
	})
	if err != nil {
		log.Fatalf("ERROR: unable to create connection to KV store for db size telemetry using '%s': '%s'\n",
			c.endpoints, err.Error())
	}
	defer sizeCli.Close()

	// output data header
	fmt.Fprintf(os.Stdout, "Time,Minimum,Maximum,Last,Average,Size\n")

	// tickers for periodic operations like report output and defragmentation
	reportTicker := time.NewTicker(c.ReportInterval)
	defer reportTicker.Stop()
	defragTicker := time.NewTicker(c.DefragInterval)
	defer defragTicker.Stop()

	// process away
	for {
		select {
		case <-defragTicker.C:

			// defrag in the background
			go func() {

				// you have to defrag the endpoints individually
				for _, ep := range c.endpoints {
					ctx, cancel := context.WithTimeout(context.Background(),
						c.DefragTimeout)
					_, err = defragCli.Defragment(ctx, ep)
					cancel()
					if err != nil {
						log.Fatalf("ERROR: unable to defragment etcd endpoint '%s': '%s'\n",
							ep, err.Error())
					}
					fmt.Fprintf(os.Stderr, "Defragmentation of '%s' completed\n", ep)
				}
			}()
		case <-reportTicker.C:
			// output completeness stats to stderr
			complete := count * int64(100) / int64(c.PutCount)
			fmt.Fprintf(os.Stderr, "%d%% - %d of %d\n",
				complete, count, c.PutCount)

			// if we don't have any data, then no need to output anything else
			if count == 0 {
				break
			}

			// calculate db size as average from each endpoint
			dbSize := int64(0)
			dbCnt := int64(0)
			for _, ep := range c.endpoints {
				ctx, cancel := context.WithTimeout(context.Background(), c.EtcdTimeout)
				status, err := sizeCli.Status(ctx, ep)
				cancel()
				if err != nil {
					log.Fatalf("ERROR: unable to read KV storage size from '%s': '%s'\n",
						ep, err.Error())
				}
				dbSize += status.DbSize
				dbCnt++
			}
			if dbCnt == 0 {
				dbSize = 0
			} else {
				dbSize = dbSize / dbCnt
			}

			// output stats as either human readable (with units) or
			// as base units milliseconds/bytes
			if c.HumanReadable {
				fmt.Fprintf(os.Stdout, "%s,%v,%v,%v,%v,%v\n",
					time.Now().Format("15:04:05"),
					min, max, last,
					time.Duration(int64(avg)/count),
					bs.New(float64(dbSize)),
				)
			} else {
				fmt.Fprintf(os.Stdout, "%s,%d,%d,%d,%d,%d\n",
					time.Now().Format("15:04:05"),
					min.Milliseconds(),
					max.Milliseconds(),
					last.Milliseconds(),
					avg.Milliseconds()/count,
					dbSize,
				)
			}
		case sample, ok := <-ch:

			// if the sample is -1, it means not more samples
			// so we exit
			if !ok || sample == time.Duration(-1) {
				// signal all done
				wg.Done()
				return
			}

			// update the telemetry information
			if count == 0 {
				min = sample
				max = sample
			}
			if sample < min {
				min = sample
			}
			if sample > max {
				max = sample
			}
			last = sample
			avg += sample
			count++
		}
	}
}

// runWorker PUT, PUT as fast as you can
func (c *configSpec) runWorker(id int, ch chan<- time.Duration, wg *sync.WaitGroup, jsonEntity string) {

	// get etcd connection
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   c.endpoints,
		DialTimeout: c.EtcdTimeout,
	})
	if err != nil {
		log.Fatalf("ERROR: unable to connect to KV store at '%s': '%s'\n",
			c.endpoints, err.Error())
	}
	defer cli.Close()

	// pre-generate the key names so we don't have to do a fmt.Sprintf for
	// each put
	var keys []string

	// note, if KeyCount isn't integer divisible by WorkerCount
	// you may not get all the keys you expect
	myKeyCount := int(c.KeyCount / c.WorkerCount)
	for i := 0; i < myKeyCount; i++ {
		keys = append(keys, fmt.Sprintf("/test-%d-%d", id, i))
	}

	// to our portion of the PUTs.
	// note, if PutCount isn't integer divisible by WorkerCount
	// you may not get all the PUTs you expect
	for i := 0; i < c.PutCount/c.WorkerCount; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), c.EtcdTimeout)

		// time the execution of the PUT
		startTime := time.Now()
		_, err = cli.Put(ctx, keys[i%myKeyCount], jsonEntity)
		endTime := time.Now()
		cancel()
		if err != nil {
			log.Fatalf("ERROR: unable to complete PUT: '%s', timeout is '%v' taking '%v'\n",
				err.Error(), c.EtcdTimeout, endTime.Sub(startTime))
		}

		// send the time of the PUT to the channel to be collated
		delta := endTime.Sub(startTime)
		ch <- delta
	}

	// signal all done
	wg.Done()
}
