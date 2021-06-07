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
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	bs "github.com/inhies/go-bytesize"
)

type Frequency struct {
	Count  int
	Period time.Duration
}

func (f *Frequency) Set(s string) error {
	parts := strings.Split(s, "/")
	if len(parts) != 2 {
		return errors.New("bad-format")
	}

	count, err := strconv.Atoi(parts[0])
	if err != nil {
		return err
	}

	period, err := time.ParseDuration(parts[1])
	if err != nil {
		return err
	}

	f.Count = count
	f.Period = period

	return nil
}

func (f *Frequency) String() string {
	return fmt.Sprintf("%d/%v", f.Count, f.Period)
}

type configSpec struct {
	PutCondition   int
	PutFrequency   Frequency
	PutWorkerCount int
	GetCondition   int
	GetFrequency   Frequency
	GetWorkerCount int
	DelCondition   int
	DelFrequency   Frequency
	DelWorkerCount int
	KeyCount       int
	OverlapKeys    bool
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

const (
	STOP int = iota
	PUT
	GET
	DEL
)

var (
	OpNames = []string{"STOP", "PUT", "GET", "DEL"}
)

type timingSample struct {
	Op       int
	Duration time.Duration
}

type stat struct {
	count               int64
	min, max, last, avg time.Duration
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func avg(sum time.Duration, count int64) time.Duration {
	if count > 0 {
		return time.Duration(sum.Milliseconds()/count) * time.Millisecond
	}
	return 0
}

func (s *stat) Update(sample timingSample) {
	// update the telemetry information
	if s.count == 0 {
		s.min = sample.Duration
		s.max = sample.Duration
	}
	if sample.Duration < s.min {
		s.min = sample.Duration
	}
	if sample.Duration > s.max {
		s.max = sample.Duration
	}
	s.last = sample.Duration
	s.avg += sample.Duration
	s.count++
}

func setFlagDefault(name, value string) {
	flag.Lookup(name).DefValue = value
	flag.Set(name, flag.Lookup(name).DefValue)
}

func main() {

	// If we don't seed then go uses a constant seed and
	// each run is identical
	rand.Seed(time.Now().UnixNano())

	var config configSpec
	flag.IntVar(&config.PutCondition, "pcon", 1000,
		"number of PUT requests to KV store to complete test")
	flag.Var(&config.PutFrequency, "puts",
		"number of PUT requests to KV store attempted over the specified period")
	setFlagDefault("puts", "1000/1m")
	flag.IntVar(&config.PutWorkerCount, "putters", 10,
		"number of worker threads to used to make PUT requests")
	flag.IntVar(&config.GetCondition, "gcon", 1000,
		"number of GET requests to KV store to complete test")
	flag.Var(&config.GetFrequency, "gets",
		"number of GET requests to KV store attempted over the specified period")
	setFlagDefault("gets", "1000/1m")
	flag.IntVar(&config.GetWorkerCount, "getters", 10,
		"number of worker threads to used to make GET requests")
	flag.IntVar(&config.DelCondition, "dcon", 1000,
		"number of DEL requests to KV store to complete test")
	flag.Var(&config.DelFrequency, "dels",
		"number of DEL requests to KV store attempted per over the specified period")
	setFlagDefault("dels", "1000/1m")
	flag.IntVar(&config.DelWorkerCount, "dellers", 10,
		"number of worker threads to used to make DEL requests")
	flag.IntVar(&config.KeyCount, "keys", 10,
		"number of keys that should be used for PUT requests")
	flag.BoolVar(&config.OverlapKeys, "overlap", false,
		"should all PUT threads write to same set of keys")
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
	var workWG, statsWG sync.WaitGroup
	workWG.Add(config.PutWorkerCount +
		config.GetWorkerCount + config.DelWorkerCount)
	statsWG.Add(1)

	// Create a channel to use to communicate between workers and
	// the routine for stats output
	ch := make(chan timingSample, 5000)
	defer close(ch)

	ctx, cancel := context.WithCancel(context.Background())

	// Kick off stats outputer
	go config.runStatus(ch, &statsWG)

	// We want to make sure that we PUT the number of keys that
	// was specified as the command line option, so we will divide
	// the keyspace by the number of workers. Once the required number
	// of keys are PUT we will go back to random keys. First we create
	// an shuffle the list of keys
	perWorker := config.KeyCount / config.PutWorkerCount
	keyIdx := 0
	endIdx := config.KeyCount

	jsonString := string(jsonBytes)
	// Kick off workers
	for j := 0; j < config.PutWorkerCount; j++ {

		// Create a range of keys for each worker
		if !config.OverlapKeys {
			keyIdx = j * perWorker
			endIdx = (j + 1) * perWorker
		}
		go config.runWorker(ctx, PUT, config.PutFrequency, ch, &workWG,
			func(ctx context.Context, cli *clientv3.Client, key string) error {
				var err error
				if keyIdx < endIdx {
					_, err = cli.Put(ctx,
						fmt.Sprintf("/test-%d", keyIdx),
						jsonString)
					keyIdx++
				} else {
					_, err = cli.Put(ctx, key, jsonString)
				}
				return err
			})
	}

	for j := 0; j < config.GetWorkerCount; j++ {
		go config.runWorker(ctx, GET, config.GetFrequency, ch, &workWG,
			func(ctx context.Context, cli *clientv3.Client, key string) error {
				_, err := cli.Get(ctx, key)
				return err
			})
	}

	for j := 0; j < config.DelWorkerCount; j++ {
		go config.runWorker(ctx, DEL, config.DelFrequency, ch, &workWG,
			func(ctx context.Context, cli *clientv3.Client, key string) error {
				_, err := cli.Delete(ctx, key)
				return err
			})
	}

	statsWG.Wait()
	cancel()

	// Wait for PUT threads to complete
	workWG.Wait()
}

// runStatus periodically outputs telemetry information about the
// test run as well as periodically deframents the store
func (c *configSpec) runStatus(ch <-chan timingSample, wg *sync.WaitGroup) {
	stats := map[int]*stat{
		STOP: &stat{},
		PUT:  &stat{},
		GET:  &stat{},
		DEL:  &stat{},
	}
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
	fmt.Fprintf(os.Stdout, "TIME,PUT_MINIMUM,PUT_MAXIMUM,PUT_LAST,PUT_AVERAGE,GET_MINIMUM,GET_MAXIMUM,GET_LAST,GET_AVERAGE,DEL_MAXIMUM,DEL_MINIMUM,DEL_LAST,DEL_AVERAGE,DB_SIZE\n")

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

			total := c.PutCondition + c.GetCondition + c.DelCondition
			count := min(stats[PUT].count, int64(c.PutCondition)) +
				min(stats[GET].count, int64(c.GetCondition)) +
				min(stats[DEL].count, int64(c.DelCondition))
			// output completeness stats to stderr
			complete := count * int64(100) / int64(total)
			fmt.Fprintf(os.Stderr, "%d%% - %d/%d    %d/%d   %d/%d\n",
				complete,
				stats[PUT].count, c.PutCondition,
				stats[GET].count, c.GetCondition,
				stats[DEL].count, c.DelCondition)

			// if we don't have any data, then no need to output anything else
			if stats[PUT].count == 0 {
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
				fmt.Fprintf(os.Stdout, "%s,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v\n",
					time.Now().Format("15:04:05"),
					stats[PUT].min, stats[PUT].max, stats[PUT].last,
					avg(stats[PUT].avg, stats[PUT].count),
					stats[GET].min, stats[GET].max, stats[GET].last,
					avg(stats[GET].avg, stats[GET].count),
					stats[DEL].min, stats[DEL].max, stats[DEL].last,
					avg(stats[DEL].avg, stats[DEL].count),
					bs.New(float64(dbSize)),
				)
			} else {
				fmt.Fprintf(os.Stdout, "%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
					time.Now().Format("15:04:05"),
					stats[PUT].min.Milliseconds(),
					stats[PUT].max.Milliseconds(),
					stats[PUT].last.Milliseconds(),
					avg(stats[PUT].avg, stats[PUT].count).Milliseconds(),
					stats[GET].min.Milliseconds(),
					stats[GET].max.Milliseconds(),
					stats[GET].last.Milliseconds(),
					avg(stats[GET].avg, stats[GET].count).Milliseconds(),
					stats[DEL].min.Milliseconds(),
					stats[DEL].max.Milliseconds(),
					stats[DEL].last.Milliseconds(),
					avg(stats[DEL].avg, stats[DEL].count).Milliseconds(),
					dbSize,
				)
			}

			// Check exit conditions
			if stats[PUT].count >= int64(c.PutCondition) &&
				stats[GET].count >= int64(c.GetCondition) &&
				stats[DEL].count >= int64(c.DelCondition) {
				wg.Done()
				return
			}
		case sample := <-ch:
			switch sample.Op {
			case STOP:
				// signal all done
				wg.Done()
				return
			default:
				stats[sample.Op].Update(sample)
			}
		}
	}
}

func (c *configSpec) withIntervalSpacer(ctx context.Context, target int, cli *clientv3.Client, frequency Frequency, spacer time.Duration, ch chan<- timingSample, work func(context.Context, *clientv3.Client, string) error) (int, time.Duration) {
	period := time.NewTicker(frequency.Period)
	defer period.Stop()
	interval := time.NewTicker(spacer)
	defer interval.Stop()
	count := 0
	for {
		select {
		case <-ctx.Done():
			return 0, time.Duration(0)
		case <-period.C:
			// Period over, didn't complete work, return how many completed
			return count, time.Duration(0)
		case <-interval.C:
			ctx, cancel := context.WithTimeout(context.Background(), c.EtcdTimeout)
			// time the execution of the PUT
			startTime := time.Now()
			key := rand.Intn(c.KeyCount)
			err := work(ctx, cli, fmt.Sprintf("/test-%d", key))
			endTime := time.Now()
			cancel()
			if err != nil {
				log.Fatalf("ERROR: unable to complete PUT: '%s', timeout is '%v' taking '%v'\n",
					err.Error(), c.EtcdTimeout, endTime.Sub(startTime))
			}
			ch <- timingSample{
				Op:       target,
				Duration: endTime.Sub(startTime),
			}
			count++
			if count > frequency.Count {
				// Reached limit of work
				startWait := time.Now()
				<-period.C
				endWait := time.Now()
				return frequency.Count, endWait.Sub(startWait)
			}
		}
	}
}

func (c *configSpec) noIntervalSpacer(ctx context.Context, target int, cli *clientv3.Client, frequency Frequency, ch chan<- timingSample, work func(context.Context, *clientv3.Client, string) error) (int, time.Duration) {
	period := time.NewTicker(frequency.Period)
	defer period.Stop()
	for count := 0; count < frequency.Count; count++ {
		select {
		case <-ctx.Done():
			return 0, time.Duration(0)
		case <-period.C:
			return count, time.Duration(0)
		default:
			ctx, cancel := context.WithTimeout(context.Background(), c.EtcdTimeout)
			// time the execution of the PUT
			startTime := time.Now()
			key := rand.Intn(c.KeyCount)
			err := work(ctx, cli, fmt.Sprintf("/test-%d", key))
			endTime := time.Now()
			cancel()
			if err != nil {
				log.Fatalf("ERROR: unable to complete PUT: '%s', timeout is '%v' taking '%v'\n",
					err.Error(), c.EtcdTimeout, endTime.Sub(startTime))
			}
			ch <- timingSample{
				Op:       target,
				Duration: endTime.Sub(startTime),
			}
		}
	}
	startWait := time.Now()
	select {
	case <-ctx.Done():
		return 0, time.Duration(0)
	case <-period.C:
		break
	}
	endWait := time.Now()
	return frequency.Count, endWait.Sub(startWait)
}

func (c *configSpec) runWorker(ctx context.Context, target int, frequency Frequency, ch chan<- timingSample, wg *sync.WaitGroup, work func(context.Context, *clientv3.Client, string) error) {

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

	// Because we are executing on frequency we want to have an even spread
	// as opposed to do all as fast as we can then wait. To implement this
	// we use a sleep, knowing that the sleep
	var spacer time.Duration

	for {
		var count int
		var left time.Duration
		select {
		case <-ctx.Done():
			wg.Done()
			return
		default:
			if spacer > 0 {
				count, left = c.withIntervalSpacer(ctx, target, cli, frequency, spacer, ch, work)
			} else {
				count, left = c.noIntervalSpacer(ctx, target, cli, frequency, ch, work)
			}
			if ctx.Err() != context.Canceled {
				if frequency.Count > 1 {
					if left > 0 {
						spacer, _ = time.ParseDuration(fmt.Sprintf("%dns", spacer.Nanoseconds()+left.Nanoseconds()/int64(frequency.Count)))
					} else {
						if count != 0 {
							if spacer != 0 {
								ns := frequency.Period.Nanoseconds() / ((int64(frequency.Count) * (frequency.Period.Nanoseconds() / spacer.Nanoseconds())) / int64(count))
								spacer, _ = time.ParseDuration(fmt.Sprintf("%dns", ns))
							}
						} else {
							spacer = 0
						}
					}
				}
			}
		}
	}
}
