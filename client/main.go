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
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	bs "github.com/inhies/go-bytesize"
	"go.etcd.io/etcd/clientv3"
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
	PutCondition        int
	PutFrequency        Frequency
	PutWorkerCount      int
	GetCondition        int
	GetFrequency        Frequency
	GetWorkerCount      int
	DelCondition        int
	DelFrequency        Frequency
	DelWorkerCount      int
	KeyCount            int
	OverlapKeys         bool
	ReportInterval      time.Duration
	DefragInterval      time.Duration
	DefragTimeout       time.Duration
	DataFile            string
	DataSize            string
	EtcdEndpoints       string
	EtcdTimeout         time.Duration
	HumanReadable       bool
	TimeMeasurementUnit string
	EtcdConnection      string
	PoolCapacity        int

	// --- implementation
	endpoints []string
	unit      time.Duration
}

const (
	STOP int = iota
	PUT
	GET
	DEL
)

var (
	OpNames     = []string{"STOP", "PUT", "GET", "DEL"}
	UnitMapping = map[string]time.Duration{
		"ns":          time.Nanosecond,
		"nanosecond":  time.Nanosecond,
		"µs":          time.Microsecond,
		"us":          time.Microsecond,
		"microsecond": time.Microsecond,
		"ms":          time.Millisecond,
		"millisecond": time.Millisecond,
	}
	ConnectionMapping = map[string]struct{}{
		"pooled":   struct{}{},
		"shared":   struct{}{},
		"separate": struct{}{},
		"robin":    struct{}{},
	}
)

type timingSample struct {
	Op    int
	Total time.Duration
	Wait  time.Duration
	Etcd  time.Duration
}

type stat struct {
	count int64
	total struct {
		min, max, last, avg time.Duration
	}
	wait struct {
		min, max, last, avg time.Duration
	}
	etcd struct {
		min, max, last, avg time.Duration
	}
}

func mapKeysAsDescription(conjunction string, in interface{}) string {
	var result strings.Builder

	inValue := reflect.ValueOf(in)

	if inValue.Kind() != reflect.Map {
		return ""
	}

	// fetch the keys and sort them
	keys := inValue.MapKeys()
	sort.Slice(keys, func(a, b int) bool {
		return keys[a].String() < keys[b].String()
	})

	// walk the keys and add them to the result
	// string. add the commas and "and" where
	// appropriate
	for keyIndex, key := range keys {
		if keyIndex != 0 {
			if len(keys) > 2 {
				result.WriteString(", ")
			} else {
				result.WriteString(" ")
			}
		}
		if keyIndex == len(keys)-1 && len(keys) > 1 {
			result.WriteString(conjunction)
			result.WriteString(" ")
		}
		result.WriteString(fmt.Sprintf("'%s'", key.String()))
	}
	return result.String()
}

func (c *configSpec) toMU(d time.Duration) int64 {
	switch c.unit {
	case time.Nanosecond:
		return d.Nanoseconds()
	case time.Microsecond:
		return d.Microseconds()
	case time.Millisecond:
		fallthrough
	default:
		return d.Milliseconds()
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (c *configSpec) avg(sum time.Duration, count int64) time.Duration {
	if count > 0 {
		return time.Duration(c.toMU(sum)/count) * c.unit
	}
	return 0
}

func (s *stat) Update(sample timingSample, minDurationValue time.Duration) {

	totalDuration := maxDuration(sample.Total, minDurationValue)
	waitDuration := maxDuration(sample.Wait, minDurationValue)
	etcdDuration := maxDuration(sample.Etcd, minDurationValue)

	// update the telemetry information
	if s.count == 0 {
		s.total.min = totalDuration
		s.total.max = totalDuration
		s.wait.min = waitDuration
		s.wait.max = waitDuration
		s.etcd.min = etcdDuration
		s.etcd.max = etcdDuration
	}
	s.total.min = minDuration(s.total.min, totalDuration)
	s.total.max = maxDuration(s.total.max, totalDuration)
	s.total.last = totalDuration
	s.total.avg += totalDuration

	s.wait.min = minDuration(s.wait.min, waitDuration)
	s.wait.max = maxDuration(s.wait.max, waitDuration)
	s.wait.last = waitDuration
	s.wait.avg += waitDuration

	s.etcd.min = minDuration(s.etcd.min, etcdDuration)
	s.etcd.max = maxDuration(s.etcd.max, etcdDuration)
	s.etcd.last = etcdDuration
	s.etcd.avg += etcdDuration
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
	flag.StringVar(&config.DataSize, "size", "0kb",
		"size of generated data instead of using a file")
	flag.StringVar(&config.EtcdEndpoints, "endpoints", "localhost:2379",
		"addresses of the etcd instances to which to make the PUT requests")
	flag.DurationVar(&config.EtcdTimeout, "etcd-timeout", 10*time.Second,
		"duration at which a call to etcd will timeout")
	flag.BoolVar(&config.HumanReadable, "human", false,
		"should output be human readable")
	flag.StringVar(&config.TimeMeasurementUnit, "unit", "ms",
		"granularity of time measurements")
	flag.StringVar(&config.EtcdConnection, "connection", "pooled",
		fmt.Sprintf("all PUT, GET, and DEL operations use a %s connection to etcd",
			mapKeysAsDescription("or", ConnectionMapping)))
	flag.IntVar(&config.PoolCapacity, "pool", 200,
		"the capacity of the connection pool when using 'pooled' or 'robin' etcd connection type")
	flag.Parse()

	// convert string specifications of end points into string slice
	for _, ep := range strings.Split(config.EtcdEndpoints, ",") {
		config.endpoints = append(config.endpoints, strings.TrimSpace(ep))
	}
	if len(config.endpoints) == 0 {
		log.Fatal("ERROR: at least one etcd endpoint must be specified")
	}

	// parse the measurement unit option
	if val, ok := UnitMapping[strings.ToLower(config.TimeMeasurementUnit)]; ok {
		config.unit = val
	} else {
		log.Fatalf("ERROR: unknown measurement unit specified ('%s'), valid value are %s",
			config.TimeMeasurementUnit, mapKeysAsDescription("and", UnitMapping))
	}

	if _, ok := ConnectionMapping[strings.ToLower(config.EtcdConnection)]; !ok {
		log.Fatalf("ERROR: unknown etcd connection type specified ('%s'), valid values are %s",
			config.EtcdConnection, mapKeysAsDescription("and", ConnectionMapping))
	}

	// if data size > 0 then we use that
	var dataBytes []byte
	var count bs.ByteSize
	var err error
	if len(config.DataSize) > 0 {
		count, err = bs.Parse(config.DataSize)
		if err != nil {
			log.Fatalf("ERROR: unable to parse data size '%s' : %s\n", config.DataSize, err.Error())
		}
		if count > 0 {
			dataBytes = make([]byte, count)
			for i := 0; i < int(count); i++ {
				dataBytes[i] = byte('a' + (i % 26))
			}
		}
	}

	if count == 0 {
		// read/parse JSON object to write to KV store
		dataFile, err := os.Open(config.DataFile)
		if err != nil {
			log.Fatalf("ERROR: unable to open data file '%s' : '%s'", config.DataFile, err.Error())
		}
		defer dataFile.Close()

		jsonDecoder := json.NewDecoder(dataFile)
		jsonData := make(map[string]interface{})
		jsonDecoder.Decode(&jsonData)
		dataBytes, err = json.Marshal(&jsonData)
		if err != nil {
			log.Fatalf("ERROR: unable to parse file '%s' as JSON", config.DataFile)
		}
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
	chCount := make(chan struct{}, 1000)

	ctx, cancel := context.WithCancel(context.Background())

	// Kick off stats outputer
	go config.runStatus(ch, chCount, &statsWG)

	// We want to make sure that we PUT the number of keys that
	// was specified as the command line option, so we will divide
	// the keyspace by the number of workers. Once the required number
	// of keys are PUT we will go back to random keys. First we create
	// an shuffle the list of keys
	perWorker := config.KeyCount / config.PutWorkerCount
	keyIdx := 0
	endIdx := config.KeyCount

	var factory ConnectionAllocatorFactory
	switch strings.ToLower(config.EtcdConnection) {
	case "shared":
		factory = NewSharedConnectionAllocatorFactory(chCount, config.endpoints, config.EtcdTimeout)
	case "pooled":
		factory = NewPooledConnectionAllocatorFactory(chCount, config.endpoints, config.EtcdTimeout, config.PoolCapacity)
	case "robin":
		factory = NewRobinConnectionAllocatorFactory(chCount, config.endpoints, config.EtcdTimeout, config.PoolCapacity)
	case "separate":
		fallthrough
	default:
		factory = NewSeparateConnectionAllocatorFactory(chCount, config.endpoints, config.EtcdTimeout)
	}

	jsonString := string(dataBytes)
	// Kick off workers
	for j := 0; j < config.PutWorkerCount; j++ {

		// Create a range of keys for each worker
		if !config.OverlapKeys {
			keyIdx = j * perWorker
			endIdx = (j + 1) * perWorker
		}

		go config.runWorker(ctx, PUT, config.PutFrequency, factory.Get(), ch, &workWG,
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
		go config.runWorker(ctx, GET, config.GetFrequency, factory.Get(), ch, &workWG,
			func(ctx context.Context, cli *clientv3.Client, key string) error {
				_, err := cli.Get(ctx, key)
				return err
			})
	}

	for j := 0; j < config.DelWorkerCount; j++ {
		go config.runWorker(ctx, DEL, config.DelFrequency, factory.Get(), ch, &workWG,
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
func (c *configSpec) runStatus(ch <-chan timingSample, chCount <-chan struct{}, wg *sync.WaitGroup) {
	stats := map[int]*stat{
		STOP: &stat{},
		PUT:  &stat{},
		GET:  &stat{},
		DEL:  &stat{},
	}
	connectionCount := 0

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
	if c.HumanReadable {
		fmt.Fprintf(os.Stdout, "% 8s    % 21s    % 21s    % 21s    % 21s    % 21s    % 21s    % 21s    % 21s    % 21s    % 21s    % 21s    % 21s    %s\n",
			"TIME",
			"PUT_MIN(WAIT/ETCD)",
			"PUT_MAX(WAIT/ETCD)",
			"PUT_LAST(WAIT/ETCD)",
			"PUT_AVG(WAIT/ETCD)",
			"GET_MIN(WAIT/ETCD)",
			"GET_MAX(WAIT/ETCD)",
			"GET_LAST(WAIT/ETCD)",
			"GET_AVG(WAIT/ETCD)",
			"DEL_MAX(WAIT/ETCD)",
			"DEL_MIN(WAIT/ETCD)",
			"DEL_LAST(WAIT/ETCD)",
			"DEL_AVG(WAIT/ETCD)",
			"DB_SIZE")
	} else {
		fmt.Fprintf(os.Stdout, "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
			"TIME",
			"PUT_MIN", "PUT_MIN_WAIT", "PUT_MIN_ETCD",
			"PUT_MAX", "PUT_MAX_WAIT", "PUT_MAX_ETCD",
			"PUT_LAST", "PUT_LAST_WAIT", "PUT_LAST_ETCD",
			"PUT_AVG", "PUT_AVG_WAIT", "PUT_AVG_ETCD",
			"GET_MIN", "GET_MIN_WAIT", "GET_MIN_ETCD",
			"GET_MAX", "GET_MAX_WAIT", "GET_MAX_ETCD",
			"GET_LAST", "GET_LAST_WAIT", "GET_LAST_ETCD",
			"GET_AVG", "GET_AVG_WAIT", "GET_AVG_ETCD",
			"DEL_MIN", "DEL_MIN_WAIT", "DEL_MIN_ETCD",
			"DEL_MAX", "DEL_MAX_WAIT", "DEL_MAX_ETCD",
			"DEL_LAST", "DEL_LAST_WAIT", "DEL_LAST_ETCD",
			"DEL_AVG", "DEL_AVG_WAIT", "DEL_AVG_ETCD",
			"DB_SIZE")
	}

	// tickers for periodic operations like report output and defragmentation
	reportTicker := time.NewTicker(c.ReportInterval)
	defer reportTicker.Stop()

	var defragTicker *time.Ticker

	// If the defrag interval is 0 then assume never
	// defrag. This is implemented by using a very long
	// time and then ignoring when the timer is triggered.
	//
	// The long timer is used so in both cases (defrag and
	// no defrag) we can dereference the var in the select.
	if c.DefragInterval == 0 {
		defragTicker = time.NewTicker(100 * time.Hour)
	} else {
		defragTicker = time.NewTicker(c.DefragInterval)
	}
	defer defragTicker.Stop()

	// process away
	for {
		select {
		case <-defragTicker.C:

			if c.DefragInterval == 0 {
				break
			}

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
			fmt.Fprintf(os.Stderr, "%d%% - %d/%d    %d/%d   %d/%d    %d\n",
				complete,
				stats[PUT].count, c.PutCondition,
				stats[GET].count, c.GetCondition,
				stats[DEL].count, c.DelCondition,
				connectionCount)

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
				fmt.Fprintf(os.Stdout, "%s    % 6v(% 6v/% 6v)    % 6v(% 6v/% 6v)    % 6v(% 6v/% 6v)    % 6v(% 6v/% 6v)    % 6v(% 6v/% 6v)    % 6v(% 6v/% 6v)    % 6v(% 6v/% 6v)    % 6v(% 6v/% 6v)    % 6v(% 6v/% 6v)    % 6v(% 6v/% 6v)    % 6v(% 6v/% 6v)    % 6v(% 6v/% 6v)    %v\n",
					time.Now().Format("15:04:05"),

					stats[PUT].total.min.Truncate(c.unit), stats[PUT].wait.min.Truncate(c.unit), stats[PUT].etcd.min.Truncate(c.unit),
					stats[PUT].total.max.Truncate(c.unit), stats[PUT].wait.max.Truncate(c.unit), stats[PUT].etcd.max.Truncate(c.unit),
					stats[PUT].total.last.Truncate(c.unit), stats[PUT].wait.last.Truncate(c.unit), stats[PUT].etcd.last.Truncate(c.unit),
					c.avg(stats[PUT].total.avg, stats[PUT].count).Truncate(c.unit), c.avg(stats[PUT].wait.avg, stats[PUT].count).Truncate(c.unit), c.avg(stats[PUT].etcd.avg, stats[PUT].count).Truncate(c.unit),

					stats[GET].total.min.Truncate(c.unit), stats[GET].wait.min.Truncate(c.unit), stats[GET].etcd.min.Truncate(c.unit),
					stats[GET].total.max.Truncate(c.unit), stats[GET].wait.max.Truncate(c.unit), stats[GET].etcd.max.Truncate(c.unit),
					stats[GET].total.last.Truncate(c.unit), stats[GET].wait.last.Truncate(c.unit), stats[GET].etcd.last.Truncate(c.unit),
					c.avg(stats[GET].total.avg, stats[GET].count).Truncate(c.unit), c.avg(stats[GET].wait.avg, stats[GET].count).Truncate(c.unit), c.avg(stats[GET].etcd.avg, stats[GET].count).Truncate(c.unit),

					stats[DEL].total.min.Truncate(c.unit), stats[DEL].wait.min.Truncate(c.unit), stats[DEL].etcd.min.Truncate(c.unit),
					stats[DEL].total.max.Truncate(c.unit), stats[DEL].wait.max.Truncate(c.unit), stats[DEL].etcd.max.Truncate(c.unit),
					stats[DEL].total.last.Truncate(c.unit), stats[DEL].wait.last.Truncate(c.unit), stats[DEL].etcd.last.Truncate(c.unit),
					c.avg(stats[DEL].total.avg, stats[DEL].count).Truncate(c.unit), c.avg(stats[DEL].wait.avg, stats[DEL].count).Truncate(c.unit), c.avg(stats[DEL].etcd.avg, stats[DEL].count).Truncate(c.unit),

					bs.New(float64(dbSize)),
				)
			} else {
				fmt.Fprintf(os.Stdout, "%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
					time.Now().Format("15:04:05"),
					c.toMU(stats[PUT].total.min), c.toMU(stats[PUT].wait.min), c.toMU(stats[PUT].etcd.min),
					c.toMU(stats[PUT].total.max), c.toMU(stats[PUT].wait.max), c.toMU(stats[PUT].etcd.max),
					c.toMU(stats[PUT].total.last), c.toMU(stats[PUT].wait.last), c.toMU(stats[PUT].etcd.last),
					c.toMU(c.avg(stats[PUT].total.avg, stats[PUT].count)), c.toMU(c.avg(stats[PUT].wait.avg, stats[PUT].count)), c.toMU(c.avg(stats[PUT].etcd.avg, stats[PUT].count)),

					c.toMU(stats[GET].total.min), c.toMU(stats[GET].wait.min), c.toMU(stats[GET].etcd.min),
					c.toMU(stats[GET].total.max), c.toMU(stats[GET].wait.max), c.toMU(stats[GET].etcd.max),
					c.toMU(stats[GET].total.last), c.toMU(stats[GET].wait.last), c.toMU(stats[GET].etcd.last),
					c.toMU(c.avg(stats[GET].total.avg, stats[GET].count)), c.toMU(c.avg(stats[GET].wait.avg, stats[GET].count)), c.toMU(c.avg(stats[GET].etcd.avg, stats[GET].count)),

					c.toMU(stats[DEL].total.min), c.toMU(stats[DEL].wait.min), c.toMU(stats[DEL].etcd.min),
					c.toMU(stats[DEL].total.max), c.toMU(stats[DEL].wait.max), c.toMU(stats[DEL].etcd.max),
					c.toMU(stats[DEL].total.last), c.toMU(stats[DEL].wait.last), c.toMU(stats[DEL].etcd.last),
					c.toMU(c.avg(stats[DEL].total.avg, stats[DEL].count)), c.toMU(c.avg(stats[DEL].wait.avg, stats[DEL].count)), c.toMU(c.avg(stats[DEL].etcd.avg, stats[DEL].count)),
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
		case <-chCount:
			connectionCount++
		case sample := <-ch:
			switch sample.Op {
			case STOP:
				// signal all done
				wg.Done()
				return
			default:
				stats[sample.Op].Update(sample, c.unit)
			}
		}
	}
}

func (c *configSpec) withIntervalSpacer(ctx context.Context, target int, alloc ConnectionAllocator, frequency Frequency, spacer time.Duration, ch chan<- timingSample, work func(context.Context, *clientv3.Client, string) error) (int, time.Duration) {
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
			cli, err := alloc.Get(ctx)
			if err != nil {
				log.Fatalf("ERROR: unable to allocate etcd connection: '%s'\n", err.Error())
			}
			endWait := time.Now()
			key := rand.Intn(c.KeyCount)
			err = work(ctx, cli, fmt.Sprintf("/test-%d", key))
			alloc.Put(cli)
			endTime := time.Now()
			cancel()
			if err != nil {
				log.Fatalf("ERROR: unable to complete operation: '%s', timeout is '%v' taking '%v' (%v/%v)\n",
					err.Error(), c.EtcdTimeout, endTime.Sub(startTime), endWait.Sub(startTime), endTime.Sub(endWait))
			}
			ch <- timingSample{
				Op:    target,
				Total: endTime.Sub(startTime),
				Wait:  endWait.Sub(startTime),
				Etcd:  endTime.Sub(endWait),
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

func (c *configSpec) noIntervalSpacer(ctx context.Context, target int, alloc ConnectionAllocator, frequency Frequency, ch chan<- timingSample, work func(context.Context, *clientv3.Client, string) error) (int, time.Duration) {
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
			cli, err := alloc.Get(ctx)
			if err != nil {
				log.Fatalf("ERROR: unable to allocate etcd connection: '%s'\n", err.Error())
			}
			endWait := time.Now()
			key := rand.Intn(c.KeyCount)
			err = work(ctx, cli, fmt.Sprintf("/test-%d", key))
			alloc.Put(cli)
			endTime := time.Now()
			cancel()
			if err != nil {
				log.Fatalf("ERROR: unable to complete operation: '%s', timeout is '%v' taking '%v' (%v/%v)\n",
					err.Error(), c.EtcdTimeout, endTime.Sub(startTime), endWait.Sub(startTime), endTime.Sub(endWait))
			}
			ch <- timingSample{
				Op:    target,
				Total: endTime.Sub(startTime),
				Wait:  endWait.Sub(startTime),
				Etcd:  endTime.Sub(endWait),
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

type ConnectionAllocator interface {
	Get(context.Context) (*clientv3.Client, error)
	Put(*clientv3.Client)
}

type ConnectionAllocatorFactory interface {
	Get() ConnectionAllocator
}

type SharedConnectionAllocator struct {
	cli *clientv3.Client
}

type UnaryConnectionAllocatorFactory struct {
	allocator ConnectionAllocator
}

func (f *UnaryConnectionAllocatorFactory) Get() ConnectionAllocator {
	return f.allocator
}

func NewSharedConnectionAllocatorFactory(ch chan<- struct{}, endpoints []string, timeout time.Duration) ConnectionAllocatorFactory {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: timeout,
	})
	if err != nil {
		log.Fatalf("ERROR: unable to connect to KV store at '%s': '%s'\n", endpoints, err.Error())
	}
	ch <- struct{}{}
	return &UnaryConnectionAllocatorFactory{
		allocator: &SharedConnectionAllocator{
			cli: cli,
		},
	}
}

func (a *SharedConnectionAllocator) Get(ctx context.Context) (*clientv3.Client, error) {
	return a.cli, nil
}

func (a *SharedConnectionAllocator) Put(cli *clientv3.Client) {
}

type SeparateConnectionAllocatorFactory struct {
	ch        chan<- struct{}
	endpoints []string
	timeout   time.Duration
}

func (f *SeparateConnectionAllocatorFactory) Get() ConnectionAllocator {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   f.endpoints,
		DialTimeout: f.timeout,
	})
	if err != nil {
		log.Fatalf("ERROR: unable to connect to KV store at '%s': '%s'\n", f.endpoints, err.Error())
	}
	f.ch <- struct{}{}

	return &SharedConnectionAllocator{
		cli: cli,
	}
}

func NewSeparateConnectionAllocatorFactory(ch chan<- struct{}, endpoints []string, timeout time.Duration) ConnectionAllocatorFactory {
	//. separate is a shared connection that is only shared with yourself
	return &SeparateConnectionAllocatorFactory{
		ch:        ch,
		endpoints: endpoints,
		timeout:   timeout,
	}
}

type RobinConnectionAllocator struct {
	sync.Mutex
	idx  int
	list []*clientv3.Client
}

func NewRobinConnectionAllocatorFactory(ch chan<- struct{}, endpoints []string, timeout time.Duration, capacity int) ConnectionAllocatorFactory {
	robin := &RobinConnectionAllocator{
		list: make([]*clientv3.Client, capacity),
	}

	for i := 0; i < capacity; i++ {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   endpoints,
			DialTimeout: timeout,
		})
		if err != nil {
			log.Fatalf("ERROR: unable to connect to KV store at '%s': '%s'\n",
				endpoints, err.Error())
		}
		ch <- struct{}{}
		robin.list[i] = cli
	}

	return &UnaryConnectionAllocatorFactory{
		allocator: robin,
	}
}

func (a *RobinConnectionAllocator) Get(context.Context) (*clientv3.Client, error) {
	a.Lock()
	defer a.Unlock()
	use := a.list[a.idx]
	a.idx++
	if a.idx >= len(a.list) {
		a.idx = 0
	}
	return use, nil
}

func (a *RobinConnectionAllocator) Put(cli *clientv3.Client) {}

type PooledConnectionAllocator struct {
	sync.Mutex
	pool chan *clientv3.Client

	ch        chan<- struct{}
	endpoints []string
	timeout   time.Duration
	capacity  int
	size      int
}

func NewPooledConnectionAllocatorFactory(ch chan<- struct{}, endpoints []string, timeout time.Duration, capacity int) ConnectionAllocatorFactory {
	return &UnaryConnectionAllocatorFactory{
		allocator: &PooledConnectionAllocator{
			pool:      make(chan *clientv3.Client, capacity),
			capacity:  capacity,
			ch:        ch,
			endpoints: endpoints,
			timeout:   timeout,
		},
	}
}

func (a *PooledConnectionAllocator) Get(ctx context.Context) (*clientv3.Client, error) {
	select {
	case cli := <-a.pool:
		return cli, nil
	default:
		a.Lock()
		if a.size < a.capacity {
			cli, err := clientv3.New(clientv3.Config{
				Endpoints:   a.endpoints,
				DialTimeout: a.timeout,
			})
			if err != nil {
				log.Fatalf("ERROR: unable to connect to KV store at '%s': '%s'\n",
					a.endpoints, err.Error())
			}
			a.ch <- struct{}{}
			a.size++
			a.Unlock()
			return cli, nil
		}
		a.Unlock()
	}

	var cli *clientv3.Client
	select {
	case cli = <-a.pool:
		return cli, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (a *PooledConnectionAllocator) Put(cli *clientv3.Client) {
	a.pool <- cli
}

func (c *configSpec) runWorker(ctx context.Context, target int, frequency Frequency, alloc ConnectionAllocator,
	ch chan<- timingSample, wg *sync.WaitGroup, work func(context.Context, *clientv3.Client, string) error) {
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
				count, left = c.withIntervalSpacer(ctx, target, alloc, frequency, spacer, ch, work)
			} else {
				count, left = c.noIntervalSpacer(ctx, target, alloc, frequency, ch, work)
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
