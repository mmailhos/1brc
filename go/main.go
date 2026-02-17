package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"time"
)

const FILEPATH = "../data/weather_stations.csv"

func parseLine(b []byte) (name []byte, val float64, err error) {
	sep := byte(';')

	i := bytes.IndexByte(b, sep)
	if i < 0 {
		return nil, 0, fmt.Errorf("invalid row: %q", string(b))
	}

	name = b[:i]
	numBytes := bytes.TrimSpace(b[i+1:])

	// An implementation like fastfloat would be even faster:
	// https://github.com/valyala/fastjson/blob/v1.6.4/fastfloat/parse.go#L361
	val, err = strconv.ParseFloat(string(numBytes), 64)
	return
}

type CityStats struct {
	sum     float64
	minTemp float64
	maxTemp float64
	count   int
}

type HashMap struct {
	keys      [][]byte
	values    []CityStats
	keyBuffer []byte
	size      int
	cap       int
}

func NewHashMap(capacity int) *HashMap {
	cap := 1
	for cap < capacity {
		cap <<= 1
	}
	return &HashMap{
		keys:      make([][]byte, cap),
		values:    make([]CityStats, cap),
		keyBuffer: make([]byte, 0, 1024*1024),
		cap:       cap,
	}
}

func (h *HashMap) hash(key []byte) int {
	// 64-bit FNV-1a offset basis. Init the hash state
	const fnvOffset uint64 = 14695981039346656037
	// 64-bit FNV prime. Mixes the hash state after each byte
	const fnvPrime uint64 = 1099511628211

	hash := fnvOffset
	for i := 0; i < len(key); i++ {
		hash ^= uint64(key[i])
		hash *= fnvPrime
	}
	return int(hash) & (h.cap - 1)
}

func (h *HashMap) keysEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (h *HashMap) GetOrCreate(key []byte) *CityStats {
	if h.size >= h.cap {
		log.Fatal("hashmap full")
	}
	idx := h.hash(key)
	startIdx := idx

	for {
		if h.keys[idx] == nil {
			start := len(h.keyBuffer)
			h.keyBuffer = append(h.keyBuffer, key...)
			h.keys[idx] = h.keyBuffer[start:len(h.keyBuffer)]
			h.size++
			return &h.values[idx]
		}
		if h.keysEqual(h.keys[idx], key) {
			return &h.values[idx]
		}
		idx = (idx + 1) & (h.cap - 1)
		if idx == startIdx {
			log.Fatal("hashmap full")
		}
	}
}

func (h *HashMap) Iterate() []struct {
	key   []byte
	value *CityStats
} {
	result := make([]struct {
		key   []byte
		value *CityStats
	}, 0, h.size)
	for i := range h.keys {
		if h.keys[i] != nil {
			result = append(result, struct {
				key   []byte
				value *CityStats
			}{h.keys[i], &h.values[i]})
		}
	}
	return result
}
func opt() {
	debug.SetGCPercent(-1)
	f, err := os.Open(FILEPATH)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	// Alocate a new hashmap bigger than the file
	hm := NewHashMap(100000)

	for {
		line, err := r.ReadBytes('\n')
		if err == io.EOF {
			if len(line) > 0 {
				name, val, e := parseLine(line)
				if e == nil {
					stats := hm.GetOrCreate(name)
					stats.count++
					stats.sum += val
					stats.maxTemp = max(stats.maxTemp, val)
					stats.minTemp = min(stats.minTemp, val)
				}
			}
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		name, val, e := parseLine(line)
		if e != nil {
			continue
		}

		stats := hm.GetOrCreate(name)
		stats.count++
		stats.sum += val
		stats.maxTemp = max(stats.maxTemp, val)
		stats.minTemp = min(stats.minTemp, val)
	}

	entries := hm.Iterate()
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})

	for _, entry := range entries {
		mean := entry.value.sum / float64(entry.value.count)
		fmt.Printf("%s=%.1f/%.1f/%.1f\n", entry.key, entry.value.minTemp, mean, entry.value.maxTemp)
	}
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func naive() {
	f, err := os.Open(FILEPATH)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.Comma = ';'

	type City struct {
		name    string
		sum     float64
		minTemp float64
		maxTemp float64
		count   int
	}

	cities := make(map[string]*City)

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		name := record[0]
		valueStr := record[1]
		value, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			log.Fatal(err)
		}

		if city, ok := cities[name]; ok {
			city.count++
			city.sum += value
			city.maxTemp = max(city.maxTemp, value)
			city.minTemp = min(city.minTemp, value)
		} else {
			cities[name] = &City{
				name:    name,
				sum:     value,
				minTemp: value,
				maxTemp: value,
				count:   1,
			}
		}
	}

	sortedCities := make([]*City, 0, len(cities))
	for _, city := range cities {
		sortedCities = append(sortedCities, city)
	}

	sort.Slice(sortedCities, func(i, j int) bool {
		return sortedCities[i].name < sortedCities[j].name
	})

	for _, city := range sortedCities {
		mean := city.sum / float64(city.count)
		fmt.Printf("%s=%.1f/%.1f/%.1f\n", city.name, city.minTemp, mean, city.maxTemp)
	}
}

func main() {
	const runs = 20
	var total time.Duration

	for i := 1; i <= runs; i++ {
		start := time.Now()
		opt()
		elapsed := time.Since(start)
		fmt.Printf("Run %d finished in: %s\n", i, elapsed)
		total += elapsed
	}

	avg := total / runs
	fmt.Printf("Average time over %d runs: %s\n", runs, avg)
}
