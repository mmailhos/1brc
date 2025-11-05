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

func opt() {
	debug.SetGCPercent(-1)
	f, err := os.Open(FILEPATH)
	if err != nil {
		log.Fatal(err)
	}
	r := bufio.NewReader(f)

	for {
		line, err := r.ReadSlice('\n')
		if err == io.EOF {
			if len(line) > 0 {
				parseLine(line)
			}
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		n, v, e := parseLine(line)
		fmt.Println(n, v, e)
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
	start := time.Now()
	naive()
	elapsed := time.Since(start)
	fmt.Printf("Finished in: %s\n", elapsed)
}
