package main

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

type Mapped struct {
	Id     string    `json:"id"`
	Time   time.Time `json:"time"`
	Value  string    `json:"value"`
	Status string    `json:"status"`
}

func read(file *os.File, wg *sync.WaitGroup, ch chan<- string) {
	defer wg.Done()

	scanner := bufio.NewScanner(file)
	var mapped []Mapped

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, ";")

		time, _ := time.Parse(time.RFC3339, fields[1])

		m := Mapped{
			Id:     fields[0],
			Time:   time,
			Value:  fields[2],
			Status: fields[3],
		}

		mapped = append(mapped, m)
	}

	j, _ := json.MarshalIndent(mapped, "", " ")

	ch <- string(j)
	close(ch)
}

func write(wg *sync.WaitGroup, ch <-chan string) {
	defer wg.Done()

	output, _ := os.Create("output.json")

	defer output.Close()

	writer := bufio.NewWriter(output)

	defer writer.Flush()

	for value := range ch {
		_, err := writer.WriteString(value + "\n")
		if err != nil {
			log.Println(err)
		}
	}

}

func main() {
	var wg sync.WaitGroup
	dataCh := make(chan string, 10000)
	file, _ := os.Open("dataset.csv")

	defer file.Close()

	wg.Add(3)
	go read(file, &wg, dataCh)
	go write(&wg, dataCh)
	go write(&wg, dataCh)

	wg.Wait()

}
