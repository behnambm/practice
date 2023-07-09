package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type Speech struct {
	Speaker string
	Topic   string
	Date    string
	Words   int
}

type Task struct {
	Error error

	SpeechValue []Speech
}

type Response struct {
	MostSpeeches string `json:"mostSpeeches"`
	MostSecurity string `json:"mostSecurity"`
	LeastWordy   string `json:"leastWordy"`
}

func main() {
	http.HandleFunc("/evaluation", evaluation)
	http.ListenAndServe(":8000", nil)
}

func evaluation(w http.ResponseWriter, req *http.Request) {
	out := make(chan Task)
	total := 0

	for _, v := range req.URL.Query() {
		if reflect.TypeOf(v) == reflect.TypeOf([]string{}) {
			for _, url := range v {
				go Process(out, url)
				total++
			}
		}
	}

	fmt.Println("Total: ", total)

	var tasks []Task
	for i := 0; i < total; i++ {
		select {
		case v, ok := <-out:
			if ok {
				tasks = append(tasks, v)
			}
		case <-time.After(time.Second * 10):
			fmt.Println("TIME OUT ")
		}
	}

	var allSpeeches []Speech
	for _, task := range tasks {
		if task.Error == nil {
			allSpeeches = append(allSpeeches, task.SpeechValue...)
		}
	}

	mostIn2013, err := ExtractMostSpeechesInYear(allSpeeches, 2013)
	if err != nil {
		log.Println("YEAR EXTRACT ERROR: ", err)
	}

	mostInInternalSecurity, err := ExtractMostSpeechesInTopic(allSpeeches, "Internal Security")
	if err != nil {
		log.Println("TOPIC EXTRACT ERROR: ", err)
	}

	fewest, _ := ExtractFewestWords(allSpeeches)

	response := Response{
		MostSecurity: mostInInternalSecurity,
		MostSpeeches: mostIn2013,
		LeastWordy:   fewest,
	}
	responseData, _ := json.Marshal(response)
	w.Write(responseData)
}

func Process(out chan<- Task, url string) {
	//fmt.Println("GETTING THE DATA FROM URL ", url)
	resp, err := http.Get(url)
	if err != nil {
		out <- Task{Error: fmt.Errorf("ERROR IN PROCESS(DOWNLOAD): %w", err)}

		return
	}

	reader := csv.NewReader(resp.Body)
	reader.FieldsPerRecord = 4
	data, readErr := reader.ReadAll()
	if readErr != nil {
		out <- Task{Error: fmt.Errorf("ERROR IN PROCESS(READ): %w", readErr)}

		return
	}
	data = append(data[1:]) // skip the header of CSV

	var speeches []Speech
	for _, row := range data {
		words, convErr := strconv.Atoi(strings.TrimSpace(row[3]))
		if convErr != nil {
			log.Println("ERROR(WORDS convert to int)")
			continue
		}
		speeches = append(
			speeches,
			Speech{
				Speaker: strings.TrimSpace(row[0]),
				Topic:   strings.TrimSpace(row[1]),
				Date:    strings.TrimSpace(row[2]),
				Words:   words,
			},
		)
	}
	out <- Task{SpeechValue: speeches}
}

func ExtractMostSpeechesInYear(speeches []Speech, year int) (string, error) {
	speakers := make(map[string]int)

	for _, speech := range speeches {
		date := strings.TrimSpace(speech.Date)
		datetime, pErr := time.Parse("2006-01-02", date)
		if pErr != nil {
			log.Println("PARSE TIME ERROR - ", pErr)
			continue
		}

		if datetime.Year() == year {
			if _, ok := speakers[speech.Speaker]; ok {
				speakers[speech.Speaker]++
			} else {
				speakers[speech.Speaker] = 1
			}
		}
	}
	topSpeaker := GetTopInMap(speakers)

	return topSpeaker, nil
}

func ExtractMostSpeechesInTopic(speeches []Speech, topic string) (string, error) {
	speakers := make(map[string]int)

	for _, speech := range speeches {
		if speech.Topic == topic {
			if _, ok := speakers[speech.Speaker]; ok {
				speakers[speech.Speaker]++
			} else {
				speakers[speech.Speaker] = 1
			}
		}
	}
	topSpeaker := GetTopInMap(speakers)

	return topSpeaker, nil
}

func ExtractFewestWords(speeches []Speech) (string, error) {
	speakers := make(map[string]int)

	for _, speech := range speeches {
		if _, ok := speakers[speech.Speaker]; ok {
			speakers[speech.Speaker] += speech.Words
		} else {
			speakers[speech.Speaker] = speech.Words
		}
	}

	fewestWords := ""
	fewest := math.MaxInt64

	for k, v := range speakers {
		if v < fewest {
			fewestWords = k
			fewest = v
		}
	}

	return fewestWords, nil
}

func GetTopInMap(m map[string]int) string {
	topKey := ""
	topItemCount := 0
	for k, v := range m {
		if v > topItemCount {
			topKey = k
			topItemCount = v
		}
	}

	return topKey
}
