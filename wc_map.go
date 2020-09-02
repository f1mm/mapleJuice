package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"strconv"
)

// Map will map
func Map(files []string) ([][]string, error) {
	pairs := [][]string{}
	for _, file := range files {
		f, err := os.Open(file)		
		if err != nil {
			fmt.Println("open file err: ", err)
			return [][]string{}, err
		}

		scanner := bufio.NewScanner(f)
		count := 0
		m := make(map[string]int)
		for scanner.Scan() {
			words := strings.Fields(scanner.Text())
			for _, w := range words {
				m[w]++
			}

			count++
			if count == 10 {
				for k, v:= range m {
					p := []string{k, strconv.Itoa(v)}
					pairs = append(pairs, p)
				}

				count = 0
				m = make(map[string]int)
			}
		}
		
		if count != 0{
			for k, v:= range m {
				p := []string{k, strconv.Itoa(v)}
				pairs = append(pairs, p)
			}
		}

		if err := scanner.Err(); err != nil{
			fmt.Println("scanner err: ", err)
			return [][]string{}, err
		}
		f.Close()
	}
	return pairs, nil
}