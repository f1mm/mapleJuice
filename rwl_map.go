package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// MapRwl will map
func MapRwl(files []string) ([][]string, error) {
	pairs := [][]string{}
	for _, file := range files {
		f, err := os.Open(file)		
		if err != nil {
			fmt.Println("open file err: ", err)
			return [][]string{}, err
		}
		
		scanner := bufio.NewScanner(f)
		count := 0
		m := make(map[string][]string)
		for scanner.Scan() {
			edge := strings.Fields(scanner.Text())
			m[edge[1]] = append(m[edge[1]], edge[0])

			count++
			if count == 10 {
				for k, v := range m {
					p := []string{k, strings.Join(v, ", ")}
					pairs = append(pairs, p)
				}

				count = 0
				m = make(map[string][]string)
			}
		}
		
		if count != 0{
			for k, v:= range m {
				p := []string{k, strings.Join(v, ", ")}
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