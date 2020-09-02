package main

import (
	"bufio"
	"fmt"
	"os"
)

// ReduceRwl will reduce
func ReduceRwl(files []string) (map[string]string, error)  {
	m := make(map[string]string)
	for _, filename := range files {
		f, err := os.Open(filename)		
		if err != nil {
			fmt.Println("open file err: ", err)
			return m, err
		}

		lastUnderscorePos := 0
		for i := len(filename) - 1; i >= 0; i-- {
			if filename[i] == '_' {
				lastUnderscorePos = i
				break
			}
		}
		k := filename[lastUnderscorePos + 1:]
		v := ""

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			val := scanner.Text()
			v = v + val + ", "
		}
		m[k] = v
		f.Close()
	}
	return m, nil
}