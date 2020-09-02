package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
)

// Reduce will reduce
func Reduce(files []string) (map[string]string, error)  {
	m := make(map[string]string)
	for _, filename := range files {
		f, err := os.Open(filename)
		if err != nil {
			fmt.Println("open file err: ", err)
			return m, err
		}

		// filename: sdfs_intermediate_prefix_0/temp_intermediate_key
		// get the lastUnderscorePos to extract the key from file name
		lastUnderscorePos := 0
		for i := len(filename) - 1; i >= 0; i-- {
			if filename[i] == '_' {
				lastUnderscorePos = i
				break
			}
		}
		k := filename[lastUnderscorePos + 1:]
		v := 0

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			val, _ := strconv.Atoi(scanner.Text())
			v += val
		}
		m[k] = strconv.Itoa(v)
		f.Close()
	}
	return m, nil
}
