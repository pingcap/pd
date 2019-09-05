package analysis

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
)

// Interpreter is the interface for all analysis to parse log
type Interpreter interface {
	CompileRegex(operator string) *regexp.Regexp
	parseLine(content string, r *regexp.Regexp) []uint64
	ParseLog(filename string, r *regexp.Regexp)
}

// CompileRegex is to provide regexp for transfer region counter.
func (TransferRegionCount) CompileRegex(operator string) *regexp.Regexp {
	var r *regexp.Regexp
	var err error

	var supportOperators = []string{"balance-region", "balance-leader", "transfer-hot-read-leader", "move-hot-read-region", "transfer-hot-write-leader", "move-hot-write-region"}
	for _, supportOperator := range supportOperators {
		if operator == supportOperator {
			r, err = regexp.Compile(".*?operator finish.*?region-id=([0-9]*).*?" + operator + ".*?store ([0-9]*) to ([0-9]*).*?")
		}
	}

	if err != nil {
		log.Fatal("Error: ", err)
	}
	if r == nil {
		log.Fatal("Error operator: ", operator, ". Support operators:", supportOperators)
	}
	return r
}

func (TransferRegionCount) parseLine(content string, r *regexp.Regexp) []uint64 {
	resultUint64 := make([]uint64, 0, 4)
	result := r.FindStringSubmatch(content)
	if len(result) == 0 {
		return resultUint64
	} else if len(result) == 4 {
		for i := 1; i < 4; i++ {
			num, err := strconv.ParseInt(result[i], 10, 64)
			if err != nil {
				log.Fatal("Error: ", err)
			}
			resultUint64 = append(resultUint64, uint64(num))
		}
	} else {
		log.Fatal("Parse Log Error, with", content)
	}
	return resultUint64
}

// ParseLog is to parse log for transfer region counter.
func (c *TransferRegionCount) ParseLog(filename string, r *regexp.Regexp) {
	// Open file
	fi, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	defer fi.Close()
	br := bufio.NewReader(fi)
	for {
		// Read content
		content, _, err := br.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error: %s\n", err)
			return
		}
		// Regex for each line
		result := c.parseLine(string(content), r)
		if len(result) == 3 {
			regionID, sourceID, targetID := result[0], result[1], result[2]
			GetTransferRegionCounter().AddTarget(regionID, targetID)
			GetTransferRegionCounter().AddSource(regionID, sourceID)
		}
	}
}
