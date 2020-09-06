package main

//
// start the master process, which is implemented
// in ../mr/master.go
//
// go run mrmaster.go pg*.txt
//
// Please do not change this file.
//

import (
	"../mr"
	"io/ioutil"
	"os"
	"strings"
)
import "time"

func main() {
	//if len(os.Args) < 2 {
	//	fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
	//	os.Exit(1)
	//}

	basePath := "C:\\Users\\hy\\Desktop\\自学课程\\MIT6.824\\LABS\\mine\\src\\main"
	files, _ := ioutil.ReadDir(basePath)
	fs := []string{}
	for _, f := range files {
		fn := f.Name()

		if strings.Split(fn, "-")[0] == "pg" {
			fs = append(fs, basePath+string(os.PathSeparator)+fn)
		}
	}
	m := mr.MakeMaster(fs, 10)

	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
