package main

import (
         "fmt"
         "flag"
         "io"
         "os"
         "encoding/json"
         "net/http"
         "log"
         "github.com/kscharpf/querysplit"
         "github.com/kscharpf/csv"
         "time"
         "math/rand"
         "io/ioutil"
         "bytes"
)

var querySplitter querysplit.QuerySplitter

func queryHandler(w http.ResponseWriter, req *http.Request) {

  queries, fields := querySplitter.Split(req.URL.RawQuery)

  fmt.Printf("Identified fields in query: %v len %v\n", fields, len(fields))
  fmt.Printf("Identified queries in query: %v\n", queries)

  tarray := []string{config.IndexField}
  for i := range fields {
    tarray = append(tarray, fields[i])
  }

  m := csv.NewCsvMatrix(tarray)

  var chans []chan string 
  for i := 0; i<len(queries); i++ {
    chans = append(chans, make(chan string))
  }

  for i:= range chans {
    go func(c chan string, q string, ival int64) {
      for {
        time.Sleep(time.Duration(rand.Int63n(ival)) * time.Second)

        fmt.Printf("Issuing query: %v\n", q)
        resp, err := http.Get(q)
        if err == nil {
          defer resp.Body.Close()
          body, err := ioutil.ReadAll(resp.Body)
          if err == nil {
            buffer := bytes.NewBuffer(body)
            c <- buffer.String()
            break
          }
        } 
      }
    } (chans[i], queries[i], int64(len(queries)/2 + 1))
  }

  for i:=range chans {

    s := <-chans[i]

    m.AppendCsv(s)
  }
  b := bytes.NewBufferString(m.DumpCsv()) 
  _, err := w.Write(b.Bytes())
  if err != nil {
    fmt.Printf("Error(%v) during http reply\n", err)
  }
}

type Config struct {
  Url string
  Handler string
  Splitkey string
  Port     int
  IndexField string
  OtherKeys []string
} 

var config Config

func main() {
  var cfgFile = flag.String("config-file", "config.json", "Path definining the location of a configuration file")
  flag.Parse()

  fin, err := os.Open(*cfgFile)

  if err != nil { panic(err) }

  buf := make([]byte, 1024)
  n, err := fin.Read(buf)
  if err != nil && err != io.EOF { panic(err) }

  if n == 0 {
    panic(fmt.Sprintf("Configuration file: %v not read", *cfgFile))
  }

  err = json.Unmarshal(buf[:n], &config)
  if err != nil {
    panic(fmt.Sprintf("Error(%s) reading json file %v", err, *cfgFile))
  }

  fmt.Printf("Executing http-csv metaserver with config %v\n", config)

  querySplitter = querysplit.NewQuerySplitter(fmt.Sprintf("%s/%s?", config.Url, config.Handler), config.Splitkey, config.OtherKeys)

  ps := fmt.Sprintf(":%d", config.Port)
  http.HandleFunc(config.Handler, queryHandler)
  log.Fatal(http.ListenAndServe(ps, nil))
}
