package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	//"controller"
	"datanode"
	"utils/config"
)

const (
	Version     = "1.1"
	LogicalCPUs = 32
)

var (
	configFile = flag.String("c", "", "config file path")
	logLevel   = flag.Int("log", 0, "log level, as DebugLevel = 0")
)

type IServer interface {
	Start(cfg *config.Config) error
	Shutdown()
}

func interceptSignal(s IServer) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		s.Shutdown()
		os.Exit(0)
	}()
}

func main() {
	log.Println("Hello, Jingdong Filesystem")

	flag.Parse()
	cfg := config.LoadConfigFile(*configFile)
	role := cfg.GetString("role")
	profPort := cfg.GetString("prof")

	//for multi-cpu scheduling
	runtime.GOMAXPROCS(runtime.NumCPU())

	//init profile server
	go func() {
		log.Println(http.ListenAndServe(":"+profPort, nil))
	}()

	var server IServer

	switch role {
	case "master":
		server = master.NewServer()
	case "datanode":
		server = datanodeIdc.NewServer()
	default:
		log.Println("Fatal: unmath role: ", role)
		return
	}

	//install the signal handler
	interceptSignal(server)

	//start the server
	err := server.Start(cfg)
	if err != nil {
		log.Fatal("Fatal: failed to start the jfs daemon - ", err)
	}
}
