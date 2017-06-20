package main

import (
	goflag "flag"
	"runtime"
	"github.com/turbonomic/mesosturbo/cmd/service"
	"github.com/golang/glog"
	"github.com/spf13/pflag"
)

func init() {
	goflag.Set("logtostderr", "true")
}

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())
	glog.V(2).Infof("*** Run Mesosturbo service ***")

	s := service.NewMesosTurboService()
	s.AddFlags(pflag.CommandLine)
	pflag.CommandLine.AddGoFlagSet(goflag.CommandLine)
	pflag.Parse()

	s.Run(pflag.CommandLine.Args())

} //end main
