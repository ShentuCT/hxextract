package main

import (
	"hxextract/app/config"
	"hxextract/app/di"
	lg "hxextract/app/log"
)

func main() {
	config.ConfigureInit()
	lg.InitLog()
	lg.Log.Info("Start up")
	app, cleanup, err := di.InitApp()
	if err != nil {
		lg.Log.Fatal(err.Error())
	}
	err = app.Start()
	if err != nil {
		lg.Log.Fatal(err.Error())
	}
	cleanup()
}
