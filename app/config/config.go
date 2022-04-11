package config

import (
	"flag"
	"github.com/go-yaml/yaml"
	"io/ioutil"
	"log"
	"time"
)

// MyConfig Config mysql config.
type (
	MyConfig struct {
		Address       string        `yaml:"Address"`       // write data source name. (dsn without database name
		Params        string        `yaml:"Params"`        // DSN params
		DefaultDbname string        `yaml:"DefaultDbname"` // default mysql database name
		DbNames       []string      `yaml:"DbNames"`       // all db names except default
		Active        int           `yaml:"Active"`        // pool
		Idle          int           `yaml:"Idle"`          // pool
		RowLimit      int           `yaml:"RowLimit"`      // limit of row numbers in a process
		IdleTimeout   time.Duration `yaml:"IdleTimeout"`   // connect max lifetime.
		QueryTimeout  time.Duration `yaml:"QueryTimeout"`  // query sql timeout
		ExecTimeout   time.Duration `yaml:"ExecTimeout"`   // execute sql timeout
		TranTimeout   time.Duration `yaml:"TranTimeout"`   // transaction sql timeout
		ExtraDatatype string        `yaml:"ExtraDatatype"` // extra finance datatype (divided by ',' like 262763,262764
		// Breaker      *breaker.Config // breaker
	}

	PgConfig struct {
		DefaultDSN   string `yaml:"DefaultDSN"`   // postgres default database connect dsn
		QueryTimeout int    `yaml:"QueryTimeout"` // time out of pg query
		MaxIdleConns int    `yaml:"MaxIdleConns"` // max number of idles existed
		MaxOpenConns int    `yaml:"MaxOpenConns"` // max number of idles opened
		LogLevel     string `yaml:"LogLevel"`     // log level of pg connection
	}

	ServiceConfig struct {
		HttpPort int `yaml:"HttpPort"` // http port
	}

	LogConfig struct {
		LogPath     string `yaml:"LogPath"`     // log file path
		StatLogPath string `yaml:"StatLogPath"` // status log file path
		GinLogPath  string `yaml:"GinLogPath"`  // gin log file path
		LogLevel    string `yaml:"LogLevel"`    // log level
	}

	Config struct {
		Mysql   MyConfig      `yaml:"Mysql"`   // mysql configure
		Pgsql   PgConfig      `yaml:"Pgsql"`   // pgsql configure
		Service ServiceConfig `yaml:"Service"` // service configure
		Log     LogConfig     `yaml:"Log"`     // log configure
	}
)

var (
	cfgFile *string
	cfg     Config
)

/*ConfigureInit
 * @Description: 获取配置
 */
func ConfigureInit() {
	// TODO: negt-go 配置模块接入
	// 注释中为线上环境配置
	//cfgFile = flag.String("f", "/usr/local/conf/conf.yaml", "config file path")
	cfgFile = flag.String("f", "D:\\workspace\\go\\202203\\hxextract\\conf\\conf.yaml",
		"config file path")
	flag.Parse()
	content, err := ioutil.ReadFile(*cfgFile)
	if yaml.Unmarshal(content, &cfg) != nil {
		log.Fatalf("解析config.yaml出错: %v", err)
	}
}

func GetMysql() MyConfig {
	return cfg.Mysql
}

func GetPgsql() PgConfig {
	return cfg.Pgsql
}

func GetService() ServiceConfig {
	return cfg.Service
}

func GetLog() LogConfig {
	return cfg.Log
}
