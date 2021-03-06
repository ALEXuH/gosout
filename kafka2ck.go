package main

import (
	"context"
	"encoding/json"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/Shopify/sarama"
	"github.com/jmoiron/sqlx"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/viper"
	"log"
	"math"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	systemConfig     SystemConfig
	clickhouseConfig ClickhouseConfig
	kafkaConfig      KafkaConfig
	filterConfig     FilterConfig
}

type match struct {
	Key       string
	Expr      string
	ExprValue string
}

type AddColumn struct {
	Matches     [][]match
	Column      string
	ColumnValue string
}

type SystemConfig struct {
	Cpu          int
	Memory       int
	ChannelCount int
	FilterCount  int
}

type FilterConfig struct {
	RenameColumn  map[string]string
	ConvertColumn []string
	AddExprColumn []*AddColumn
	Filter        [][]match
}

type ClickhouseConfig struct {
	Database           string
	TableName          string
	Fields             string
	Debug              bool
	UserName           string
	Password           string
	AltHosts           string
	BlockSize          int
	Lz4Compress        int
	Concurrent         int
	FlushSize          int
	FlushTime          int
	Location           string
	DateFormat         []string
	ConnectionLifeTime int
}

type KafkaConfig struct {
	Topic    string
	Broker   string
	threads  int
	Version  string
	ClientId string
	GroupId  string
	Offset   bool
}

type consumerHandler struct {
	ready chan bool
}

var logger = log.Default()

func (consumer *consumerHandler) Setup(sarama.ConsumerGroupSession) error {
	//consumer.ready <- true
	logger.Println("consumer ready ... ")
	return nil
}
func (consumer *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

var jsonConfig = jsoniter.Config{
	EscapeHTML:              true,
	MarshalFloatWith6Digits: true,
	UseNumber:               true,
}.Froze()

func (consumer *consumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		sess.MarkMessage(msg, "")
		var f interface{}
		err := jsonConfig.Unmarshal(msg.Value, &f)
		if err != nil {
			logger.Println("err parse")
			logger.Println(msg.Value)
		} else {
			m := f.(map[string]interface{})
			kafkaChannel <- m
		}
	}
	return nil
}

var kafkaChannel chan map[string]interface{}
var convertChannel chan []interface{}
var columns []*RowDesc
var con *sqlx.DB
var defaultMap = map[string]interface{}{"String": "", "Nullable(String)": nil,
	"Nullable(UInt64)": 0, "Nullable(UInt8)": 0, "Nullable(UInt16)": 0, "Nullable(UInt32)": 0, "Nullable(Int8)": 0, "Nullable(Int16)": 0, "Nullable(Int32)": 0, "Nullable(Int64)": 0, "UInt8": 0, "UInt16": 0, "UInt32": 0, "UInt64": 0, "Int8": 0, "Int16": 0, "Int32": 0, "Int64": 0,
	"Float32": 0.0, "Float64": 0.0, "Nullable(Float32)": 0.0, "Nullable(Float64)": 0.0,
	"DateTime": "1996-03-01 12:12:12", "Nullable(DateTime)": "1996-03-01 12:12:12",
}
var config *Config
var wg sync.WaitGroup
var location *time.Location

func init() {
	fmt.Println("start...")
	// ??????config????????????
	viper.SetConfigName("config")                 // name of config file (without extension)
	viper.SetConfigType("yaml")                   // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath(filepath.Dir(os.Args[0])) // optionally look for config in the working directory
	//viper.AddConfigPath("C:\\Users\\think\\go\\src\\awesomeProject1\\") // optionally look for config in the working directory
	logger.Println(" current run path:", filepath.Dir(os.Args[0]))
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}

	systemConfig := SystemConfig{
		Cpu:          viper.GetInt("System.Cpu"),
		Memory:       viper.GetInt("System.Memory"),
		ChannelCount: viper.GetInt("System.ChannelCount"),
		FilterCount:  viper.GetInt("System.FilterCount"),
	}
	runtime.GOMAXPROCS(systemConfig.Cpu)
	kafkaChannel = make(chan map[string]interface{}, systemConfig.ChannelCount)
	convertChannel = make(chan []interface{}, systemConfig.ChannelCount*2)

	clickhouseConfig := ClickhouseConfig{
		Database:           viper.GetString("Clickhouse.Database"),
		TableName:          viper.GetString("Clickhouse.TableName"),
		Fields:             viper.GetString("Clickhouse.Fields"),
		Debug:              viper.GetBool("Clickhouse.Debug"),
		UserName:           viper.GetString("Clickhouse.UserName"),
		Password:           viper.GetString("Clickhouse.Password"),
		AltHosts:           viper.GetString("Clickhouse.Hosts"),
		BlockSize:          viper.GetInt("Clickhouse.BlockSize"),
		Lz4Compress:        viper.GetInt("Clickhouse.Lz4Compress"),
		Concurrent:         viper.GetInt("Clickhouse.Concurrent"),
		FlushSize:          viper.GetInt("Clickhouse.FlushSize"),
		FlushTime:          viper.GetInt("Clickhouse.FlushTime"),
		Location:           viper.GetString("Clickhouse.Location"),
		DateFormat:         viper.GetStringSlice("Clickhouse.DateTimeFormat"),
		ConnectionLifeTime: viper.GetInt("Clickhouse.ConnectionLifeTime"),
	}
	kafkaConfig := KafkaConfig{
		Topic:    viper.GetString("Kafka.Topic"),
		Broker:   viper.GetString("Kafka.Broker"),
		threads:  viper.GetInt("Kafka.Topic"),
		Version:  viper.GetString("Kafka.Version"),
		ClientId: viper.GetString("Kafka.ClientId"),
		GroupId:  viper.GetString("Kafka.GroupId"),
		Offset:   viper.GetBool("Kafka.FromBeginning"),
	}

	add := make([]*AddColumn, 0)

	for k := range viper.GetStringMap("Filter.Add") {
		add = append(add, &AddColumn{
			Matches:     parseExpr(viper.GetStringSlice("Filter.Add." + k + ".match")),
			Column:      viper.GetString("Filter.Add." + k + ".column"),
			ColumnValue: viper.GetString("Filter.Add." + k + ".value"),
		})
	}

	var rename = make(map[string]string)
	for _, v := range viper.GetStringSlice("Filter.Rename") {
		rename[strings.Split(v, ":")[0]] = strings.Split(v, ":")[1]
	}
	filterConfig := FilterConfig{
		RenameColumn:  rename,
		ConvertColumn: viper.GetStringSlice("Filter.Convert"),
		AddExprColumn: add,
		Filter:        parseExpr(viper.GetStringSlice("Filter.match")),
	}
	config = &Config{
		systemConfig,
		clickhouseConfig,
		kafkaConfig,
		filterConfig,
	}
	logger.Println(config)
	location, err = time.LoadLocation(clickhouseConfig.Location)
	if err != nil {
		location, _ = time.LoadLocation("Asia/Shanghai")
	}
	host := strings.Split(clickhouseConfig.AltHosts, ",")
	//driver := fmt.Sprintf("tcp://%s?debug=%s&database=%s&compress=%d&block_size=%d",host[0],strconv.FormatBool(clickhouseConfig.Debug),clickhouseConfig.Database, clickhouseConfig.Lz4Compress, clickhouseConfig.BlockSize)
	driver := fmt.Sprintf("tcp://%s?debug=%s&database=%s&compress=%d&alt_hosts=%swrite_timeout=600&username=%s&password=%s", host[0], strconv.FormatBool(config.clickhouseConfig.Debug), config.clickhouseConfig.Database, config.clickhouseConfig.Lz4Compress, strings.Join(host, ","), config.clickhouseConfig.UserName, config.clickhouseConfig.Password)

	connect, err := sqlx.Open("clickhouse", driver)
	if err != nil {
		logger.Fatal("connect clickhouse err:", err)
	}
	con = connect
	if clickhouseConfig.Fields == "*" {
		retrieve(clickhouseConfig.Database, clickhouseConfig.TableName)
	}
	con.SetMaxOpenConns(20)
	con.SetMaxIdleConns(config.clickhouseConfig.Concurrent)
	con.SetConnMaxLifetime(time.Second * time.Duration(config.clickhouseConfig.ConnectionLifeTime))
}

func consumer(ctx context.Context) {
	defer wg.Done()
	KafkaConfig := sarama.NewConfig()
	KafkaConfig.Version = sarama.V0_10_2_1
	if v, err := sarama.ParseKafkaVersion(config.kafkaConfig.Version); err == nil {
		KafkaConfig.Version = v
	}
	KafkaConfig.ClientID = config.kafkaConfig.ClientId
	KafkaConfig.Consumer.Return.Errors = true

	if config.kafkaConfig.Offset {
		KafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		KafkaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	}
	KafkaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	KafkaConfig.Consumer.Group.Session = struct{ Timeout time.Duration }{Timeout: 180 * time.Second}
	KafkaConfig.Consumer.Group.Heartbeat = struct{ Interval time.Duration }{Interval: 60 * time.Second}

	client, err := sarama.NewConsumerGroup(strings.Split(config.kafkaConfig.Broker, ","),
		config.kafkaConfig.GroupId,
		KafkaConfig)
	defer func() {
		if err = client.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	if err != nil {
		logger.Printf("error create consumer group:brokers %s ", config.kafkaConfig.Broker, err.Error())
	}

	// kafka????????????
	threadCount := config.kafkaConfig.threads
	topic := config.kafkaConfig.Topic
	for i := 0; i <= threadCount; i++ {
		wg.Add(1)
		go func(ctx context.Context) {
			defer wg.Done()
			handler := consumerHandler{
				ready: make(chan bool),
			}
			for {
				err = client.Consume(ctx, strings.Split(topic, ","), &handler)
				if err != nil {
					logger.Println(err.Error(), err)
					//return
				}
				//select {
				//case <-ctx.Done():
				//	log.Println("consumer terminating: context cancelled")
				//	return
				//default:
				//	err = client.Consume(ctx, strings.Split(topic,","), &handler)
				//	if err != nil {
				//		fmt.Println(err.Error(), err)
				//	}
				//}
			}

		}(ctx)
	}

	select {
	case <-ctx.Done():
		log.Println("Kafka terminating: context cancelled")
		return
	}
}

func parseExpr(value []string) [][]match {
	result := make([][]match, 0)
	for _, v := range value {
		res := make([]match, 0)
		for _, v1 := range strings.Split(v, "&") {
			if strings.Contains(v1, "==") {
				v2 := strings.Split(v1, "==")
				res = append(res, match{
					Key:       strings.TrimSpace(v2[0]),
					Expr:      "==",
					ExprValue: strings.TrimSpace(v2[1]),
				})
			} else if strings.Contains(v1, "!=") {
				v2 := strings.Split(v1, "!=")
				res = append(res, match{
					Key:       strings.TrimSpace(v2[0]),
					Expr:      "!=",
					ExprValue: strings.TrimSpace(v2[1]),
				})
			} else if strings.Contains(v1, "in") {
				v2 := strings.Split(v1, "in")
				res = append(res, match{
					Key:       strings.TrimSpace(v2[0]),
					Expr:      "in",
					ExprValue: strings.TrimSpace(v2[1]),
				})
			} else if strings.Contains(v1, "regex") {
				v2 := strings.Split(v1, "regex")
				res = append(res, match{
					Key:       strings.TrimSpace(v2[0]),
					Expr:      "regex",
					ExprValue: strings.TrimSpace(v2[1]),
				})
			} else {
				logger.Println("expr do not support ", v1)
				continue
			}
		}
		if len(res) != 0 {
			result = append(result, res)
		}
	}
	return result
}

func isIn(str string, value string) bool {
	arr := strings.Split(str, ",")
	for _, v := range arr {
		if value == v {
			return true
		}
	}
	return false
}

func isFilter(msg map[string]interface{}, filters *[][]match) bool {
	if len(*filters) == 0 {
		return true
	}
	for _, v := range *filters {
		flag := true
		for _, filter := range v {
			switch filter.Expr {
			case "==":
				if msg[filter.Key] == nil {
					flag = false
					break
				} else {
					if interface2String(msg[filter.Key]) == filter.ExprValue {
						continue
					} else {
						flag = false
						break
					}
				}
			case "!=":
				if msg[filter.Key] == nil {
					flag = false
					break
				} else {
					if interface2String(msg[filter.Key]) != filter.ExprValue {
						continue
					} else {
						flag = false
						break
					}
				}
			case "in":
				if msg[filter.Key] == nil {
					flag = false
					break
				} else {
					if isIn(filter.ExprValue, interface2String(msg[filter.Key])) {
						continue
					} else {
						flag = false
						break
					}
				}
			case "regex":
				if msg[filter.Key] == nil {
					flag = false
					break
				} else {
					if f, _ := regexp.MatchString(filter.ExprValue, interface2String(msg[filter.Key])); f {
						continue
					} else {
						flag = false
						break
					}
				}
			}
		}
		if flag {
			return true
		}
	}
	return false
}

func interface2String(inter interface{}) string {
	switch inter.(type) {
	case string:
		return inter.(string)
	case json.Number:
		return inter.(json.Number).String()
	case float64:
		return strconv.FormatFloat(inter.(float64), 'f', 0, 64)
	case int:
		return strconv.FormatInt(inter.(int64), 10)
	}
	return ""
}

// filter
func filer(ctx context.Context) {
	defer wg.Done()
	for i := 0; i <= config.systemConfig.FilterCount; i++ {
		wg.Add(1)
		go func() {
			for {
				select {
				case msg := <-kafkaChannel:
					// map??????????????????????????? 1 a ???/?????????/??????????????????/string????????????
					// ????????????
					//fmt.Println(msg)
					if !isFilter(msg, &config.filterConfig.Filter) {
						//logger.Println("filter msg ...")
						continue
					}
					// ????????????????????????
					for _, v := range config.filterConfig.AddExprColumn {
						if len(v.Matches) == 0 {
							msg[v.Column] = v.ColumnValue
						} else {
							if isFilter(msg, &v.Matches) {
								msg[v.Column] = v.ColumnValue
							}
						}
					}
					// ??????ckTime??????
					msg["ckDate"] = time.Now().Format("2006-01-02 15:04:05")
					//msg["ckDate"] = time.Now()
					// ?????????
					for k, v := range config.filterConfig.RenameColumn {
						msg[v] = msg[k]
					}
					// ????????????(???????????????string)
					for _, v := range config.filterConfig.ConvertColumn {
						msg[v] = interface2String(msg[v])
					}
					//fmt.Println(msg)
					// ???????????????????????? ,????????????????????????
					args := make([]interface{}, len(columns))
					for ix, v := range columns {
						name := v.Name
						value := msg[name]
						if value == nil {
							args[ix] = defaultMap[v.Type]
						} else {
							switch v.Type {
							case "String", "Nullable(String)":
								switch value.(type) {
								case json.Number:
									args[ix] = value.(json.Number).String()
								case string:
									args[ix] = value
								}
							case "Nullable(UInt32)", "UInt32", "Nullable(UInt8)", "Nullable(UInt64)", "Nullable(UInt16)", "Nullable(Int8)", "Int16", "UInt8", "UInt64", "UInt16", "Int8", "Int64":
								switch value.(type) {
								case json.Number:
									if va, err := value.(json.Number).Int64(); err == nil {
										args[ix] = va
									} else {
										logger.Println(name, "warning int parse err... value: ", name, ":", value)
									}
								case string:
									if vt, err := strconv.ParseInt(value.(string), 10, 64); err != nil {
										logger.Println(name, "warning int parse err... value: ", name, ":", value)
										args[ix] = 0
									} else {
										args[ix] = vt
									}
								}
							case "Float32", "Float64", "Nullable(Float32)", "Nullable(Float64)":
								switch value.(type) {
								case json.Number:
									if va, err := value.(json.Number).Float64(); err == nil {
										args[ix] = va
									} else {
										logger.Println(name, "warning int parse err... value: ", name, ":", value)
									}
								case string:
									if vt, err := strconv.ParseFloat(value.(string), 64); err != nil {
										logger.Println(name, "warning float parse err... value: ", name, ":", value)
										args[ix] = vt
									} else {
										args[ix] = 0.0
									}
								}
							case "DateTime", "Nullable(DateTime)":
								args[ix] = "1996-03-01 12:12:12"
								//logger.Println(reflect.TypeOf(value))
								//args[ix] = time.Now().Format("2006-01-02 15:04:05")
								switch value.(type) {
								case json.Number:
									value = value.(json.Number).String()
								}
								for _, layout := range config.clickhouseConfig.DateFormat {
									if layout == "UNIX_MS" {
										if t1, err := ParseTimeStampMs(value); err == nil {
											args[ix] = t1
											break
										}
									} else if layout == "UNIX" {
										if t1, err := ParseTimeStamp(value); err == nil {
											args[ix] = t1
											break
										}
									} else {
										if t1, err := time.ParseInLocation(layout, value.(string), location); err == nil {
											args[ix] = t1
											break
										}
									}
								}
								if args[ix] == "1996-03-01 12:12:12" {
									logger.Println("warning dateTime parse error", name, ":", value)
								}
							default:
								logger.Panicln(v.Type, " CURRENT type not support ...")
							}
						}
					}
					convertChannel <- args
				case <-ctx.Done():
					logger.Println("filter over ...")
					return
				}
			}
		}()
	}
}

// ??????ck
func sink2ck(ctx context.Context) {
	for i := 0; i <= config.clickhouseConfig.Concurrent; i++ {
		wg.Add(1)
		go exec(ctx)
	}
	defer wg.Done()
	select {
	case <-ctx.Done():
		log.Println("clickhouse terminating: context cancelled")
		return
	}
}

// ????????????????????????
func exec(ctx context.Context) {
	defer wg.Done()
	//fmt.Println("exec sql start")
	var fieldsName = make([]string, len(columns))
	var fieldValue = make([]string, len(columns))
	for ix, rowDesc := range columns {
		fieldsName[ix] = rowDesc.Name
		fieldValue[ix] = "?"
	}
	fields := strings.Join(fieldsName, ",")

	//logger.Println(fields)
	logger.Println("start clickhouse sink ...")
	//defer func(con *sqlx.DB) {
	//	err := con.Close()
	//	if err != nil {
	//		logger.Println("connect close error...", err)
	//	}}
	//}(con)
	//
	defer func() {
		if r := recover(); r != nil {
			host := strings.Split(config.clickhouseConfig.AltHosts, ",")
			driver := fmt.Sprintf("tcp://%s?debug=%s&database=%s&compress=%d&alt_hosts=%swrite_timeout=600&username=%s&password=%s", host[0], strconv.FormatBool(config.clickhouseConfig.Debug), config.clickhouseConfig.Database, config.clickhouseConfig.Lz4Compress, strings.Join(host, ","), config.clickhouseConfig.UserName, config.clickhouseConfig.Password)
			for {
				time.Sleep(2 * time.Second)
				con, _ = sqlx.Open("clickhouse", driver)
				con.SetMaxOpenConns(20)
				con.SetMaxIdleConns(config.clickhouseConfig.Concurrent)
				con.SetConnMaxLifetime(time.Second * time.Duration(config.clickhouseConfig.ConnectionLifeTime))
				if err := con.Ping(); err == nil {
					wg.Add(1)
					go exec(ctx)
					break
				} else {
					logger.Println("connect error: ", err)
				}
			}
		}
	}()
	tx, err := con.Begin()
	sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s)", config.clickhouseConfig.TableName, fields, strings.Join(fieldValue, ","))

	if err != nil {
		logger.Println("db begin create transaction error %s", err)
	}
	//logger.Println(sql)
	st, err := tx.Prepare(sql)

	if err != nil {
		logger.Println(fmt.Sprintf("prepare sql err %s", err))
	}
	defer st.Close()

	countFlush := 0
	t := time.NewTicker(time.Duration(config.clickhouseConfig.FlushTime) * time.Second)
	for {
		select {
		case msg := <-convertChannel:
			//fmt.Println("bbb", len(t.C))
			if _, err := st.Exec(msg...); err != nil {
				logger.Println(fmt.Sprintf(" exec error %s", err))
			}
			countFlush += 1
			if countFlush >= config.clickhouseConfig.FlushSize {
				if err := tx.Commit(); err != nil {
					fmt.Println(err)
					logger.Println(err)
				}
				logger.Println(fmt.Sprintf("Count Flush count:%d has commit", countFlush))
				if tx, err = con.Begin(); err != nil {
					logger.Println(fmt.Sprintf("begin sql err %s", err))
				}
				if st, err = tx.Prepare(sql); err != nil {
					logger.Println(fmt.Sprintf("st sql err %s", err))
				}
				countFlush = 0
			}
		case <-ctx.Done():
			logger.Println("clickhouse exec closed")
			return
		case <-t.C:
			if err := tx.Commit(); err != nil {
				fmt.Println(err)
				logger.Println(err)
			}
			logger.Println(fmt.Sprintf("Time FLush count:%d has commit", countFlush))
			countFlush = 0
			if tx, err = con.Begin(); err != nil {
				logger.Println(fmt.Sprintf("begin sql err %s", err))
			}
			if st, err = tx.Prepare(sql); err != nil {
				logger.Println(fmt.Sprintf("st sql err %s", err))
			}
		}
	}
}

type RowDesc struct {
	Name              string `db:"column_name"`
	Type              string `db:"column_type"`
	DefaultType       string `db:"default_kind"`
	DefaultExpression string `db:"default_expression"`
}

// ???????????????
func retrieve(database string, tableName string) {
	if err := con.Select(&columns, fmt.Sprintf("SELECT `name` as column_name ,`type` as column_type,default_kind,default_expression FROM `system`.columns where database='%s' AND table='%s' ", database, tableName)); err != nil {
		logger.Fatal("retrieve column fail ", err)
	}
}

func ParseTimeStamp(t interface{}) (time.Time, error) {
	var (
		rst time.Time
	)
	if v, ok := t.(json.Number); ok {
		t1, err := v.Int64()
		if err != nil {
			return rst, err
		}
		return time.Unix(t1, 0), nil
	}

	if v, ok := t.(string); ok {
		t1, err := strconv.Atoi(v)
		if err != nil {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return rst, err
			}
			t1 := math.Floor(f)
			return time.Unix(int64(t1), int64(1000000000*(f-t1))), nil
		}
		return time.Unix(int64(t1), 0), nil
	}

	if t1, ok := t.(int); ok {
		return time.Unix(int64(t1), 0), nil
	}
	if t1, ok := t.(int64); ok {
		return time.Unix(t1, 0), nil
	}
	return rst, fmt.Errorf("%s unknown type:%s", t, reflect.TypeOf(t).String())
}

func ParseTimeStampMs(t interface{}) (time.Time, error) {
	var (
		rst time.Time
	)
	if reflect.TypeOf(t).String() == "json.Number" {
		t1, err := t.(json.Number).Int64()
		if err != nil {
			return rst, err
		}
		return time.Unix(t1/1000, t1%1000*1000000), nil
	}
	if reflect.TypeOf(t).Kind() == reflect.String {
		t1, err := strconv.Atoi(t.(string))
		if err != nil {
			return rst, err
		}
		t2 := int64(t1)
		return time.Unix(t2/1000, t2%1000*1000000), nil
	}
	if reflect.TypeOf(t).Kind() == reflect.Int {
		t1 := int64(t.(int))
		return time.Unix(t1/1000, t1%1000*1000000), nil
	}
	if reflect.TypeOf(t).Kind() == reflect.Int64 {
		t1 := t.(int64)
		return time.Unix(t1/1000, t1%1000*1000000), nil
	}
	return rst, fmt.Errorf("%s unknown type:%s", t, reflect.TypeOf(t).String())
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	go consumer(ctx)
	go filer(ctx)
	go sink2ck(ctx)
	// ????????????
	//go func() {
	//	err := http.ListenAndServe("localhost:6060", nil)
	//	if err != nil {
	//		fmt.Println(err)
	//		return
	//	}
	//}()

	select {
	case <-sigterm:
		cancel()
		logger.Println("main terminating: via signal over  ... ")
	}
	wg.Wait()
	logger.Println("over")

}
