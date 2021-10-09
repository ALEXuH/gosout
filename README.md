# gosout
Extract json  data from Kafka to Clickhouse

### 运行

```shell
nohup ./kafka2ck > 1.log 2>&1 &
```

### 配置

#### 示例配置

```
System:
  Cpu: 24
  Memory: 10
  ChannelCount: 200000
  FilterCount: 30
Filter:
  match:
    - 'bytesIn==100 & bytesIn!=100'
    - 'bytesIn==100'
    - 'bytesIn in 100,200,300'
    - 'deviceSent regex \d\d\d[1-9]'
  Rename:
    hostName1: 'hostName1'
    hostName2: 'hostName2'
    hostName3: 'hostName3'
  Add:
    columns1:
      match:
         - 'name == a & age != 10'
         - 'bytesOut==100'
      value: 20
    columns2:
      value: 'dsad'
    columns3:
      value: '10'
  Convert:
    columns: 'eventId'
Kafka:
  Topic: 'gotest6'
  Broker: '10.50.2.57:19092,10.50.2.57:19093,10.50.2.57:19091'
  threads: 10
  Version: '0.10.2.1'
  ClientId: 'goclient111d'
  GroupId: 'gotest10'
  FromBeginning: False
Clickhouse:
  Database: 'ailpha'
  TableName: 'ailpha_securitylog'
  Fields: '*'
  Debug: False
  BlockSize: 350000
  UserName: 'default'
  Password: ''
  Hosts: '10.50.2.57:29100'
  FlushSize: 200000
  FlushTime: 30
  Lz4Compress: 1
  Concurrent: 10
  Location: 'Asia/Shanghai'
  ConnectionLifeTime: 60
  DateTimeFormat:
    - '2006-01-02 15:04:05'
```

#### System

```
System:
  Cpu: 24
  Memory: 10
  ChannelCount: 200000
  FilterCount: 30
```

**cpu**

使用cpu核数，根据服务器资源和数据量来调整大小

**ChannelCount**

存放数据通道容量，当数据量较大时可以增加该数值，但相应内存使用会上升

**FilterCount**

数据处理协程数量，当有复杂的过滤条件时可以适当增加该数量

#### Kafka

```
Kafka:
  Topic: 'gotest6'
  Broker: '10.50.2.57:19092,10.50.2.57:19093,10.50.2.57:19091'
  threads: 10
  Version: '0.10.2.1'
  ClientId: 'goclient111d'
  GroupId: 'gotest10'
  FromBeginning: False
```



**Topic**

kafka主题，可以配置多个

**Broker**

kafka地址

**threads**

kafka消费线程数量，数据量大时可以增加该数量

**Version**

kafka版本

**ClientId**

消费标识

**GroupId**

消费者组

**FromBeginning**

是从头消费还是从最新消费. 默认是 false，从最新的消费， 但是如果groupid已经消费过, 会接着之前的消费.

#### Clickhouse

```
Clickhouse:
  Database: 'ailpha'
  TableName: 'ailpha_securitylog'
  Fields: '*'
  Debug: False
  BlockSize: 350000
  UserName: 'default'
  Password: ''
  Hosts: '10.50.2.57:29100'
  FlushSize: 200000
  FlushTime: 30
  Lz4Compress: 1
  Concurrent: 10
  Location: 'Asia/Shanghai'
  DateTimeFormat:
    - '2006-01-02 15:04:05'
```

**Database**

数据库

**TableName**

表名

**Fields**

字段，*代表自动获取当前表的所有字段，多个字段使用，隔开

**Debug**

是否开启ck写入日志

**BlockSize**

缓存中的值

**UserName**

数据库连接用户

**Password**

密码

**Hosts**

clickhouse地址，可以填多个会自动负载均衡，使用，分隔 如'10.50.2.57:29100，10.50.2.56:39100'

**FlushSize**

刷新数量，当满足该数据量时会进行写入缓存操作，当数据量小时可以降低该值

**FlushTime**

刷新时间，当满足该值时进行写入缓存操作， 只要满足属性数量或者刷新时间就会将数据刷入到缓存中

当缓存中的值大于等于blocksize时就会真正进入ck的数据写入操作

**Lz4Compress**

数据压缩 1开启 0关闭

**Concurrent**

同时写入ck的线程数目，ck并发写入较弱，建议配置不要太高

**Location**

时区设置 如Asia/Shanghai

**DateTimeFormat**

时间格式化，支持多种时间格式

**ConnectionLifeTime**

clickhouse连接生存时间



#### Filter

支持过滤数据，重命名字段，添加字段,类型转化（只能转化为string）

```
Filter:
  match:
    - 'bytesIn==100 & bytesIn!=100'
    - 'bytesIn==100'
    - 'bytesIn in 100,200,300' 支持in 和正则匹配
    - 'deviceSent regex ^10\.' 
  Rename:
    - 'hostName1:hostName2'   // srcColumn : destColumn
    - 'hostName2:hostName2'
    - 'hostName2:hostName2'
  Add:
    columns1:
      match:
         - 'name == a & age != 10'
         - 'bytesOut==100'
      value: 20
      column: 'column1'  // 字段名
    columns2:
      value: 'dsad'
      column: 'column3'  // 字段名
    columns3:
      value: '10'
      column: 'column2'  // 字段名
 Convert:
   - 'eventId'              // float类型转化为string
   - 'eventNum'
```

**match**

根据字段值筛选数据，支持等于不等常量值，正则匹配，in 等语法，支持多组过滤条件，只要有一组符合该数据会被筛选，都不符合该数据会被过滤掉

**Rename**

重命名字段

**Add**

可以添加match来判断是否添加该常量，也可直接添加常量值

##### Convert

将数字类型转化为string类型