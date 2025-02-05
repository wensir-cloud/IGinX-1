# IGinX Installation Manual - By Source (Compilation and Installation)

[TOC]

IGinX is an open source polystore system. A polystore system provides an integrated data management service over a set of one or more potentially heterogeneous database/storage engines, serving heterogeneous workloads.

Currently, IGinX directly supports big data service over relational database PostgreSQL, time series databases InfluxDB/IoTDB/TimescaleDB/OpenTSDB, and Parquet data files.

## Download and Installation

### Java Installation

Since ZooKeeper, IGinX and IoTDB are all developed using Java, Java needs to be installed first. If a running environment of JDK >= 1.8 has been installed locally, **skip this step entirely**.

1. First, visit the [official Java website] (https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html) to download the JDK package for your current system.

2. Installation

```shell
$ cd ~/Downloads
$ tar -zxf jdk-8u181-linux-x64.gz # unzip files
$ mkdir /opt/jdk
$ mv jdk-1.8.0_181 /opt/jdk/
```

3. Set the path

Edit the ~/.bashrc file and add the following two lines at the end of the file:

```shell
export JAVA_HOME = /usr/jdk/jdk-1.8.0_181
export PATH=$PATH:$JAVA_HOME/bin
```

Load the file with the changed configuration (into shell scripts):

```shell
$ source ~/.bashrc
```

4. Use java -version to determine whether JDK installed successfully.

```shell
$ java -version
java version "1.8.0_181"
Java(TM) SE Runtime Environment (build 1.8.0_181-b13)
Java HotSpot(TM) 64-Bit Server VM (build 25.181-b13, mixed mode)
```

If the words above are displayed, it means the installation was successful.

### Maven Installation

Maven is a build automation tool used primarily to build and managa Java projects. If you need to compile from the source code, you also need to install a Maven environment >= 3.6. Otherwise, **skip this step entirely**.

1. Visit the [official website](http://maven.apache.org/download.cgi)to download and unzip Maven

```shell
$ wget http://mirrors.hust.edu.cn/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
$ tar -xvf  apache-maven-3.3.9-bin.tar.gz
$ sudo mv -f apache-maven-3.3.9 /usr/local/
```

2. Set the path

Edit the ~/.bashrc file and add the following two lines at the end of the file:

```shell
export MAVEN_HOME=/usr/local/apache-maven-3.3.9
export PATH=${PATH}:${MAVEN_HOME}/bin
```

Load the file with the changed configuration (into shell scripts):

```shell
$ source ~/.bashrc
```

3. Type mvn -v to determine whether Maven installed successfully.

```shell
$ mvn -v
Apache Maven 3.6.1 (d66c9c0b3152b2e69ee9bac180bb8fcc8e6af555; 2019-04-05T03:00:29+08:00)
```

If the words above are displayed, that means the installation was successful.

### IoTDB Installation

IoTDB is Apache's Apache IoT native database with high performance for data management and analysis, deployable on the edge and the cloud.

The specific installation method is as follows:

```shell
$ cd ~
$ wget https://mirrors.bfsu.edu.cn/apache/iotdb/0.12.0/apache-iotdb-0.12.0-server-bin.zip
$ unzip apache-iotdb-0.12.0-server-bin.zip
```

### IGinX Installation

Compile with source code. If you need to modify code yourself, you can use this installation method. 

#### Compilation with source code

Fetch the latest development version and build it locally.

```shell
$ cd ~
$ git clone git@github.com:THUIGinX/IGinX.git
$ cd IGinX
$ mvn clean install -Dmaven.test.skip=true
$ mvn package -pl core -Dmaven.test.skip=true
```

The following words are displayed, indicating that the IGinX build is successful:

```shell
[INFO] Reactor Summary for IGinX 0.6.0-SNAPSHOT:
[INFO]
[INFO] IGinX .............................................. SUCCESS [  0.252 s]
[INFO] IGinX Thrift ....................................... SUCCESS [  5.961 s]
[INFO] IGinX Core ......................................... SUCCESS [  4.383 s]
[INFO] IGinX IoTDB ........................................ SUCCESS [  0.855 s]
[INFO] IGinX InfluxDB ..................................... SUCCESS [  0.772 s]
[INFO] IGinX Client ....................................... SUCCESS [  7.713 s]
[INFO] IGinX Example ...................................... SUCCESS [  0.677 s]
[INFO] IGinX Test ......................................... SUCCESS [  0.114 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  20.887 s
[INFO] Finished at: 2021-07-12T16:01:31+08:00
[INFO] ------------------------------------------------------------------------
```

Additionally, IGinX supports Docker. Use the following command to build a local IGinX image:

```shell
mvn clean package -pl core -DskipTests docker:build
```

This may not work, which is not an immediate issue because you don't need Docker for IGinX installation.

## Launch

### IoTDB 

First of all, you need to launch IoTDB.

```shell
$ cd ~
$ cd apache-iotdb-0.12.0-server-bin/
$ ./sbin/start-server.sh
```

The following display of words means the IoTDB installation was successful：

```shell
2021-05-27 08:21:07,440 [main] INFO  o.a.i.d.s.t.ThriftService:125 - IoTDB: start RPC ServerService successfully, listening on ip 0.0.0.0 port 6667
2021-05-27 08:21:07,440 [main] INFO  o.a.i.db.service.IoTDB:129 - IoTDB is set up, now may some sgs are not ready, please wait several seconds...
2021-05-27 08:21:07,448 [main] INFO  o.a.i.d.s.UpgradeSevice:109 - finish counting upgrading files, total num:0
2021-05-27 08:21:07,449 [main] INFO  o.a.i.d.s.UpgradeSevice:74 - Waiting for upgrade task pool to shut down
2021-05-27 08:21:07,449 [main] INFO  o.a.i.d.s.UpgradeSevice:76 - Upgrade service stopped
2021-05-27 08:21:07,449 [main] INFO  o.a.i.db.service.IoTDB:146 - Congratulation, IoTDB is set up successfully. Now, enjoy yourself!
2021-05-27 08:21:07,450 [main] INFO  o.a.i.db.service.IoTDB:93 - IoTDB has started.
```

### IGinX

Using source code to launch

```shell
$ cd ~
$ cd IGinX
$ chmod +x startIginX.sh # enable permissions for startup scripts
$ ./startIginX.sh
```

The following display of words means the IGinX installation was successful：

```shell
May 27, 2021 8:32:19 AM org.glassfish.grizzly.http.server.NetworkListener start
INFO: Started listener bound to [127.0.0.1:6666]
May 27, 2021 8:32:19 AM org.glassfish.grizzly.http.server.HttpServer start
INFO: [HttpServer] Started.
08:32:19.446 [Thread-0] INFO cn.edu.tsinghua.iginx.rest.RestServer - IGinX REST server has been available at http://127.0.0.1:6666/.
```

## Using IGinX

### RESTful Interface

After the startup is complete, you can easily use the RESTful interface to write and query data to IGinX.

Create a file insert.json and add the following into it:

```json
[
  {
    "name": "archive_file_tracked",
    "datapoints": [
        [1359788400000, 123.3],
        [1359788300000, 13.2 ],
        [1359788410000, 23.1 ]
    ],
    "tags": {
        "host": "server1",
        "data_center": "DC1"
    }
  },
  {
      "name": "archive_file_search",
      "timestamp": 1359786400000,
      "value": 321,
      "tags": {
          "host": "server2"
      }
  }
]
```

Insert data into the database using the following command:

```shell
$ curl -XPOST -H'Content-Type: application/json' -d @insert.json http://127.0.0.1:6666/api/v1/datapoints
```

After inserting data, you can also query the data just written using the RESTful interface.

Create a file query.json and write the following data into it:

```json
{
    "start_absolute" : 1,
    "end_relative": {
        "value": "5",
        "unit": "days"
    },
    "time_zone": "Asia/Kabul",
    "metrics": [
        {
        "name": "archive_file_tracked"
        },
        {
        "name": "archive_file_search"
        }
    ]
}
```

Enter the following command to query the data:

```shell
$ curl -XPOST -H'Content-Type: application/json' -d @query.json http://127.0.0.1:6666/api/v1/datapoints/query
```

The command will return information about the data point just inserted:

```json
{
    "queries": [
        {
            "sample_size": 3,
            "results": [
                {
                    "name": "archive_file_tracked",
                    "group_by": [
                        {
                            "name": "type",
                            "type": "number"
                        }
                    ],
                    "tags": {
                        "data_center": [
                            "DC1"
                        ],
                        "host": [
                            "server1"
                        ]
                    },
                    "values": [
                        [
                            1359788300000,
                            13.2
                        ],
                        [
                            1359788400000,
                            123.3
                        ],
                        [
                            1359788410000,
                            23.1
                        ]
                    ]
                }
            ]
        },
        {
            "sample_size": 1,
            "results": [
                {
                    "name": "archive_file_search",
                    "group_by": [
                        {
                            "name": "type",
                            "type": "number"
                        }
                    ],
                    "tags": {
                        "host": [
                            "server2"
                        ]
                    },
                    "values": [
                        [
                            1359786400000,
                            321.0
                        ]
                    ]
                }
            ]
        }
    ]
}
```

If you see the following information returned, it means you are able to successfully use RESTful interface to write and query data to IGinX.

For more interfaces, please refer to the official [IGinX manual](https://github.com/THUIGinX/IGinX/blob/main/docs/pdf/userManualC.pdf).

If you want to use a different interface, there is another option.

In addition to the RESTful interface, IGinX also provides the RPC data access interface. For this specific interface, please refer to the official [IGinX manual](https://github.com/THUIGinX/IGinX/blob/main/docs/pdf/userManualC.pdf).

At the same time, IGinX also provides some official examples, showing the most common usage of the RPC interface.

Below is a short tutorial on how to use it.

### RPC Interface

Since the IGinX 0.5.1 version has not been released to the Maven central repository, if you want to use it, you need to manually install it to the local Maven repository. 

The specific installation method is as follows:

```shell
# download iginx 0.4 release version source code package
$ wget https://github.com/THUIGinX/IGinX/archive/refs/tags/release/v0.5.1.tar.gz
# Unzip the source package
$ tar -zxvf v0.5.1.tar.gz
# go to the main project's directory
$ cd IGinX-rc-v0.5.1
# Install to local Maven repository
$ mvn clean install -DskipTests
```

Specifically, when using it, you only need to introduce the following dependencies in the pom file of the corresponding project:

```xml
<dependency>
    <groupId>cn.edu.tsinghua</groupId>
    <artifactId>iginx-core</artifactId>
    <version>0.6.0-SNAPSHOT</version>
</dependency>
```

Before accessing IGinX, you first need to create a session and try to connect. The Session constructor has 4 parameters, which are the ip and port IGinX will to connect to, and the username and password for IGinX authentication. The current authentication system is still being written, so the account name and password to access the backend IGinX can directly fill in root:

```Java
Session session = new Session("127.0.0.1", 6888, "root", "root");
session.openSession();
```

You can then try to insert data into IGinX. Since IGinX supports the creation of time-series when data is written for the first time, there is no need to call the relevant series creation interface in advance. IGinX provides row-style and column-style data writing interfaces. 

The following is an example of using the column-style data writing interface:


```java
private static void insertColumnRecords(Session session) throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add("sg.d1.s1");
        paths.add("sg.d2.s2");
        paths.add("sg.d3.s3");
        paths.add("sg.d4.s4");

        int size = 1500;
        long[] timestamps = new long[size];
        for (long i = 0; i < size; i++) {
            timestamps[(int) i] = i;
        }

        Object[] valuesList = new Object[4];
        for (long i = 0; i < 4; i++) {
            Object[] values = new Object[size];
            for (long j = 0; j < size; j++) {
                if (i < 2) {
                  values[(int) j] = i + j;
                } else {
                  values[(int) j] = RandomStringUtils.randomAlphanumeric(10).getBytes();
                }
            }
            valuesList[(int) i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            dataTypeList.add(DataType.LONG);
        }
        for (int i = 0; i < 2; i++) {
            dataTypeList.add(DataType.BINARY);
        }

        session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
}
```

After completing the data writing, you can use the data query interface to query the data just written:

```java
private static void queryData(Session session) throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add("sg.d1.s1");
        paths.add("sg.d2.s2");
        paths.add("sg.d3.s3");
        paths.add("sg.d4.s4");

        long startTime = 100L;
        long endTime = 200L;

        SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime);
        dataSet.print();
}
```

You can also use the downsampling aggregation query interface to query the interval statistics of the data:

```java
private static void downsampleQuery(Session session) throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add("sg.d1.s1");
        paths.add("sg.d2.s2");

        long startTime = 100L;
        long endTime = 1101L;

        // MAX
        SessionQueryDataSet dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.MAX, 100);
        dataSet.print();

        // MIN
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.MIN, ROW_INTERVAL * 100);
        dataSet.print();

        // FIRST
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.FIRST, ROW_INTERVAL * 100);
        dataSet.print();

        // LAST
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.LAST, ROW_INTERVAL * 100);
        dataSet.print();

        // COUNT
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.COUNT, ROW_INTERVAL * 100);
        dataSet.print();

        // SUM
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.SUM, ROW_INTERVAL * 100);
        dataSet.print();

        // AVG
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.AVG, ROW_INTERVAL * 100);
        dataSet.print();

}
```

After the session is completed, you need to manually close and release your connection from your terminal/backend:

```shell
session.closeSession();
```

For the full version of the code, please refer to: https://github.com/THUIGinX/IGinX/blob/main/example/src/main/java/cn/edu/tsinghua/iginx/session/IoTDBSessionExample.java
