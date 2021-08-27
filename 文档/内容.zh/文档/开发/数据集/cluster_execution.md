---
title:  "集群执行"
weight: 13
type: docs
aliases:
  - /zh/dev/cluster_execution.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# 集群执行



Flink 程序可以分布式运行在多机器集群上。有两种方式可以将程序提交到集群上执行：

<a name="command-line-interface"></a>

## 命令行界面（Interface）

命令行界面使你可以将打包的程序（JARs）提交到集群（或单机设置）。

有关详细信息，请参阅[命令行界面]({{< ref "docs/deployment/cli" >}})文档。

<a name="remote-environment"></a>

## 远程环境（Remote Environment）

远程环境使你可以直接在集群上执行 Flink Java 程序。远程环境指向你要执行程序的集群。

<a name="maven-dependency"></a>

### Maven Dependency

如果将程序作为 Maven 项目开发，则必须添加 `flink-clients` 模块的依赖：

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-clients{{< scala_version >}}</artifactId>
  <version>{{< version >}}</version>
</dependency>
```

<a name="example"></a>

### 示例

下面演示了 `RemoteEnvironment` 的用法：

```java
public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment
        .createRemoteEnvironment("flink-jobmanager", 8081, "/home/user/udfs.jar");

    DataSet<String> data = env.readTextFile("hdfs://path/to/file");

    data
        .filter(new FilterFunction<String>() {
            public boolean filter(String value) {
                return value.startsWith("http://");
            }
        })
        .writeAsText("hdfs://path/to/result");

    env.execute();
}
```

请注意，该程序包含用户自定义代码，因此需要一个带有附加代码类的 JAR 文件。远程环境的构造函数使用 JAR 文件的路径进行构造。
1.获取参数
val parms = ParameterTool.fromArgs();
var host = parms.get("host")
var prot = parms.get("prot");
flink启动

//启动任务 c 全限定名  p子任务数  jar包地址 --host url  --port 端口

./bin/flink run -c com.zhp.flink01 -p 3 execjar/day08-1.0-SNAPSHOT-jar-with-dependencies.jar --host hadoop102 -prot 6666
取消任务  cancel +jbo

StreamExectionEnvironment.getExecutionEnvironment
3.api 读取数据源基本操作
flink数据源：
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.setParallelism(1)
//1.从集合中获取数据 env.fromCollection
val inputDS = env.fromCollection(List(
    SensorReading("sensor_1",1547718199,35.80018327300259)
    ))
inputDS.print()
//定义样例类，传感器id、时间戳、温度
case class SensorReading(id:String,timetamp:Long,temperature:Double)
//2.从元素中获取数据 env.fromElements
env.fromElements(1,2.0,"asd").print()
//3.从 kafka 中获取数据 env.addSource
(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),prop))
var prop = new Properties()
prop.setProperty("bootstrap.servers","hadoop102:9092")
prop.setProperty("zookeeper.connect","hadoop102:2181")
val inputDS = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),prop))
inputDS.print("kafka source")
//4.从mysql中读取数据
env.createInput(JDBCInputFormat.buildJDBCInputFormat()
//1、基于流式环境
def main(args:Array[String]):Unit = {
    val env = StreamExcutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val dataDS = MySqlRead(env)
    dataDS.map(t => (t.getField(0),t.getField(1))).print()
    env.execute()
}
def MySqlRead(env:StreamExcutionEnvironment)={
    val inputMysql = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
    .setDrivername("com.mysql.jdbc.Driver")
    .setDBUrl("jdbc:mysql://hadoop102:3306/1901b")
    .setUsername("root")
    .setPassword("root")
    .setQuery("select * from t_stu")
    .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.DOUBLE_TYPE_INFO))
    .finish()
)    
    inputMysql
}
//2、批处理mysql
env.createInput(JDBCInputFormat.buildJDBCInputFormat()
def main(args:Array[String]):Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val input = MySqlRead(env)
    imput.map(t => (t.getField(0),t.getField(1))).print
}
def MySqlRead(env:ExcutionEnvironment)={
    val inputMysql = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
    .setDrivername("com.mysql.jdbc.Driver")
    .setDBUrl("jdbc:mysql://hadoop102:3306/week3")
    .setUsername("root")
    .setPassword("root")
    .setQuery("select * from week3")
    .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.DOUBLE_TYPE_INFO))
    .finish()
)    
    inputMysql
}
{{< top >}}
