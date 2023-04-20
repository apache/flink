---
title:  "Azure Table storage"
weight: 4
type: docs
aliases:
- /zh/dev/connectors/formats/azure_table_storage.html
- /zh/apis/streaming/connectors/formats/azure_table_storage.html
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

# Azure Table Storage

本例使用 `HadoopInputFormat` 包装器来使用现有的 Hadoop input format 实现访问 [Azure's Table Storage](https://docs.microsoft.com/en-us/azure/storage/tables/table-storage-overview).

1. 下载并编译 `azure-tables-hadoop` 项目。该项目开发的 input format 在 Maven 中心尚不存在，因此，我们必须自己构建该项目。 
   执行如下命令：

```bash
git clone https://github.com/mooso/azure-tables-hadoop.git
cd azure-tables-hadoop
mvn clean install
```

2. 使用 quickstarts 创建一个新的 Flink 项目：

```bash
curl https://flink.apache.org/q/quickstart.sh | bash
```

3. 在你的 `pom.xml` 文件 `<dependencies>` 部分添加如下依赖：

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-hadoop-compatibility{{< scala_version >}}</artifactId>
    <version>{{< version >}}</version>
</dependency>
<dependency>
    <groupId>com.microsoft.hadoop</groupId>
    <artifactId>microsoft-hadoop-azure</artifactId>
    <version>0.0.5</version>
</dependency>
```

`flink-hadoop-compatibility` 是一个提供 Hadoop input format 包装器的 Flink 包。
`microsoft-hadoop-azure` 可以将之前构建的部分添加到项目中。

现在可以开始进行项目的编码。我们建议将项目导入 IDE，例如 IntelliJ。你应该将其作为 Maven 项目导入。
跳转到文件 `Job.java`。这是 Flink 作业的初始框架。

粘贴如下代码：

```java
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataStream;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoopcompatibility.mapreduce.HadoopInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import com.microsoft.hadoop.azure.AzureTableConfiguration;
import com.microsoft.hadoop.azure.AzureTableInputFormat;
import com.microsoft.hadoop.azure.WritableEntity;
import com.microsoft.windowsazure.storage.table.EntityProperty;

public class AzureTableExample {

  public static void main(String[] args) throws Exception {
    // 安装 execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    // 使用 Hadoop input format 包装器创建 AzureTableInputFormat
    HadoopInputFormat<Text, WritableEntity> hdIf = new HadoopInputFormat<Text, WritableEntity>(new AzureTableInputFormat(), Text.class, WritableEntity.class, new Job());

    // 设置 Account URI，如 https://apacheflink.table.core.windows.net
    hdIf.getConfiguration().set(azuretableconfiguration.Keys.ACCOUNT_URI.getKey(), "TODO");
    // 设置存储密钥
    hdIf.getConfiguration().set(AzureTableConfiguration.Keys.STORAGE_KEY.getKey(), "TODO");
    // 在此处设置表名
    hdIf.getConfiguration().set(AzureTableConfiguration.Keys.TABLE_NAME.getKey(), "TODO");

    DataStream<Tuple2<Text, WritableEntity>> input = env.createInput(hdIf);
    // 如何在 map 中使用数据的简单示例。
    DataStream<String> fin = input.map(new MapFunction<Tuple2<Text,WritableEntity>, String>() {
      @Override
      public String map(Tuple2<Text, WritableEntity> arg0) throws Exception {
        System.err.println("--------------------------------\nKey = "+arg0.f0);
        WritableEntity we = arg0.f1;

        for(Map.Entry<String, EntityProperty> prop : we.getProperties().entrySet()) {
          System.err.println("key="+prop.getKey() + " ; value (asString)="+prop.getValue().getValueAsString());
        }

        return arg0.f0.toString();
      }
    });

    // 发送结果（这仅在本地模式有效）
    fin.print();

    // 执行程序
    env.execute("Azure Example");
  }
}
```
该示例展示了如何访问 Azure 表和如何将数据转换为 Flink 的 `DataStream`（更具体地说，集合的类型是 `DataStream<Tuple2<Text, WritableEntity>>`）。你可以将所有已知的 transformations 应用到 DataStream 实例。

{{< top >}}
