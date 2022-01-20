---
title:  "Azure Table storage"
weight: 4
type: docs
aliases:
- /dev/connectors/formats/azure_table_storage.html
- /apis/streaming/connectors/formats/azure_table_storage.html
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

This example is using the `HadoopInputFormat` wrapper to use an existing Hadoop input format implementation for accessing [Azure's Table Storage](https://docs.microsoft.com/en-us/azure/storage/tables/table-storage-overview).

1. Download and compile the `azure-tables-hadoop` project. The input format developed by the project is not yet available in Maven Central, therefore, we have to build the project ourselves.
   Execute the following commands:

```bash
git clone https://github.com/mooso/azure-tables-hadoop.git
cd azure-tables-hadoop
mvn clean install
```

2. Setup a new Flink project using the quickstarts:

```bash
curl https://flink.apache.org/q/quickstart.sh | bash
```

3. Add the following dependencies (in the `<dependencies>` section) to your `pom.xml` file:

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

`flink-hadoop-compatibility` is a Flink package that provides the Hadoop input format wrappers.
`microsoft-hadoop-azure` is adding the project we've build before to our project.

The project is now ready for starting to code. We recommend to import the project into an IDE, such as IntelliJ. You should import it as a Maven project.
Browse to the file `Job.java`. This is an empty skeleton for a Flink job.

Paste the following code:

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
    // set up the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    // create a  AzureTableInputFormat, using a Hadoop input format wrapper
    HadoopInputFormat<Text, WritableEntity> hdIf = new HadoopInputFormat<Text, WritableEntity>(new AzureTableInputFormat(), Text.class, WritableEntity.class, new Job());

    // set the Account URI, something like: https://apacheflink.table.core.windows.net
    hdIf.getConfiguration().set(azuretableconfiguration.Keys.ACCOUNT_URI.getKey(), "TODO");
    // set the secret storage key here
    hdIf.getConfiguration().set(AzureTableConfiguration.Keys.STORAGE_KEY.getKey(), "TODO");
    // set the table name here
    hdIf.getConfiguration().set(AzureTableConfiguration.Keys.TABLE_NAME.getKey(), "TODO");

    DataStream<Tuple2<Text, WritableEntity>> input = env.createInput(hdIf);
    // a little example how to use the data in a mapper.
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

    // emit result (this works only locally)
    fin.print();

    // execute program
    env.execute("Azure Example");
  }
}
```

The example shows how to access an Azure table and turn data into Flink's `DataStream` (more specifically, the type of the set is `DataStream<Tuple2<Text, WritableEntity>>`). With the `DataStream`, you can apply all known transformations to the DataStream.

{{< top >}}
