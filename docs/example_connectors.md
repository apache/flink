---
title:  "Example: Connectors"
---

Apache Flink allows users to access many different systems as data sources or sinks. The system is designed for very easy extensibility. Similar to Apache Hadoop, Flink has the concept of so called `InputFormat`s and `OutputFormat`s.

One implementation of these `InputFormat`s is the `HadoopInputFormat`. This is a wrapper that allows users to use all existing Hadoop input formats with Flink.

This page shows some examples for connecting Flink to other systems.


## Access Microsoft Azure Table Storage

_Note: This example works starting from Flink 0.6-incubating_

This example is using the `HadoopInputFormat` wrapper to use an existing Hadoop input format implementation for accessing [Azure's Table Storage](https://azure.microsoft.com/en-us/documentation/articles/storage-introduction/).

1. Download and compile the `azure-tables-hadoop` project. The input format developed by the project is not yet available in Maven Central, therefore, we have to build the project ourselves.
Execute the following commands:

    ```bash
    git clone https://github.com/mooso/azure-tables-hadoop.git
    cd azure-tables-hadoop
    mvn clean install
    ```

2. Setup a new Flink project using the quickstarts:

    ```bash
    curl https://raw.githubusercontent.com/apache/incubator-flink/master/flink-quickstart/quickstart.sh | bash
    ```

3. Set the the version of Flink to `{{site.FLINK_VERSION_HADOOP_2_STABLE}}` in the `pom.xml` file. The quickstart.sh script sets the version to the `hadoop1` version of Flink. Since the `microsoft-hadoop-azure` has been written for Hadoop 2.2 (mapreduce-API) version, we need to use the appropriate Flink version. 

    Replace all occurences of `<version>{{site.FLINK_VERSION_STABLE}}</version>` with `<version>{{site.FLINK_VERSION_HADOOP_2_STABLE}}</version>`.
4. Add the following dependencies (in the `<dependencies>` section) to your `pom.xml` file:

    ```xml
    <dependency>
    	<groupId>org.apache.flink</groupId>
    	<artifactId>flink-hadoop-compatibility</artifactId>
    	<version>{{site.FLINK_VERSION_HADOOP_2_STABLE}}</version>
    </dependency>
    <dependency>
      <groupId>com.microsoft.hadoop</groupId>
      <artifactId>microsoft-hadoop-azure</artifactId>
      <version>0.0.4</version>
    </dependency>
    ```
    - `flink-hadoop-compatibility` is a Flink package that provides the Hadoop input format wrappers.
    - `microsoft-hadoop-azure` is adding the project we've build before to our project.

The project is now prepared for starting to code. We recommend to import the project into an IDE, such as Eclipse or IntelliJ. (Import as a Maven project!).
Browse to the code of the `Job.java` file. Its an empty skeleton for a Flink job.

Paste the following code into it:
```java
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
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
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    
    // create a  AzureTableInputFormat, using a Hadoop input format wrapper
    HadoopInputFormat<Text, WritableEntity> hdIf = new HadoopInputFormat<Text, WritableEntity>(new AzureTableInputFormat(), Text.class, WritableEntity.class, new Job());

    // set the Account URI, something like: https://apacheflink.table.core.windows.net
    hdIf.getConfiguration().set(AzureTableConfiguration.Keys.ACCOUNT_URI.getKey(), "TODO"); 
    // set the secret storage key here
    hdIf.getConfiguration().set(AzureTableConfiguration.Keys.STORAGE_KEY.getKey(), "TODO");
    // set the table name here
    hdIf.getConfiguration().set(AzureTableConfiguration.Keys.TABLE_NAME.getKey(), "TODO");
    
    DataSet<Tuple2<Text, WritableEntity>> input = env.createInput(hdIf);
    // a little example how to use the data in a mapper.
    DataSet<String> fin = input.map(new MapFunction<Tuple2<Text,WritableEntity>, String>() {
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
The example shows how to access an Azure table and turn data into Flink's `DataSet` (more specifically, the type of the set is `DataSet<Tuple2<Text, WritableEntity>>`). With the `DataSet`, you can apply all known transformations to the DataSet.

## Access MongoDB

_Note: This example works starting from Flink 0.5 (then called Stratosphere)_

Please see this (slightly outdated) blogpost on [How to access MongoDB with Apache Flink](http://flink.incubator.apache.org/news/2014/01/28/querying_mongodb.html).

