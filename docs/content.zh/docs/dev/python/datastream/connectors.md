---
title: "Connectors"
weight: 32
type: docs
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

## Bundled Connectors

Apache Flink has supported many [bundled connectors]({{< ref "docs/connectors/datastream/overview" >}}). 
In order to use the [bundled connectors]({{< ref "docs/connectors/datastream/overview" >}}) in PyFlink jobs, the corresponding dependencies are required.

See [Python dependency management]({{< ref "docs/dev/python/dependency_management" >}}#jar-dependencies) for more details on how to use JARs in PyFlink.

## Custom third-party Connectors

Similar to [Bundled Connectors](#bundled-connectors), you need to add the corresponding dependencies. Besides that, you need to add the simple Python
Wrapper Class to wrap your Java Implementation. We can take [RabbitMQ Connector]({{< ref "docs/connectors/datastream/rabbitmq" >}}) as example. 

In RabbitMQ Connector API, it provides the following three classes for use.

```java
public class RMQConnectionConfig implements Serializable {
    // ... methods
}

public class RMQSource<OUT> extends MultipleIdsMessageAcknowledgingSourceBase<OUT, String, Long>
        implements ResultTypeQueryable<OUT> {
    // ... methods
}

public class RMQSink<IN> extends RichSinkFunction<IN> {
    // ... methods
}
```

We can take use of [py4j](https://github.com/py4j/py4j) to provide the Python wrapper class easily.

```python
class RMQConnectionConfig(object):
    """
    Connection Configuration for RMQ.
    """

    def __init__(self, j_rmq_connection_config):
        # j_rmq_connection_config is the corresponding Java RMQConnectionConfig instance.
        self._j_rmq_connection_config = j_rmq_connection_config

    # Aligned with Java Methods
    def get_host(self) -> str:
        return self._j_rmq_connection_config.getHost()

    # ... other methods



class RMQSource(SourceFunction):
    def __init__(self,
                 connection_config: 'RMQConnectionConfig',
                 queue_name: str,
                 use_correlation_id: bool,
                 deserialization_schema: DeserializationSchema
                 ):
    # use py4j to create the corresponding Java RMQSource.
    JRMQSource = get_gateway().jvm.org.apache.flink.streaming.connectors.rabbitmq.RMQSource
    j_rmq_source = JRMQSource(
        connection_config._j_rmq_connection_config,
        queue_name,
        use_correlation_id,
        deserialization_schema._j_deserialization_schema
    )
    super(RMQSource, self).__init__(source_func=j_rmq_source)


class RMQSink(SinkFunction):
    def __init__(self, connection_config: 'RMQConnectionConfig',
                 queue_name: str, serialization_schema: SerializationSchema):
        # use py4j to create the corresponding Java RMQSource.
        JRMQSink = get_gateway().jvm.org.apache.flink.streaming.connectors.rabbitmq.RMQSink
        j_rmq_sink = JRMQSink(
            connection_config._j_rmq_connection_config,
            queue_name,
            serialization_schema._j_serialization_schema,
        )
        super(RMQSink, self).__init__(sink_func=j_rmq_sink)
```

For more details, you can refer to the [Python implementation of RabbitMQ Connector](https://github.com/apache/flink/blob/master/flink-python/pyflink/datastream/connectors/rabbitmq.py)
