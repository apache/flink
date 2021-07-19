package org.apache.flink.glue.schema.registry.test.json.specific;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.json.glue.schema.registry.GlueSchemaRegistryJsonDeserializationSchema;
import org.apache.flink.formats.json.glue.schema.registry.GlueSchemaRegistryJsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * A simple example that shows how to read from and write to Kinesis. This will read JSON messages
 * from the input stream, and finally write back to another stream.
 *
 * <p>Example usage: --input-stream test-input --output-stream test-output --aws.endpoint
 * https://localhost:4567 --flink.stream.initpos TRIM_HORIZON
 */
public class GlueSchemaRegistryPojoKinesisExample {

    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String inputStream = parameterTool.getRequired("input-stream");
        String outputStream = parameterTool.getRequired("output-stream");

        FlinkKinesisConsumer<Car> consumer =
                new FlinkKinesisConsumer<>(
                        inputStream,
                        new GlueSchemaRegistryJsonDeserializationSchema<>(
                                Car.class, inputStream, getConfigs()),
                        parameterTool.getProperties());

        Properties producerProperties = new Properties(parameterTool.getProperties());
        // producer needs region even when URL is specified
        producerProperties.put(ConsumerConfigConstants.AWS_REGION, "ca-central-1");
        // test driver does not deaggregate
        producerProperties.put("AggregationEnabled", String.valueOf(false));

        // KPL does not recognize endpoint URL..
        String kinesisUrl = producerProperties.getProperty(ConsumerConfigConstants.AWS_ENDPOINT);
        if (kinesisUrl != null) {
            URL url = new URL(kinesisUrl);
            producerProperties.put("KinesisEndpoint", url.getHost());
            producerProperties.put("KinesisPort", Integer.toString(url.getPort()));
            producerProperties.put("VerifyCertificate", "false");
        }

        FlinkKinesisProducer<Car> producer =
                new FlinkKinesisProducer<>(
                        new GlueSchemaRegistryJsonSerializationSchema<>(outputStream, getConfigs()),
                        producerProperties);
        producer.setDefaultStream(outputStream);
        producer.setDefaultPartition("fakePartition");

        DataStream<Car> input = env.addSource(consumer);
        input.addSink(producer);
        env.execute();
    }

    private static Map<String, Object> getConfigs() {
        Map<String, Object> configs = new HashMap();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "ca-central-1");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        configs.put(
                AWSSchemaRegistryConstants.AVRO_RECORD_TYPE,
                AvroRecordType.GENERIC_RECORD.getName());

        return configs;
    }
}
