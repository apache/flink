package org.apache.flink.connector.kafka.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Configurations for KafkaSink. */
public class KafkaSinkOptions {

    public static final ConfigOption<String> CLIENT_ID_PREFIX =
            ConfigOptions.key("client.id.prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The prefix to use for the Kafka producers.");
}
