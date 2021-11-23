package org.apache.flink.mongodb.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Options for the Mongodb connector. */
@PublicEvolving
public class MongodbConnectorOptions {

    public static final ConfigOption<String> DATABASE = ConfigOptions.key("database".toLowerCase())
            .stringType()
            .noDefaultValue()
            .withDescription("The data base to connect.");
    public static final ConfigOption<String> URI = ConfigOptions.key("uri".toLowerCase())
            .stringType()
            .noDefaultValue()
            .withDescription("The uri to connect.");
    public static final ConfigOption<String> COLLECTION_NAME = ConfigOptions
            .key("collection".toLowerCase())
            .stringType()
            .noDefaultValue()
            .withDescription("The name of the collection to return.");
    public static final ConfigOption<Integer> MAX_CONNECTION_IDLE_TIME = ConfigOptions
            .key("maxConnectionIdleTime".toLowerCase())
            .intType()
            .defaultValue(Integer.valueOf(60000))
            .withDescription("The maximum idle time for a pooled connection.");
    public static final ConfigOption<Integer> BATCH_SIZE = ConfigOptions
            .key("batchSize".toLowerCase())
            .intType()
            .defaultValue(Integer.valueOf(1024))
            .withDescription("The batch size when table invoking.");

    public static final ConfigOption<String> FORMAT =
            ConfigOptions.key("format")
                    .stringType()
                    .defaultValue("json")
                    .withDescription(
                            "Defines the format identifier for encoding data. "
                                    + "The identifier is used to discover a suitable format factory.");
    public static final ConfigOption<String> SERVER =
            ConfigOptions.key("format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding data. "
                                    + "The identifier is used to discover a suitable format factory.");
    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding data. "
                                    + "The identifier is used to discover a suitable format factory.");
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding data. "
                                    + "The identifier is used to discover a suitable format factory.");
    public static final ConfigOption<Boolean> RETRY_WRITES =
            ConfigOptions.key("format")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Defines the format identifier for encoding data. "
                                    + "The identifier is used to discover a suitable format factory.");
    public static final ConfigOption<Long> TIMEOUT =
            ConfigOptions.key("format")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription(
                            "Defines the format identifier for encoding data. "
                                    + "The identifier is used to discover a suitable format factory.");

    public MongodbConnectorOptions() {
    }
}
