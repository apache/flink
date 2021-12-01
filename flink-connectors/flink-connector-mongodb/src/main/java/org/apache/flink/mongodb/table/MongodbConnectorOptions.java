package org.apache.flink.mongodb.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;

/** Options for the Mongodb connector. */
@PublicEvolving
public class MongodbConnectorOptions {

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url".toLowerCase())
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Mongodb database URL.");

    public static final ConfigOption<String> COLLECTION_NAME =
            ConfigOptions.key("collection".toLowerCase())
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Mongodb collection name.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Mongodb user name.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Mongodb password.");

    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database".toLowerCase())
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Mongodb database.");

    public static final ConfigOption<Integer> MAX_CONNECTION_IDLE_TIME =
            ConfigOptions.key("maxConnectionIdleTime".toLowerCase())
                    .intType()
                    .defaultValue(Integer.valueOf(60000))
                    .withDescription("The maximum idle time for a pooled connection.");

    public static final ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key("batchSize".toLowerCase())
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

    public static final ConfigOption<Duration> MAX_RETRY_TIMEOUT =
            ConfigOptions.key("connection.max-retry-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription("Maximum timeout between retries.");

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

    // -----------------------------------------------------------------------------------------
    // Lookup options
    // -----------------------------------------------------------------------------------------

    public static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS =
            ConfigOptions.key("lookup.cache.max-rows")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription(
                            "The max number of rows of lookup cache, over this value, the oldest rows will "
                                    + "be eliminated. \"cache.max-rows\" and \"cache.ttl\" options must all be specified if any of them is "
                                    + "specified.");

    public static final ConfigOption<Duration> LOOKUP_CACHE_TTL =
            ConfigOptions.key("lookup.cache.ttl")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription("The cache time to live.");

    public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES =
            ConfigOptions.key("lookup.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("The max retry times if lookup database failed.");

    public static final ConfigOption<Boolean> LOOKUP_CACHE_MISSING_KEY =
            ConfigOptions.key("lookup.cache.caching-missing-key")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Flag to cache missing key. true by default");

    // write config options
    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.buffer-flush.max-rows")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "The flush max size (includes all append, upsert and delete records), over this number"
                                    + " of records, will flush data.");

    public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("sink.buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            "The flush interval mills, over this time, asynchronous threads will flush data.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("The max retry times if writing records to database failed.");

    public MongodbConnectorOptions() {}
}
