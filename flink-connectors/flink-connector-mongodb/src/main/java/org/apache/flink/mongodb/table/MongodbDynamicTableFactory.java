package org.apache.flink.mongodb.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.mongodb.internal.options.MongodbConnectorOptions;
import org.apache.flink.mongodb.internal.options.MongodbLookupOptions;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.mongodb.table.MongodbConnectorOptions.BATCH_SIZE;
import static org.apache.flink.mongodb.table.MongodbConnectorOptions.COLLECTION_NAME;
import static org.apache.flink.mongodb.table.MongodbConnectorOptions.DATABASE;
import static org.apache.flink.mongodb.table.MongodbConnectorOptions.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.mongodb.table.MongodbConnectorOptions.LOOKUP_CACHE_MISSING_KEY;
import static org.apache.flink.mongodb.table.MongodbConnectorOptions.LOOKUP_CACHE_TTL;
import static org.apache.flink.mongodb.table.MongodbConnectorOptions.LOOKUP_MAX_RETRIES;
import static org.apache.flink.mongodb.table.MongodbConnectorOptions.MAX_CONNECTION_IDLE_TIME;
import static org.apache.flink.mongodb.table.MongodbConnectorOptions.MAX_RETRY_TIMEOUT;
import static org.apache.flink.mongodb.table.MongodbConnectorOptions.PASSWORD;
import static org.apache.flink.mongodb.table.MongodbConnectorOptions.SINK_MAX_RETRIES;
import static org.apache.flink.mongodb.table.MongodbConnectorOptions.SINK_PARALLELISM;
import static org.apache.flink.mongodb.table.MongodbConnectorOptions.URL;
import static org.apache.flink.mongodb.table.MongodbConnectorOptions.USERNAME;

/** Insert-Mondodb factory. */
@Internal
public class MongodbDynamicTableFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "mongodb";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet();
        requiredOptions.add(DATABASE);
        requiredOptions.add(COLLECTION_NAME);
        requiredOptions.add(URL);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionals = new HashSet();
        optionals.add(MAX_CONNECTION_IDLE_TIME);
        optionals.add(BATCH_SIZE);
        return optionals;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        helper.validate();
        validateConfigOptions(config);

        return new MongodbDynamicTableSink(
                getMongodbOptions(helper.getOptions()), context.getPhysicalRowDataType());
    }

    private MongodbConnectorOptions getMongodbOptions(ReadableConfig readableConfig) {
        final String url = readableConfig.get(URL);
        final MongodbConnectorOptions.Builder builder =
                MongodbConnectorOptions.builder()
                        .setDBUrl(url)
                        .setTableName(readableConfig.get(COLLECTION_NAME))
                        .setParallelism(readableConfig.getOptional(SINK_PARALLELISM).orElse(null))
                        .setConnectionCheckTimeoutSeconds(
                                (int) readableConfig.get(MAX_RETRY_TIMEOUT).getSeconds());
        readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
        readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
        return builder.build();
    }

    private void validateConfigOptions(ReadableConfig config) {
        String mongodbUrl = config.get(URL);

        checkAllOrNone(config, new ConfigOption[] {USERNAME, PASSWORD});

        checkAllOrNone(config, new ConfigOption[] {LOOKUP_CACHE_MAX_ROWS, LOOKUP_CACHE_TTL});

        if (config.get(LOOKUP_MAX_RETRIES) < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option shouldn't be negative, but is %s.",
                            LOOKUP_MAX_RETRIES.key(), config.get(LOOKUP_MAX_RETRIES)));
        }

        if (config.get(SINK_MAX_RETRIES) < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option shouldn't be negative, but is %s.",
                            SINK_MAX_RETRIES.key(), config.get(SINK_MAX_RETRIES)));
        }

        if (config.get(MAX_RETRY_TIMEOUT).getSeconds() <= 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option must be in second granularity and shouldn't be smaller than 1 second, but is %s.",
                            MAX_RETRY_TIMEOUT.key(),
                            config.get(
                                    ConfigOptions.key(MAX_RETRY_TIMEOUT.key())
                                            .stringType()
                                            .noDefaultValue())));
        }
    }

    private void checkAllOrNone(ReadableConfig config, ConfigOption<?>[] configOptions) {
        int presentCount = 0;
        for (ConfigOption configOption : configOptions) {
            if (config.getOptional(configOption).isPresent()) {
                presentCount++;
            }
        }
        String[] propertyNames =
                Arrays.stream(configOptions).map(ConfigOption::key).toArray(String[]::new);
        Preconditions.checkArgument(
                configOptions.length == presentCount || presentCount == 0,
                "Either all or none of the following options should be provided:\n"
                        + String.join("\n", propertyNames));
    }

    private MongodbLookupOptions getJdbcLookupOptions(ReadableConfig readableConfig) {
        return new MongodbLookupOptions(
                readableConfig.get(LOOKUP_CACHE_MAX_ROWS),
                readableConfig.get(LOOKUP_CACHE_TTL).toMillis(),
                readableConfig.get(LOOKUP_MAX_RETRIES),
                readableConfig.get(LOOKUP_CACHE_MISSING_KEY));
    }
}
