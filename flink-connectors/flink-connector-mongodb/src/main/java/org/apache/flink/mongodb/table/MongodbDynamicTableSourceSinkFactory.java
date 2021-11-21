package org.apache.flink.mongodb.table;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.mongodb.table.sink.MongodbDynamicTableSink;
import org.apache.flink.mongodb.table.sink.MongodbSinkConf;
import org.apache.flink.mongodb.table.source.MongodbDynamicTableSource;
import org.apache.flink.mongodb.table.util.ContextUtil;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class MongodbDynamicTableSourceSinkFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MongodbDynamicTableSourceSinkFactory.class);
    @VisibleForTesting
    public static final String IDENTIFIER = "mongodb";
    public static final ConfigOption<String> DATABASE = ConfigOptions.key("database".toLowerCase())
            .stringType()
            .noDefaultValue()
            .withDescription("The data base to connect.");
    public static final ConfigOption<String> URI = ConfigOptions.key("uri".toLowerCase())
            .stringType()
            .noDefaultValue()
            .withDescription("The uri to connect.");
    public static final ConfigOption<String> COLLECTION_NAME = ConfigOptions.key("collection".toLowerCase())
            .stringType()
            .noDefaultValue()
            .withDescription("The name of the collection to return.");
    public static final ConfigOption<Integer> MAX_CONNECTION_IDLE_TIME = ConfigOptions.key("maxConnectionIdleTime".toLowerCase())
            .intType()
            .defaultValue(Integer.valueOf(60000))
            .withDescription("The maximum idle time for a pooled connection.");
    public static final ConfigOption<Integer> BATCH_SIZE = ConfigOptions.key("batchSize".toLowerCase())
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

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        ContextUtil.transformContext(this, context);
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        MongodbSinkConf mongodbSinkConf = new MongodbSinkConf((String) helper.getOptions().get(DATABASE), (String) helper.getOptions().get(COLLECTION_NAME), (String) helper.getOptions().get(URI), ((Integer) helper.getOptions().get(MAX_CONNECTION_IDLE_TIME)).intValue(), ((Integer) helper.getOptions().get(BATCH_SIZE)).intValue());

        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        LOG.info("Create dynamic mongoDB table table: {}.", mongodbSinkConf);
        return new MongodbDynamicTableSink(mongodbSinkConf, physicalSchema);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet();
        requiredOptions.add(DATABASE);
        requiredOptions.add(COLLECTION_NAME);
        requiredOptions.add(URI);
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
    public DynamicTableSource createDynamicTableSource(Context context) {
        ContextUtil.transformContext(this, context);
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        MongodbSinkConf mongodbSinkConf = new MongodbSinkConf((String) helper.getOptions().get(DATABASE), (String) helper.getOptions().get(COLLECTION_NAME), (String) helper.getOptions().get(URI), ((Integer) helper.getOptions().get(MAX_CONNECTION_IDLE_TIME)).intValue(), ((Integer) helper.getOptions().get(BATCH_SIZE)).intValue());

        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        LOG.info("Create dynamic mongoDB table table: {}.", mongodbSinkConf);

        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(
                        DeserializationFormatFactory.class, FORMAT);
        return new MongodbDynamicTableSource(mongodbSinkConf, physicalSchema, decodingFormat);
    }

}
