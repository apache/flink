package org.apache.flink.mongodb.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.mongodb.table.sink.MongodbDynamicTableSink;
import org.apache.flink.mongodb.table.sink.MongodbSinkConf;
import org.apache.flink.mongodb.table.source.MongodbDynamicTableSource;
import org.apache.flink.mongodb.table.util.ContextUtil;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.mongodb.table.MongodbConnectorOptions.BATCH_SIZE;
import static org.apache.flink.mongodb.table.MongodbConnectorOptions.COLLECTION_NAME;
import static org.apache.flink.mongodb.table.MongodbConnectorOptions.DATABASE;
import static org.apache.flink.mongodb.table.MongodbConnectorOptions.FORMAT;
import static org.apache.flink.mongodb.table.MongodbConnectorOptions.MAX_CONNECTION_IDLE_TIME;
import static org.apache.flink.mongodb.table.MongodbConnectorOptions.PASSWORD;
import static org.apache.flink.mongodb.table.MongodbConnectorOptions.RETRY_WRITES;
import static org.apache.flink.mongodb.table.MongodbConnectorOptions.SERVER;
import static org.apache.flink.mongodb.table.MongodbConnectorOptions.TIMEOUT;
import static org.apache.flink.mongodb.table.MongodbConnectorOptions.URI;

public class MongodbDynamicTableSourceSinkFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {
    public static final String IDENTIFIER = "mongodb";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        ResolvedSchema physicalSchema = context.getCatalogTable().getResolvedSchema();

        ContextUtil.transformContext(this, context);
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        MongodbSinkConf mongodbSinkConf = new MongodbSinkConf((String) helper
                .getOptions()
                .get(DATABASE),
                (String) helper.getOptions().get(COLLECTION_NAME),
                (String) helper.getOptions().get(URI),
                ((Integer) helper.getOptions().get(MAX_CONNECTION_IDLE_TIME)).intValue(),
                ((Integer) helper.getOptions().get(BATCH_SIZE)).intValue());
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
        requiredOptions.add(SERVER);
        requiredOptions.add(PASSWORD);
        requiredOptions.add(COLLECTION_NAME);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionals = new HashSet();
        optionals.add(MAX_CONNECTION_IDLE_TIME);
        optionals.add(BATCH_SIZE);
        optionals.add(RETRY_WRITES);
        optionals.add(TIMEOUT);
        return optionals;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        ContextUtil.transformContext(this, context);
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        MongodbSinkConf mongodbSinkConf = new MongodbSinkConf(
                (String) helper
                        .getOptions()
                        .get(DATABASE),
                (String) helper.getOptions().get(COLLECTION_NAME),
                (String) helper.getOptions().get(URI),
                ((Integer) helper.getOptions().get(MAX_CONNECTION_IDLE_TIME)).intValue(),
                ((Integer) helper.getOptions().get(BATCH_SIZE)).intValue());

        ResolvedSchema physicalSchema = context.getCatalogTable().getResolvedSchema();

        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(
                        DeserializationFormatFactory.class, FORMAT);
        return new MongodbDynamicTableSource(mongodbSinkConf, physicalSchema, decodingFormat);
    }

}
