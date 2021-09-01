package org.apache.flink.streaming.connectors.gcp.pubsub.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

/** Factory for creating {@link PubsubDynamicSource}. */
@Internal
public class PubSubDynamicTableFactory implements DynamicTableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return PubSubConnectorOptions.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PubSubConnectorOptions.PROJECT_NAME);
        options.add(PubSubConnectorOptions.SUBSCRIPTION);
        options.add(FactoryUtil.FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        // discover a suitable decoding format
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(
                        DeserializationFormatFactory.class, FactoryUtil.FORMAT);

        // validate all options
        helper.validate();

        // get the validated options
        final ReadableConfig options = helper.getOptions();
        final String project = options.get(PubSubConnectorOptions.PROJECT_NAME);
        final String subscription = options.get(PubSubConnectorOptions.SUBSCRIPTION);

        // derive the produced data type (excluding computed columns) from the catalog table
        final DataType producedDataType =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        // create and return dynamic table source
        return new PubsubDynamicSource(project, subscription, decodingFormat, producedDataType);
    }
}
