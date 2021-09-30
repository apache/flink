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
    private static final String IDENTIFIER = "pubsub";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
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

        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(
                        DeserializationFormatFactory.class, FactoryUtil.FORMAT);

        helper.validate();

        final ReadableConfig options = helper.getOptions();
        final String project = options.get(PubSubConnectorOptions.PROJECT_NAME);
        final String subscription = options.get(PubSubConnectorOptions.SUBSCRIPTION);

        final DataType producedDataType =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        return new PubsubDynamicSource(project, subscription, decodingFormat, producedDataType);
    }
}
