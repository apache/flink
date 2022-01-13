/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.firehose.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.table.AsyncDynamicTableSinkFactory;
import org.apache.flink.connector.firehose.table.util.KinesisFirehoseConnectorOptionUtils;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connector.firehose.table.KinesisFirehoseConnectorOptions.DELIVERY_STREAM;
import static org.apache.flink.connector.firehose.table.KinesisFirehoseConnectorOptions.SINK_FAIL_ON_ERROR;
import static org.apache.flink.connector.firehose.table.util.KinesisFirehoseConnectorOptionUtils.FIREHOSE_CLIENT_PROPERTIES_KEY;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;

/** Factory for creating {@link KinesisFirehoseDynamicSink} . */
@Internal
public class KinesisFirehoseDynamicTableFactory extends AsyncDynamicTableSinkFactory {

    public static final String IDENTIFIER = "firehose";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig tableOptions = helper.getOptions();
        ResolvedCatalogTable catalogTable = context.getCatalogTable();
        DataType physicalDataType = catalogTable.getResolvedSchema().toPhysicalRowDataType();

        // initialize the table format early in order to register its consumedOptionKeys
        // in the TableFactoryHelper, as those are needed for correct option validation
        EncodingFormat<SerializationSchema<RowData>> encodingFormat =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, FORMAT);

        KinesisFirehoseDynamicSink.KinesisDataFirehoseDynamicSinkBuilder builder =
                new KinesisFirehoseDynamicSink.KinesisDataFirehoseDynamicSinkBuilder();

        KinesisFirehoseConnectorOptionUtils optionsUtils =
                new KinesisFirehoseConnectorOptionUtils(catalogTable.getOptions(), tableOptions);
        // validate the data types of the table options
        helper.validateExcept(optionsUtils.getNonValidatedPrefixes().toArray(new String[0]));
        Properties properties = optionsUtils.getSinkProperties();

        builder.setDeliveryStream((String) properties.get(DELIVERY_STREAM.key()))
                .setFirehoseClientProperties(
                        (Properties) properties.get(FIREHOSE_CLIENT_PROPERTIES_KEY))
                .setEncodingFormat(encodingFormat)
                .setConsumedDataType(physicalDataType);
        Optional.ofNullable((Boolean) properties.get(SINK_FAIL_ON_ERROR.key()))
                .ifPresent(builder::setFailOnError);
        return super.addAsyncOptionsToBuilder(properties, builder).build();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DELIVERY_STREAM);
        options.add(FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = super.optionalOptions();
        options.add(SINK_FAIL_ON_ERROR);
        return options;
    }
}
