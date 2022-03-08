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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.connector.base.table.AsyncDynamicTableSinkFactory;
import org.apache.flink.connector.firehose.table.util.KinesisFirehoseConnectorOptionUtils;
import org.apache.flink.table.connector.sink.DynamicTableSink;

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

        AsyncDynamicSinkContext factoryContext = new AsyncDynamicSinkContext(this, context);

        KinesisFirehoseDynamicSink.KinesisFirehoseDynamicSinkBuilder builder =
                new KinesisFirehoseDynamicSink.KinesisFirehoseDynamicSinkBuilder();

        KinesisFirehoseConnectorOptionUtils optionsUtils =
                new KinesisFirehoseConnectorOptionUtils(
                        factoryContext.getResolvedOptions(), factoryContext.getTableOptions());
        // validate the data types of the table options
        factoryContext
                .getFactoryHelper()
                .validateExcept(optionsUtils.getNonValidatedPrefixes().toArray(new String[0]));
        Properties properties = optionsUtils.getSinkProperties();

        builder.setDeliveryStream((String) properties.get(DELIVERY_STREAM.key()))
                .setFirehoseClientProperties(
                        (Properties) properties.get(FIREHOSE_CLIENT_PROPERTIES_KEY))
                .setEncodingFormat(factoryContext.getEncodingFormat())
                .setConsumedDataType(factoryContext.getPhysicalDataType());
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
