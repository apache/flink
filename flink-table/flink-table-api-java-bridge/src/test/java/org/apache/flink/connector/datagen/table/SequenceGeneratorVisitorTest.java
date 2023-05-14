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

package org.apache.flink.connector.datagen.table;

import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.SequenceGenerator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.FactoryUtil;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;

/** Tests for {@link SequenceGeneratorVisitor}. */
public class SequenceGeneratorVisitorTest {

    @Test
    void testDefaultValueForSequence() {
        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
        descriptor.putString(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.KIND,
                DataGenConnectorOptionsUtil.SEQUENCE);

        DataGenTableSource source =
                (DataGenTableSource)
                        createTableSource(
                                ResolvedSchema.of(Column.physical("f0", DataTypes.BIGINT())),
                                descriptor.asMap());
        DataGenerator<?>[] fieldGenerators = source.getFieldGenerators();
        SequenceGenerator<?> fieldGenerator = (SequenceGenerator<?>) fieldGenerators[0];
        long start = fieldGenerator.getStart();
        long end = fieldGenerator.getEnd();

        Assertions.assertThat(SequenceGeneratorVisitor.START_DEFAULT_VALUE)
                .describedAs(
                        "The default start value should be %s.",
                        SequenceGeneratorVisitor.START_DEFAULT_VALUE)
                .isEqualTo(start);
        Assertions.assertThat(SequenceGeneratorVisitor.END_DEFAULT_VALUE)
                .describedAs(
                        "The default start value should be %s.",
                        SequenceGeneratorVisitor.END_DEFAULT_VALUE)
                .isEqualTo(end);
    }

    @Test
    void testStartEndForSequence() {
        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
        descriptor.putString(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.KIND,
                DataGenConnectorOptionsUtil.SEQUENCE);
        final int setupStart = 10;
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.START,
                setupStart);
        final int setupEnd = 100;
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.END,
                setupEnd);

        DataGenTableSource source =
                (DataGenTableSource)
                        createTableSource(
                                ResolvedSchema.of(Column.physical("f0", DataTypes.BIGINT())),
                                descriptor.asMap());
        DataGenerator<?>[] fieldGenerators = source.getFieldGenerators();
        SequenceGenerator<?> fieldGenerator = (SequenceGenerator<?>) fieldGenerators[0];
        long start = fieldGenerator.getStart();
        long end = fieldGenerator.getEnd();

        Assertions.assertThat(setupStart)
                .describedAs("The default start value should be %s. ", setupStart)
                .isEqualTo(start);
        Assertions.assertThat(setupEnd)
                .describedAs("The default start value should be %s. ", setupEnd)
                .isEqualTo(end);
    }
}
