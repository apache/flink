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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.createKeyFormatProjection;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.createValueFormatProjection;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link KafkaConnectorOptionsUtil}. */
public class KafkaConnectorOptionsUtilTest {

    @Test
    public void testFormatProjection() {
        final DataType dataType =
                DataTypes.ROW(
                        FIELD("id", INT()),
                        FIELD("name", STRING()),
                        FIELD("age", INT()),
                        FIELD("address", STRING()));

        final Map<String, String> options = createTestOptions();
        options.put("key.fields", "address; name");
        options.put("value.fields-include", "EXCEPT_KEY");

        final Configuration config = Configuration.fromMap(options);

        assertThat(createKeyFormatProjection(config, dataType)).isEqualTo(new int[] {3, 1});
        assertThat(createValueFormatProjection(config, dataType)).isEqualTo(new int[] {0, 2});
    }

    @Test
    public void testMissingKeyFormatProjection() {
        final DataType dataType = ROW(FIELD("id", INT()));
        final Map<String, String> options = createTestOptions();

        final Configuration config = Configuration.fromMap(options);

        assertThatThrownBy(() -> createKeyFormatProjection(config, dataType))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "A key format 'key.format' requires the declaration of one or more "
                                + "of key fields using 'key.fields'.");
    }

    @Test
    public void testInvalidKeyFormatFieldProjection() {
        final DataType dataType = ROW(FIELD("id", INT()), FIELD("name", STRING()));
        final Map<String, String> options = createTestOptions();
        options.put("key.fields", "non_existing");

        final Configuration config = Configuration.fromMap(options);

        assertThatThrownBy(() -> createKeyFormatProjection(config, dataType))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Could not find the field 'non_existing' in the table schema for "
                                + "usage in the key format. A key field must be a regular, "
                                + "physical column. The following columns can be selected "
                                + "in the 'key.fields' option:\n"
                                + "[id, name]");
    }

    @Test
    public void testInvalidKeyFormatPrefixProjection() {
        final DataType dataType =
                ROW(FIELD("k_part_1", INT()), FIELD("part_2", STRING()), FIELD("name", STRING()));
        final Map<String, String> options = createTestOptions();
        options.put("key.fields", "k_part_1;part_2");
        options.put("key.fields-prefix", "k_");

        final Configuration config = Configuration.fromMap(options);

        assertThatThrownBy(() -> createKeyFormatProjection(config, dataType))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "All fields in 'key.fields' must be prefixed with 'k_' when option "
                                + "'key.fields-prefix' is set but field 'part_2' is not prefixed.");
    }

    @Test
    public void testInvalidValueFormatProjection() {
        final DataType dataType = ROW(FIELD("k_id", INT()), FIELD("id", STRING()));
        final Map<String, String> options = createTestOptions();
        options.put("key.fields", "k_id");
        options.put("key.fields-prefix", "k_");

        final Configuration config = Configuration.fromMap(options);

        assertThatThrownBy(() -> createValueFormatProjection(config, dataType))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "A key prefix is not allowed when option 'value.fields-include' "
                                + "is set to 'ALL'. Set it to 'EXCEPT_KEY' instead to avoid field overlaps.");
    }

    // --------------------------------------------------------------------------------------------

    private static Map<String, String> createTestOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("key.format", "test-format");
        options.put("key.test-format.delimiter", ",");
        options.put("value.format", "test-format");
        options.put("value.test-format.delimiter", "|");
        options.put("value.test-format.fail-on-missing", "true");
        return options;
    }
}
