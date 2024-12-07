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

package org.apache.flink.api.java.typeutils.runtime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the @{@link JavaRecordBuilderFactory}. */
class RecordBuilderFactoryTest {

    Field[] fields;

    record TestRecord(int i1, int i2, String s1, String s2) {}

    @BeforeEach
    void setup() {
        fields = TestRecord.class.getDeclaredFields();
    }

    @Test
    void testNoDefaultOrParamMapping() {
        JavaRecordBuilderFactory<TestRecord> helper =
                JavaRecordBuilderFactory.create(TestRecord.class, fields);
        JavaRecordBuilderFactory<TestRecord>.JavaRecordBuilder builder = helper.newBuilder();
        builder.setField(1, 100);
        builder.setField(0, 50);
        builder.setField(3, "test");

        assertThat(builder.build()).isEqualTo(new TestRecord(50, 100, null, "test"));
    }

    @Test
    void testNewFieldsAdded() {
        // Test restoring from fields [i2, s1]
        JavaRecordBuilderFactory<TestRecord> helper =
                JavaRecordBuilderFactory.create(TestRecord.class, Arrays.copyOfRange(fields, 1, 3));

        JavaRecordBuilderFactory<TestRecord>.JavaRecordBuilder builder = helper.newBuilder();
        builder.setField(0, 100);
        builder.setField(1, "test");

        assertThat(builder.build()).isEqualTo(new TestRecord(0, 100, "test", null));
    }

    @Test
    void testFieldsAddedRemovedAndRearranged() {
        Field[] oldFields = new Field[] {fields[3], null, fields[0]};
        JavaRecordBuilderFactory<TestRecord> helper =
                JavaRecordBuilderFactory.create(TestRecord.class, oldFields);

        JavaRecordBuilderFactory<TestRecord>.JavaRecordBuilder builder = helper.newBuilder();
        builder.setField(0, "test");
        builder.setField(2, 100);

        assertThat(builder.build()).isEqualTo(new TestRecord(100, 0, null, "test"));
    }

    @Test
    void testReorderFields() {
        // Swap first and last field
        Field temp = fields[0];
        fields[0] = fields[3];
        fields[3] = temp;

        JavaRecordBuilderFactory<TestRecord> helper =
                JavaRecordBuilderFactory.create(TestRecord.class, fields);

        JavaRecordBuilderFactory<TestRecord>.JavaRecordBuilder builder = helper.newBuilder();
        builder.setField(0, "4");
        builder.setField(1, 2);
        builder.setField(2, "3");
        builder.setField(3, 1);

        assertThat(builder.build()).isEqualTo(new TestRecord(1, 2, "3", "4"));
    }

    @Test
    void testMissingRequiredField() {
        JavaRecordBuilderFactory<TestRecord> helper =
                JavaRecordBuilderFactory.create(TestRecord.class, fields);
        JavaRecordBuilderFactory<TestRecord>.JavaRecordBuilder builder = helper.newBuilder();

        builder.setField(0, 50);
        // Do not set required param 1

        assertThatThrownBy(builder::build)
                .hasMessage("Could not instantiate record")
                .hasCause(new IllegalArgumentException());
    }
}
