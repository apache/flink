/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro;

import org.apache.flink.table.types.logical.RowType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AvroFieldMatcher}. */
class AvroFieldMatcherTest {

    @Test
    void testMatchesReorderedFields() {
        final RowType rowType = rowType(FIELD("a", STRING()), FIELD("b", INT()));
        final Schema schema = parse("{'name':'b','type':'int'}, {'name':'a','type':'string'}");

        final AvroFieldMatcher.Plan plan = AvroFieldMatcher.forSerialization(rowType, schema);

        assertThat(plan.avroPositionOf(0)).isEqualTo(1);
        assertThat(plan.avroPositionOf(1)).isZero();
    }

    @Test
    void testMatchesIgnoringCase() {
        final RowType rowType = rowType(FIELD("firstName", STRING()));
        final Schema schema = parse("{'name':'FIRSTNAME','type':'string'}");

        assertThat(AvroFieldMatcher.forSerialization(rowType, schema).avroPositionOf(0)).isZero();
    }

    @Test
    void testMatchesAvroFieldAlias() {
        final RowType rowType = rowType(FIELD("legacy_name", STRING()));
        final Schema schema = parse("{'name':'name','aliases':['legacy_name'],'type':'string'}");

        assertThat(AvroFieldMatcher.forSerialization(rowType, schema).avroPositionOf(0)).isZero();
    }

    @Test
    void testExactMatchWinsOverCaseInsensitiveMatch() {
        // Both columns match both fields when ignoring case, so only running the exact stage to
        // completion first can pair them up the way the user wrote them.
        final RowType rowType = rowType(FIELD("foo", STRING()), FIELD("FOO", STRING()));
        final Schema schema =
                parse("{'name':'FOO','type':'string'}, {'name':'foo','type':'string'}");

        final AvroFieldMatcher.Plan plan = AvroFieldMatcher.forSerialization(rowType, schema);

        assertThat(plan.avroPositionOf(0)).isEqualTo(1);
        assertThat(plan.avroPositionOf(1)).isZero();
    }

    @Test
    void testAmbiguousCaseInsensitiveMatchIsRejected() {
        final RowType rowType = rowType(FIELD("Foo", STRING()));
        final Schema schema =
                parse("{'name':'foo','type':'string'}, {'name':'FOO','type':'string'}");

        assertThatThrownBy(() -> AvroFieldMatcher.forSerialization(rowType, schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column 'Foo' matches more than one field")
                .hasMessageContaining("a case-insensitive comparison");
    }

    @Test
    void testTwoColumnsCompetingForOneFieldAreRejected() {
        final RowType rowType = rowType(FIELD("Foo", STRING()), FIELD("foo", STRING()));
        final Schema schema = parse("{'name':'Foo','type':'string'}");

        assertThatThrownBy(() -> AvroFieldMatcher.forSerialization(rowType, schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Columns 'Foo' and 'foo' both match field 'Foo'");
    }

    @Test
    void testSerializationRejectsColumnWithoutAvroField() {
        final RowType rowType = rowType(FIELD("a", STRING()), FIELD("missing", INT()));
        final Schema schema = parse("{'name':'a','type':'string'}");

        assertThatThrownBy(() -> AvroFieldMatcher.forSerialization(rowType, schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column 'missing' cannot be written")
                .hasMessageContaining("fields: a");
    }

    @Test
    void testSerializationRejectsUnwritableRequiredAvroField() {
        final RowType rowType = rowType(FIELD("a", STRING()));
        final Schema schema =
                parse("{'name':'a','type':'string'}, {'name':'required','type':'int'}");

        assertThatThrownBy(() -> AvroFieldMatcher.forSerialization(rowType, schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Field 'required'")
                .hasMessageContaining("neither nullable nor does it declare a default value");
    }

    @Test
    void testSerializationToleratesUnmatchedNullableAvroField() {
        final RowType rowType = rowType(FIELD("a", STRING()));
        final Schema schema =
                parse("{'name':'a','type':'string'}, {'name':'spare','type':['null','int']}");

        final AvroFieldMatcher.Plan plan = AvroFieldMatcher.forSerialization(rowType, schema);

        final GenericRecord record = new GenericData.Record(schema);
        plan.fillDefaults(record);
        assertThat(record.get("spare")).isNull();
    }

    @Test
    void testSerializationAppliesDefaultOfUnmatchedAvroField() {
        final RowType rowType = rowType(FIELD("a", STRING()));
        final Schema schema =
                parse("{'name':'a','type':'string'}, {'name':'version','type':'int','default':7}");

        final AvroFieldMatcher.Plan plan = AvroFieldMatcher.forSerialization(rowType, schema);

        final GenericRecord record = new GenericData.Record(schema);
        plan.fillDefaults(record);
        assertThat(record.get("version")).isEqualTo(7);
    }

    @Test
    void testDeserializationLeavesNullableColumnUnmatched() {
        final RowType rowType = rowType(FIELD("a", STRING()), FIELD("absent", INT()));
        final Schema schema = parse("{'name':'a','type':'string'}");

        final AvroFieldMatcher.Plan plan = AvroFieldMatcher.forDeserialization(rowType, schema);

        assertThat(plan.avroPositionOf(0)).isZero();
        assertThat(plan.avroPositionOf(1)).isEqualTo(AvroFieldMatcher.UNMATCHED);
    }

    @Test
    void testDeserializationRejectsUnmatchedNotNullColumn() {
        final RowType rowType = rowType(FIELD("a", STRING()), FIELD("absent", INT().notNull()));
        final Schema schema = parse("{'name':'a','type':'string'}");

        assertThatThrownBy(() -> AvroFieldMatcher.forDeserialization(rowType, schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column 'absent' is declared NOT NULL")
                .hasMessageContaining("could only be read as NULL");
    }

    @Test
    void testDeserializationIgnoresUnreadAvroField() {
        final RowType rowType = rowType(FIELD("a", STRING()));
        final Schema schema = parse("{'name':'a','type':'string'}, {'name':'extra','type':'int'}");

        assertThat(AvroFieldMatcher.forDeserialization(rowType, schema).avroPositionOf(0)).isZero();
    }

    @Test
    void testNonRecordSchemaIsRejected() {
        final RowType rowType = rowType(FIELD("a", STRING()));

        assertThatThrownBy(
                        () ->
                                AvroFieldMatcher.forSerialization(
                                        rowType, Schema.create(Schema.Type.STRING)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires an Avro RECORD schema");
    }

    @Test
    void testPlanIsBoundToTheSchemaItWasResolvedFor() {
        final RowType rowType = rowType(FIELD("a", STRING()), FIELD("b", BOOLEAN()));
        final Schema schema = parse("{'name':'a','type':'string'}, {'name':'b','type':'boolean'}");
        final Schema equalButDistinct =
                parse("{'name':'a','type':'string'}, {'name':'b','type':'boolean'}");

        final AvroFieldMatcher.Plan plan = AvroFieldMatcher.forSerialization(rowType, schema);

        assertThat(plan.appliesTo(schema)).isTrue();
        assertThat(plan.appliesTo(equalButDistinct)).isFalse();
    }

    private static RowType rowType(org.apache.flink.table.api.DataTypes.Field... fields) {
        return (RowType) ROW(fields).notNull().getLogicalType();
    }

    /** Parses a record schema from the given field list, using {@code '} instead of {@code "}. */
    private static Schema parse(String fields) {
        return new Schema.Parser()
                .parse(
                        ("{'type':'record','name':'TestRecord','fields':[" + fields + "]}")
                                .replace('\'', '"'));
    }
}
