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

package org.apache.flink.connector.mongodb.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MongoKeyExtractor}. */
public class MongoKeyExtractorTest {

    @Test
    public void testSimpleKey() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.BIGINT().notNull()),
                                Column.physical("b", DataTypes.STRING())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("a")));

        Function<RowData, BsonValue> keyExtractor = MongoKeyExtractor.createKeyExtractor(schema);

        BsonValue key = keyExtractor.apply(GenericRowData.of(12L, StringData.fromString("ABCD")));
        assertThat(key).isEqualTo(new BsonInt64(12L));
    }

    @Test
    public void testPrimaryKeyWithReversedId() {
        ResolvedSchema schema0 =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("_id", DataTypes.STRING().notNull()),
                                Column.physical("b", DataTypes.STRING())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("_id, a")));

        assertThatThrownBy(() -> MongoKeyExtractor.createKeyExtractor(schema0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "The primary key should be declared as (_id) when mongo reversed _id field is present");

        ResolvedSchema schema1 =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("_id", DataTypes.STRING().notNull()),
                                Column.physical("b", DataTypes.STRING())),
                        Collections.emptyList(),
                        null);

        assertThatThrownBy(() -> MongoKeyExtractor.createKeyExtractor(schema1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "The primary key should be declared as (_id) when mongo reversed _id field is present");

        ResolvedSchema schema2 =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("_id", DataTypes.STRING().notNull()),
                                Column.physical("b", DataTypes.STRING())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("_id")));

        Function<RowData, BsonValue> keyExtractor = MongoKeyExtractor.createKeyExtractor(schema2);

        ObjectId objectId = new ObjectId();
        BsonValue key =
                keyExtractor.apply(
                        GenericRowData.of(
                                StringData.fromString(objectId.toHexString()),
                                StringData.fromString("ABCD")));
        assertThat(key).isEqualTo(new BsonObjectId(objectId));
    }

    @Test
    public void testNoPrimaryKey() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.BIGINT().notNull()),
                                Column.physical("b", DataTypes.STRING())),
                        Collections.emptyList(),
                        null);

        Function<RowData, BsonValue> keyExtractor = MongoKeyExtractor.createKeyExtractor(schema);

        BsonValue key = keyExtractor.apply(GenericRowData.of(12L, StringData.fromString("ABCD")));
        assertThat(key).isNull();
    }

    @Test
    public void testTwoFieldsKey() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.BIGINT().notNull()),
                                Column.physical("b", DataTypes.STRING()),
                                Column.physical("c", DataTypes.TIMESTAMP().notNull())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Arrays.asList("a", "b")));

        Function<RowData, BsonValue> keyExtractor = MongoKeyExtractor.createKeyExtractor(schema);

        BsonValue key =
                keyExtractor.apply(
                        GenericRowData.of(
                                12L,
                                StringData.fromString("ABCD"),
                                TimestampData.fromLocalDateTime(
                                        LocalDateTime.parse("2012-12-12T12:12:12"))));

        BsonDocument expect = new BsonDocument();
        expect.append("a", new BsonInt64(12L));
        expect.append("b", new BsonString("ABCD"));

        assertThat(key).isEqualTo(expect);
    }

    @Test
    public void testAllTypesKey() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.TINYINT().notNull()),
                                Column.physical("b", DataTypes.SMALLINT().notNull()),
                                Column.physical("c", DataTypes.INT().notNull()),
                                Column.physical("d", DataTypes.BIGINT().notNull()),
                                Column.physical("e", DataTypes.BOOLEAN().notNull()),
                                Column.physical("f", DataTypes.FLOAT().notNull()),
                                Column.physical("g", DataTypes.DOUBLE().notNull()),
                                Column.physical("h", DataTypes.STRING().notNull()),
                                Column.physical("i", DataTypes.TIMESTAMP().notNull()),
                                Column.physical(
                                        "j", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE().notNull())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey(
                                "pk",
                                Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")));

        Function<RowData, BsonValue> keyExtractor = MongoKeyExtractor.createKeyExtractor(schema);

        BsonValue key =
                keyExtractor.apply(
                        GenericRowData.of(
                                (byte) 1,
                                (short) 2,
                                3,
                                (long) 4,
                                true,
                                1.0f,
                                2.0d,
                                StringData.fromString("ABCD"),
                                TimestampData.fromLocalDateTime(
                                        LocalDateTime.parse("2012-12-12T12:12:12")),
                                TimestampData.fromInstant(Instant.parse("2013-01-13T13:13:13Z"))));

        BsonDocument expect = new BsonDocument();
        expect.append("a", new BsonInt32(1));
        expect.append("b", new BsonInt32(2));
        expect.append("c", new BsonInt32(3));
        expect.append("d", new BsonInt64(4L));
        expect.append("e", new BsonBoolean(true));
        expect.append("f", new BsonDouble(Float.valueOf("1.0f").doubleValue()));
        expect.append("g", new BsonDouble(2.0d));
        expect.append("h", new BsonString("ABCD"));
        expect.append(
                "i",
                new BsonDateTime(
                        LocalDateTime.parse("2012-12-12T12:12:12")
                                .atZone(ZoneId.systemDefault())
                                .toInstant()
                                .toEpochMilli()));
        expect.append("j", new BsonDateTime(Instant.parse("2013-01-13T13:13:13Z").toEpochMilli()));

        assertThat(key).isEqualTo(expect);
    }
}
