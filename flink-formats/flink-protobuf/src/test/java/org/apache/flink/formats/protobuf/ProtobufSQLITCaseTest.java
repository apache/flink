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

package org.apache.flink.formats.protobuf;

import org.apache.flink.formats.protobuf.table.TestProtobufTestStore;
import org.apache.flink.formats.protobuf.testproto.MapTest;
import org.apache.flink.formats.protobuf.testproto.Pb3Test;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/** Integration SQL test for protobuf. */
public class ProtobufSQLITCaseTest extends BatchTestBase {

    private MapTest getProtoTestObject() {
        MapTest.InnerMessageTest innerMessageTest =
                MapTest.InnerMessageTest.newBuilder().setA(1).setB(2).build();
        MapTest mapTest =
                MapTest.newBuilder()
                        .setA(1)
                        .putMap1("a", "b")
                        .putMap1("c", "d")
                        .putMap2("f", innerMessageTest)
                        .build();
        return mapTest;
    }

    @Test
    public void testSource() {
        TestProtobufTestStore.sourcePbInputs.clear();
        TestProtobufTestStore.sourcePbInputs.add(getProtoTestObject().toByteArray());

        env().setParallelism(1);
        String sql =
                "create table bigdata_source ( "
                        + "	a int, "
                        + "	map1 map<string,string>,"
                        + " map2 map<string, row<a int, b bigint>>"
                        + ")  with ("
                        + "	'connector' = 'protobuf-test-connector', "
                        + "	'format' = 'protobuf', "
                        + " 'protobuf.message-class-name' = 'org.apache.flink.formats.protobuf.testproto.MapTest'"
                        + ")";
        tEnv().executeSql(sql);
        TableResult result = tEnv().executeSql("select * from bigdata_source");
        Row row = result.collect().next();
        assertEquals(1, (int) row.getField(0));
        Map<String, String> map1 = (Map<String, String>) row.getField(1);
        assertEquals("b", map1.get("a"));
        assertEquals("d", map1.get("c"));
        Map<String, Row> map2 = (Map<String, Row>) row.getField(2);
        Row innerRow = map2.get("f");
        assertEquals(1, innerRow.getField(0));
        assertEquals(2L, innerRow.getField(1));
    }

    @Test
    public void testSourceNotIgnoreParseError() throws InterruptedException {
        TestProtobufTestStore.sourcePbInputs.clear();
        // pass an incompatible bytes
        TestProtobufTestStore.sourcePbInputs.add(new byte[] {127, 127, 127, 127, 127});

        env().setParallelism(1);
        String sql =
                "create table bigdata_source ( "
                        + "	a int, "
                        + "	map1 map<string,string>,"
                        + " map2 map<string, row<a int, b bigint>>"
                        + ")  with ("
                        + "	'connector' = 'protobuf-test-connector', "
                        + "	'format' = 'protobuf', "
                        + " 'protobuf.message-class-name' = 'org.apache.flink.formats.protobuf.testproto.MapTest'"
                        + ")";
        tEnv().executeSql(sql);
        TableResult result = tEnv().executeSql("select * from bigdata_source");
        try {
            result.await();
        } catch (Exception ex) {
            return;
        }
        fail("executeSql should raise exception");
    }

    @Test
    public void testSourceIgnoreParseError() throws InterruptedException, ExecutionException {
        TestProtobufTestStore.sourcePbInputs.clear();
        // pass an incompatible bytes
        TestProtobufTestStore.sourcePbInputs.add(new byte[] {127, 127, 127, 127, 127});

        env().setParallelism(1);
        String sql =
                "create table bigdata_source ( "
                        + "	a int, "
                        + "	map1 map<string,string>,"
                        + " map2 map<string, row<a int, b bigint>>"
                        + ")  with ("
                        + "	'connector' = 'protobuf-test-connector', "
                        + "	'format' = 'protobuf', "
                        + " 'protobuf.message-class-name' = 'org.apache.flink.formats.protobuf.testproto.MapTest',"
                        + " 'protobuf.ignore-parse-errors' = 'true'"
                        + ")";
        tEnv().executeSql(sql);
        TableResult result = tEnv().executeSql("select * from bigdata_source");
        CloseableIterator<Row> iterator = result.collect();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testSourceWithDefaultValueOfPb2WhenTrue() {
        MapTest mapTest = MapTest.newBuilder().build();

        TestProtobufTestStore.sourcePbInputs.clear();
        TestProtobufTestStore.sourcePbInputs.add(mapTest.toByteArray());

        env().setParallelism(1);
        String sql =
                "create table bigdata_source ( "
                        + "	a int, "
                        + "	map1 map<string,string>,"
                        + " map2 map<string, row<a int, b bigint>>"
                        + ")  with ("
                        + "	'connector' = 'protobuf-test-connector', "
                        + "	'format' = 'protobuf', "
                        + " 'protobuf.message-class-name' = 'org.apache.flink.formats.protobuf.testproto.MapTest', "
                        + " 'protobuf.read-default-values' = 'true' "
                        + ")";
        tEnv().executeSql(sql);
        TableResult result = tEnv().executeSql("select * from bigdata_source");
        Row row = result.collect().next();
        assertEquals(0, (int) row.getField(0));
    }

    @Test
    public void testSourceWithDefaultValueOfPb2WhenFalse() {
        MapTest mapTest = MapTest.newBuilder().build();

        TestProtobufTestStore.sourcePbInputs.clear();
        TestProtobufTestStore.sourcePbInputs.add(mapTest.toByteArray());

        env().setParallelism(1);
        String sql =
                "create table bigdata_source ( "
                        + "	a int, "
                        + "	map1 map<string,string>,"
                        + " map2 map<string, row<a int, b bigint>>"
                        + ")  with ("
                        + "	'connector' = 'protobuf-test-connector', "
                        + "	'format' = 'protobuf', "
                        + " 'protobuf.message-class-name' = 'org.apache.flink.formats.protobuf.testproto.MapTest', "
                        + " 'protobuf.read-default-values' = 'false' "
                        + ")";
        tEnv().executeSql(sql);
        TableResult result = tEnv().executeSql("select * from bigdata_source");
        Row row = result.collect().next();
        assertNull(row.getField(0));
    }

    @Test
    public void testSourceWithDefaultValueOfPb3WhenTrue() {
        Pb3Test pb3Test = Pb3Test.newBuilder().build();

        TestProtobufTestStore.sourcePbInputs.clear();
        TestProtobufTestStore.sourcePbInputs.add(pb3Test.toByteArray());
        env().setParallelism(1);
        String sql =
                "create table bigdata_source ( "
                        + "	a int,"
                        + "	b bigint,"
                        + " c string,"
                        + " d float"
                        + ")  with ("
                        + "	'connector' = 'protobuf-test-connector', "
                        + "	'format' = 'protobuf', "
                        + " 'protobuf.message-class-name' = 'org.apache.flink.formats.protobuf.testproto.Pb3Test', "
                        + " 'protobuf.read-default-values' = 'true' "
                        + ")";
        tEnv().executeSql(sql);
        TableResult result = tEnv().executeSql("select * from bigdata_source");
        Row row = result.collect().next();
        assertEquals(0, (int) row.getField(0));
    }

    @Test
    public void testSourceWithDefaultValueOfPb3WhenFalse() {
        Pb3Test pb3Test = Pb3Test.newBuilder().build();

        TestProtobufTestStore.sourcePbInputs.clear();
        TestProtobufTestStore.sourcePbInputs.add(pb3Test.toByteArray());
        env().setParallelism(1);
        String sql =
                "create table bigdata_source ( "
                        + "	a int,"
                        + "	b bigint,"
                        + " c string,"
                        + " d float"
                        + ")  with ("
                        + "	'connector' = 'protobuf-test-connector', "
                        + "	'format' = 'protobuf', "
                        + " 'protobuf.message-class-name' = 'org.apache.flink.formats.protobuf.testproto.Pb3Test', "
                        + " 'protobuf.read-default-values' = 'false' "
                        + ")";
        tEnv().executeSql(sql);
        TableResult result = tEnv().executeSql("select * from bigdata_source");
        Row row = result.collect().next();
        assertEquals(0, (int) row.getField(0));
    }

    @Test
    public void testSink() throws Exception {
        TestProtobufTestStore.sourcePbInputs.clear();
        TestProtobufTestStore.sourcePbInputs.add(getProtoTestObject().toByteArray());
        TestProtobufTestStore.sinkResults.clear();

        env().setParallelism(1);
        String sql =
                "create table bigdata_sink ( "
                        + "	a int, "
                        + "	map1 map<string,string>,"
                        + " map2 map<string, row<a int, b bigint>>"
                        + ")  with ("
                        + "	'connector' = 'protobuf-test-connector', "
                        + "	'format' = 'protobuf', "
                        + " 'protobuf.message-class-name' = 'org.apache.flink.formats.protobuf.testproto.MapTest'"
                        + ")";
        tEnv().executeSql(sql);
        TableResult tableResult =
                tEnv().executeSql(
                                "insert into bigdata_sink select 1, map['a', 'b', 'c', 'd'], map['f', row(1,cast(2 as bigint))] ");
        tableResult.await();

        byte[] bytes = TestProtobufTestStore.sinkResults.get(0);
        MapTest mapTest = MapTest.parseFrom(bytes);
        assertEquals(1, mapTest.getA());
        assertEquals("b", mapTest.getMap1Map().get("a"));
        assertEquals("d", mapTest.getMap1Map().get("c"));
        MapTest.InnerMessageTest innerMessageTest = mapTest.getMap2Map().get("f");
        assertEquals(1, innerMessageTest.getA());
        assertEquals(2L, innerMessageTest.getB());
    }

    @Test
    public void testSinkWithNullLiteral() throws Exception {
        TestProtobufTestStore.sourcePbInputs.clear();
        TestProtobufTestStore.sourcePbInputs.add(getProtoTestObject().toByteArray());
        TestProtobufTestStore.sinkResults.clear();

        env().setParallelism(1);
        String sql =
                "create table bigdata_sink ( "
                        + "	a int, "
                        + "	map1 map<string,string>,"
                        + " map2 map<string, row<a int, b bigint>>"
                        + ")  with ("
                        + "	'connector' = 'protobuf-test-connector', "
                        + "	'format' = 'protobuf', "
                        + " 'protobuf.message-class-name' = 'org.apache.flink.formats.protobuf.testproto.MapTest', "
                        + " 'protobuf.write-null-string-literal' = 'NULL' "
                        + ")";
        tEnv().executeSql(sql);
        TableResult tableResult =
                tEnv().executeSql(
                                "insert into bigdata_sink select 1, map['a', null], map['b', cast(null as row<a int, b bigint>)]");
        tableResult.await();

        byte[] bytes = TestProtobufTestStore.sinkResults.get(0);
        MapTest mapTest = MapTest.parseFrom(bytes);
        assertEquals(1, mapTest.getA());
        assertEquals("NULL", mapTest.getMap1Map().get("a"));
        MapTest.InnerMessageTest innerMessageTest = mapTest.getMap2Map().get("b");
        assertEquals(MapTest.InnerMessageTest.getDefaultInstance(), innerMessageTest);
    }

    @Test
    public void testSinkWithNullLiteralWithEscape() throws Exception {
        TestProtobufTestStore.sourcePbInputs.clear();
        TestProtobufTestStore.sourcePbInputs.add(getProtoTestObject().toByteArray());
        TestProtobufTestStore.sinkResults.clear();

        env().setParallelism(1);
        String sql =
                "create table bigdata_sink ( "
                        + "	a int, "
                        + "	map1 map<string,string>,"
                        + " map2 map<string, row<a int, b bigint>>"
                        + ")  with ("
                        + "	'connector' = 'protobuf-test-connector', "
                        + "	'format' = 'protobuf', "
                        + " 'protobuf.message-class-name' = 'org.apache.flink.formats.protobuf.testproto.MapTest', "
                        + " 'protobuf.write-null-string-literal' = '\\\"NULL\\\"' "
                        + ")";
        tEnv().executeSql(sql);
        TableResult tableResult =
                tEnv().executeSql(
                                "insert into bigdata_sink select 1, map['a', null], map['b', cast(null as row<a int, b bigint>)]");
        tableResult.await();

        byte[] bytes = TestProtobufTestStore.sinkResults.get(0);
        MapTest mapTest = MapTest.parseFrom(bytes);
        assertEquals(1, mapTest.getA());
        assertEquals("\"NULL\"", mapTest.getMap1Map().get("a"));
        MapTest.InnerMessageTest innerMessageTest = mapTest.getMap2Map().get("b");
        assertEquals(MapTest.InnerMessageTest.getDefaultInstance(), innerMessageTest);
    }

    @Test
    public void testUnsupportedBulkFilesystemSink() {
        env().setParallelism(1);
        String sql =
                "create table bigdata_sink ( "
                        + "	a int, "
                        + "	map1 map<string,string>,"
                        + " map2 map<string, row<a int, b bigint>>"
                        + ")  with ("
                        + "	'connector' = 'filesystem', "
                        + "	'path' = '/tmp/unused', "
                        + "	'format' = 'protobuf', "
                        + " 'protobuf.message-class-name' = 'org.apache.flink.formats.protobuf.testproto.MapTest'"
                        + ")";
        tEnv().executeSql(sql);

        assertThatThrownBy(
                        () -> {
                            TableResult tableResult =
                                    tEnv().executeSql(
                                                    "insert into bigdata_sink select 1, map['a', 'b', 'c', 'd'], map['f', row(1,cast(2 as bigint))] ");
                            tableResult.await();
                        })
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "The 'protobuf' format is not supported for the 'filesystem' connector."));
    }
}
