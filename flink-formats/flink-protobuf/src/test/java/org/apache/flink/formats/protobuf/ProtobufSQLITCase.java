package org.apache.flink.formats.protobuf;

import org.apache.flink.formats.protobuf.table.TestProtobufSinkFunction;
import org.apache.flink.formats.protobuf.table.TestProtobufSourceFunction;
import org.apache.flink.formats.protobuf.testproto.MapTest;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/** Integration SQL test for protobuf. */
public class ProtobufSQLITCase extends BatchTestBase {

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
        TestProtobufSourceFunction.messages.clear();
        TestProtobufSourceFunction.messages.add(getProtoTestObject());

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
    public void testSink() throws Exception {
        TestProtobufSourceFunction.messages.clear();
        TestProtobufSourceFunction.messages.add(getProtoTestObject());
        TestProtobufSinkFunction.results.clear();

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
        sql =
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
                tEnv().executeSql("insert into bigdata_sink select * from bigdata_source");
        tableResult.await();

        byte[] bytes = TestProtobufSinkFunction.results.get(0);
        MapTest mapTest = MapTest.parseFrom(bytes);
        assertEquals(1, mapTest.getA());
        assertEquals("b", mapTest.getMap1Map().get("a"));
        assertEquals("d", mapTest.getMap1Map().get("c"));
        MapTest.InnerMessageTest innerMessageTest = mapTest.getMap2Map().get("f");
        assertEquals(1, innerMessageTest.getA());
        assertEquals(2L, innerMessageTest.getB());
    }
}
