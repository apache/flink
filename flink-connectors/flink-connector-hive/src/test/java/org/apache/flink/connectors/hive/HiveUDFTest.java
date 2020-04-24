package org.apache.flink.connectors.hive;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;

import org.apache.flink.table.functions.hive.HiveFunctionWrapper;
import org.apache.flink.table.functions.hive.HiveGenericUDF;
import org.apache.flink.table.functions.hive.HiveSimpleUDF;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.apache.flink.table.HiveVersionTestUtil.HIVE_110_OR_LATER;

public class HiveUDFTest {
	private static HiveShim hiveShim = HiveShimLoader.loadHiveShim(HiveShimLoader.getHiveVersion());
	StreamExecutionEnvironment bsEnv;
	EnvironmentSettings bsSettings;
	StreamTableEnvironment bsTableEnv;

	@Before
	public void createEnv() throws ClassNotFoundException {

		Assume.assumeTrue(HIVE_110_OR_LATER);
		bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		bsEnv.setParallelism(1);
		bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

		registerSimpleFunction("org.apache.flink.connectors.hive.HiveUDFV1", "hivetest");
		registerSimpleFunction("org.apache.flink.connectors.hive.HiveMapUDFTest", "hiveMapReturnTest");
		registerSimpleFunction("org.apache.flink.connectors.hive.HiveArrayUDFTest", "hiveArrayUDFTest");
		registerSimpleFunction("org.apache.flink.connectors.hive.HiveIntergerUDFTest", "hiveIntergerUDFTest");
		registerSimpleFunction("org.apache.flink.connectors.hive.HiveLongUDFTest", "hiveLongUDFTest");
		registerSimpleFunction("org.apache.flink.connectors.hive.HiveFloatUDFTest", "hiveFloatUDFTest");
		registerSimpleFunction("org.apache.flink.connectors.hive.HiveDoubleUDFTest", "hiveDoubleUDFTest");
		registerSimpleFunction("org.apache.flink.connectors.hive.HiveBooleanUDFTest", "hiveBooleanUDFTest");
	}

	@Test
	public void testHiveBooleanUDFTest() throws Exception {
		loadSqlQuery("select hiveBooleanUDFTest(1)");
	}

	@Test
	public void testHiveDoubleUDFTest() throws Exception {
		loadSqlQuery("select hiveDoubleUDFTest(123)");
	}
	@Test
	public void testHiveFloatUDFTest() throws Exception {
		loadSqlQuery("select hiveFloatUDFTest(123)");
	}
	@Test
	public void testHiveLongType() throws Exception {
		loadSqlQuery("select hiveLongUDFTest(123)");
	}
	@Test
	public void testHiveIntType() throws Exception {
		loadSqlQuery("select hiveIntergerUDFTest(1)");
	}
	@Test
	public void testHiveArrayType() throws Exception {
		loadSqlQuery("select hiveArrayUDFTest('ttt222')");
	}
	@Test
	public void testHiveMapType() throws Exception {
		loadSqlQuery("select hiveMapReturnTest('test', 'rrrr')");
	}
	@Test
	public void testHiveUdf() throws Exception {
		loadSqlQuery("select hivetest('11')");
	}
	@After
	public void execute() throws Exception {
		bsEnv.execute("test");
	}

	public void registerFunction(String classReference, String funcName) throws ClassNotFoundException {
		HiveGenericUDF udf = new HiveGenericUDF(new HiveFunctionWrapper<>(Class.forName(classReference).getName()), hiveShim);
		bsTableEnv.registerFunction(funcName, udf);
	}

	public void registerSimpleFunction(String classReference, String funcName) throws ClassNotFoundException {
		HiveSimpleUDF udf = new HiveSimpleUDF(new HiveFunctionWrapper(Class.forName(classReference).getName()), hiveShim);
		bsTableEnv.registerFunction(funcName, udf);
	}




	public void loadSqlQuery(String sql) throws ClassNotFoundException {
		Table resTable = bsTableEnv.sqlQuery(sql);
		DataStream<Row> resStream = bsTableEnv.toAppendStream(resTable, Row.class);
		resStream.print();
	}
}
