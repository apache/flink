package org.apache.flink.api.java.batch.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.BatchTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableConfig;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.util.TestBaseUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;

public class GroupingSetsTest {

	private final static String TABLE_NAME = "MyTable";
	private BatchTableEnvironment tableEnv;

	@Before
	public void setup() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		tableEnv = TableEnvironment.getTableEnvironment(env, new TableConfig());
		DataSet<Tuple3<Integer, Long, String>> dataSet = CollectionDataSets.get3TupleDataSet(env);
		tableEnv.registerDataSet(TABLE_NAME, dataSet);
	}

	@Test
	public void testGroupingSets() throws Exception {
		String query =
			"SELECT f1, f2, avg(f0) as a, GROUP_ID() as g FROM " + TABLE_NAME +
				" GROUP BY GROUPING SETS (f1, f2)";

		String expected =
			"6,null,18,1\n5,null,13,1\n4,null,8,1\n3,null,5,1\n2,null,2,1\n1,null,1,1\n" +
				"null,Luke Skywalker,6,2\nnull,I am fine.,5,2\nnull,Hi,1,2\n" +
				"null,Hello world, how are you?,4,2\nnull,Hello world,3,2\nnull,Hello,2,2\n" +
				"null,Comment#9,15,2\nnull,Comment#8,14,2\nnull,Comment#7,13,2\n" +
				"null,Comment#6,12,2\nnull,Comment#5,11,2\nnull,Comment#4,10,2\n" +
				"null,Comment#3,9,2\nnull,Comment#2,8,2\nnull,Comment#15,21,2\n" +
				"null,Comment#14,20,2\nnull,Comment#13,19,2\nnull,Comment#12,18,2\n" +
				"null,Comment#11,17,2\nnull,Comment#10,16,2\nnull,Comment#1,7,2";

		checkSql(query, expected);
	}

	@Test
	public void testCubeAsGroupingSets() throws Exception {
		String cubeQuery =
			"SELECT f1, f2, avg(f0) as a, GROUP_ID() as g, " +
				" GROUPING(f1) as gf1, GROUPING(f2) as gf2, " +
				" GROUPING_ID(f1) as gif1, GROUPING_ID(f2) as gif2, " +
				" GROUPING_ID(f1, f2) as gid " +
				" FROM " + TABLE_NAME + " GROUP BY CUBE (f1, f2)";

		String groupingSetsQuery =
			"SELECT f1, f2, avg(f0) as a, GROUP_ID() as g, " +
				" GROUPING(f1) as gf1, GROUPING(f2) as gf2, " +
				" GROUPING_ID(f1) as gif1, GROUPING_ID(f2) as gif2, " +
				" GROUPING_ID(f1, f2) as gid " +
				" FROM " + TABLE_NAME +
				" GROUP BY GROUPING SETS ((f1, f2), (f1), (f2), ())";

		compareSql(cubeQuery, groupingSetsQuery);
	}

	@Test
	public void testRollupAsGroupingSets() throws Exception {
		String rollupQuery =
			"SELECT f1, f2, avg(f0) as a, GROUP_ID() as g, " +
				" GROUPING(f1) as gf1, GROUPING(f2) as gf2, " +
				" GROUPING_ID(f1) as gif1, GROUPING_ID(f2) as gif2, " +
				" GROUPING_ID(f1, f2) as gid " +
				" FROM " + TABLE_NAME + " GROUP BY ROLLUP (f1, f2)";

		String groupingSetsQuery =
			"SELECT f1, f2, avg(f0) as a, GROUP_ID() as g, " +
				" GROUPING(f1) as gf1, GROUPING(f2) as gf2, " +
				" GROUPING_ID(f1) as gif1, GROUPING_ID(f2) as gif2, " +
				" GROUPING_ID(f1, f2) as gid " +
				" FROM " + TABLE_NAME +
				" GROUP BY GROUPING SETS ((f1, f2), (f1), ())";

		compareSql(rollupQuery, groupingSetsQuery);
	}

	/**
	 * Execute SQL query and check results.
	 *
	 * @param query    SQL query.
	 * @param expected Expected result.
	 */
	private void checkSql(String query, String expected) throws Exception {
		Table resultTable = tableEnv.sql(query);
		DataSet<Row> resultDataSet = tableEnv.toDataSet(resultTable, Row.class);
		List<Row> results = resultDataSet.collect();
		TestBaseUtils.compareResultAsText(results, expected);
	}

	private void compareSql(String query1, String query2) throws Exception {

		// Function to map row to string
		MapFunction<Row, String> mapFunction = new MapFunction<Row, String>() {

			@Override
			public String map(Row value) throws Exception {
				return value == null ? "null" : value.toString();
			}
		};

		// Execute first query and store results
		Table resultTable1 = tableEnv.sql(query1);
		DataSet<Row> resultDataSet1 = tableEnv.toDataSet(resultTable1, Row.class);
		List<String> results1 = resultDataSet1.map(mapFunction).collect();

		// Execute second query and store results
		Table resultTable2 = tableEnv.sql(query2);
		DataSet<Row> resultDataSet2 = tableEnv.toDataSet(resultTable2, Row.class);
		List<String> results2 = resultDataSet2.map(mapFunction).collect();

		// Compare results
		TestBaseUtils.compareResultCollections(results1, results2, new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				return o2 == null ? o1 == null ? 0 : 1 : o1.compareTo(o2);
			}
		});
	}
}
