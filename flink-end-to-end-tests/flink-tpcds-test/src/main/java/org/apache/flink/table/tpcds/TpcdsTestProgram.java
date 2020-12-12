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

package org.apache.flink.table.tpcds;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.graph.GlobalDataExchangeMode;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.tpcds.schema.TpcdsSchema;
import org.apache.flink.table.tpcds.schema.TpcdsSchemaProvider;
import org.apache.flink.table.tpcds.stats.TpcdsStatsProvider;
import org.apache.flink.table.types.utils.TypeConversions;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * End-to-end test for TPC-DS.
 */
public class TpcdsTestProgram {

	private static final List<String> TPCDS_TABLES = Arrays.asList(
			"catalog_sales", "catalog_returns", "inventory", "store_sales",
			"store_returns", "web_sales", "web_returns", "call_center", "catalog_page",
			"customer", "customer_address", "customer_demographics", "date_dim",
			"household_demographics", "income_band", "item", "promotion", "reason",
			"ship_mode", "store", "time_dim", "warehouse", "web_page", "web_site"
	);
	private static final List<String> TPCDS_QUERIES = Arrays.asList(
			"1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
			"11", "12", "13", "14a", "14b", "15", "16", "17", "18", "19", "20",
			"21", "22", "23a", "23b", "24a", "24b", "25", "26", "27", "28", "29", "30",
			"31", "32", "33", "34", "35", "36", "37", "38", "39a", "39b", "40",
			"41", "42", "43", "44", "45", "46", "47", "48", "49", "50",
			"51", "52", "53", "54", "55", "56", "57", "58", "59", "60",
			"61", "62", "63", "64", "65", "66", "67", "68", "69", "70",
			"71", "72", "73", "74", "75", "76", "77", "78", "79", "80",
			"81", "82", "83", "84", "85", "86", "87", "88", "89", "90",
			"91", "92", "93", "94", "95", "96", "97", "98", "99"
	);

	private static final String QUERY_PREFIX = "query";
	private static final String QUERY_SUFFIX = ".sql";
	private static final String DATA_SUFFIX = ".dat";
	private static final String RESULT_SUFFIX = ".ans";
	private static final String COL_DELIMITER = "|";
	private static final String FILE_SEPARATOR = "/";

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		String sourceTablePath = params.getRequired("sourceTablePath");
		String queryPath = params.getRequired("queryPath");
		String sinkTablePath = params.getRequired("sinkTablePath");
		Boolean useTableStats = params.getBoolean("useTableStats");
		TableEnvironment tableEnvironment = prepareTableEnv(sourceTablePath, useTableStats);

		//execute TPC-DS queries
		for (String queryId : TPCDS_QUERIES) {
			System.out.println("[INFO]Run TPC-DS query " + queryId + " ...");
			String queryName = QUERY_PREFIX + queryId + QUERY_SUFFIX;
			String queryFilePath = queryPath + FILE_SEPARATOR + queryName;
			String queryString = loadFile2String(queryFilePath);
			Table resultTable = tableEnvironment.sqlQuery(queryString);

			//register sink table
			String sinkTableName = QUERY_PREFIX + queryId + "_sinkTable";
			((TableEnvironmentInternal) tableEnvironment).registerTableSinkInternal(sinkTableName,
					new CsvTableSink(
						sinkTablePath + FILE_SEPARATOR + queryId + RESULT_SUFFIX,
						COL_DELIMITER,
						1,
						FileSystem.WriteMode.OVERWRITE,
						resultTable.getSchema().getFieldNames(),
						resultTable.getSchema().getFieldDataTypes()
					));
			TableResult tableResult = resultTable.executeInsert(sinkTableName);
			// wait job finish
			tableResult.getJobClient().get()
					.getJobExecutionResult()
					.get();
			System.out.println("[INFO]Run TPC-DS query " + queryId + " success.");
		}
	}

	/**
	 * Prepare TableEnvironment for query.
	 *
	 * @param sourceTablePath
	 * @return
	 */
	private static TableEnvironment prepareTableEnv(String sourceTablePath, Boolean useTableStats) {
		//init Table Env
		EnvironmentSettings environmentSettings = EnvironmentSettings
				.newInstance()
				.useBlinkPlanner()
				.inBatchMode()
				.build();
		TableEnvironment tEnv = TableEnvironment.create(environmentSettings);

		//config Optimizer parameters
		tEnv.getConfig().getConfiguration()
				.setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
		tEnv.getConfig().getConfiguration()
				.setString(ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE, GlobalDataExchangeMode.POINTWISE_EDGES_PIPELINED.toString());
		tEnv.getConfig().getConfiguration()
				.setLong(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, 10 * 1024 * 1024);
		tEnv.getConfig().getConfiguration()
				.setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true);

		//register TPC-DS tables
		TPCDS_TABLES.forEach(table -> {
			TpcdsSchema schema = TpcdsSchemaProvider.getTableSchema(table);
			CsvTableSource.Builder builder = CsvTableSource.builder();
			builder.path(sourceTablePath + FILE_SEPARATOR + table + DATA_SUFFIX);
			for (int i = 0; i < schema.getFieldNames().size(); i++) {
				builder.field(
						schema.getFieldNames().get(i),
						TypeConversions.fromDataTypeToLegacyInfo(schema.getFieldTypes().get(i)));
			}
			builder.fieldDelimiter(COL_DELIMITER);
			builder.emptyColumnAsNull();
			builder.lineDelimiter("\n");
			CsvTableSource tableSource = builder.build();
			ConnectorCatalogTable catalogTable = ConnectorCatalogTable.source(tableSource, true);
			tEnv.getCatalog(tEnv.getCurrentCatalog()).ifPresent(catalog -> {
				try {
					catalog.createTable(new ObjectPath(tEnv.getCurrentDatabase(), table), catalogTable, false);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
		});
		// register statistics info
		if (useTableStats) {
			TpcdsStatsProvider.registerTpcdsStats(tEnv);
		}
		return tEnv;
	}

	private static String loadFile2String(String filePath) throws Exception {
		StringBuilder stringBuilder = new StringBuilder();
		Stream<String> stream = Files.lines(Paths.get(filePath), StandardCharsets.UTF_8);
		stream.forEach(s -> stringBuilder.append(s).append('\n'));
		return stringBuilder.toString();
	}
}
