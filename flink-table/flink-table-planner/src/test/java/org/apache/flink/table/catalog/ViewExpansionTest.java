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

package org.apache.flink.table.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.utils.StreamTableTestUtil;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import scala.Some;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.catalog.CatalogStructureBuilder.database;
import static org.apache.flink.table.catalog.CatalogStructureBuilder.root;
import static org.apache.flink.table.catalog.CatalogStructureBuilder.table;
import static org.apache.flink.table.catalog.CatalogStructureBuilder.view;

/**
 * Tests for expanding {@link CatalogView}.
 */
public class ViewExpansionTest {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void testSqlViewExpansion() throws Exception {
		CatalogManager catalogManager = root()
			.builtin(
				database(
					"default",
					table("tab1"),
					view("view").withQuery("SELECT * FROM `builtin`.`default`.tab1")
				)
			).build();

		StreamTableTestUtil util = new StreamTableTestUtil(new Some<>(catalogManager));
		util.verifyJavaSql(
			"SELECT * FROM `builtin`.`default`.`view`",
			source("builtin", "default", "tab1"));
	}

	@Test
	public void testSqlViewExpansionWithMismatchRowType() throws Exception {
		CatalogManager catalogManager = root()
			.builtin(
				database(
					"default",
					table("tab1")
						.withTableSchema(
							TableSchema.builder()
								.field("a", DataTypes.INT())
								.field("b", DataTypes.STRING())
								.field("c", DataTypes.INT())
								.build()),
					view("view")
						.withTableSchema(
							TableSchema.builder()
								// Change the nullability intentionally.
								.field("a", DataTypes.INT().notNull())
								.field("b", DataTypes.STRING())
								.field("c", DataTypes.INT())
								.build())
						.withQuery("SELECT a, b, count(c) FROM `builtin`.`default`.tab1 group by a, b")
				)
			).build();

		StreamTableTestUtil util = new StreamTableTestUtil(new Some<>(catalogManager));
		final String expected = "DataStreamCalc(select=[a, b, CAST(EXPR$2) AS c])\n"
			+ "DataStreamGroupAggregate(groupBy=[a, b], select=[a, b, COUNT(c) AS EXPR$2])\n"
			+ "StreamTableSourceScan(table=[[builtin, default, tab1]], fields=[a, b, c], source=[isTemporary=[false]])";
		util.verifyJavaSql(
				"SELECT * FROM `builtin`.`default`.`view`",
				expected);
	}

	@Test
	public void testTableViewExpansion() throws Exception {
		CatalogManager catalogManager = root()
			.builtin(
				database(
					"default",
					table("tab1"),
					view("view").withQuery("SELECT * FROM `builtin`.`default`.tab1")
				)
			).build();

		StreamTableTestUtil util = new StreamTableTestUtil(new Some<>(catalogManager));
		Table tab = util.javaTableEnv().scan("builtin", "default", "view").select($("*"));
		util.verifyJavaTable(
			tab,
			source("builtin", "default", "tab1"));
	}

	@Test
	public void testTableViewExpansionWithMismatchRowType() throws Exception {
		CatalogManager catalogManager = root()
			.builtin(
				database(
					"default",
					table("tab1")
						.withTableSchema(
							TableSchema.builder()
								.field("a", DataTypes.INT())
								.field("b", DataTypes.STRING())
								.field("c", DataTypes.INT())
								.build()),
					view("view")
						.withTableSchema(
							TableSchema.builder()
								// Change the nullability intentionally.
								.field("a", DataTypes.INT().notNull())
								.field("b", DataTypes.STRING())
								.field("c", DataTypes.INT())
								.build())
						.withQuery("SELECT a, b, count(c) FROM `builtin`.`default`.tab1 group by a, b")
				)
			).build();

		StreamTableTestUtil util = new StreamTableTestUtil(new Some<>(catalogManager));
		Table tab = util.javaTableEnv().scan("builtin", "default", "view").select($("*"));
		final String expected = "DataStreamCalc(select=[a, b, CAST(EXPR$2) AS c])\n"
			+ "DataStreamGroupAggregate(groupBy=[a, b], select=[a, b, COUNT(c) AS EXPR$2])\n"
			+ "StreamTableSourceScan(table=[[builtin, default, tab1]], fields=[a, b, c], source=[isTemporary=[false]])";
		util.verifyJavaTable(
			tab,
			expected);
	}

	@Test
	public void testSqlViewWithoutFullyQualified() throws Exception {
		CatalogManager catalogManager = root()
			.builtin(
				database(
					"default",
					table("tab1")
				),
				database(
					"different",
					table("tab1"),
					view("view").withQuery("SELECT * FROM tab1")
				)
			).build();

		StreamTableTestUtil util = new StreamTableTestUtil(new Some<>(catalogManager));
		StreamTableEnvironmentImpl tableEnv = util.javaTableEnv();

		tableEnv.useCatalog("builtin");
		tableEnv.useDatabase("default");

		//Note: even though default path is set to builtin.default, the default path for view expansion
		//is the path of the view.
		util.verifyJavaSql(
			"SELECT * FROM `builtin`.`different`.view",
			source("builtin", "different", "tab1"));
	}

	@Test
	public void testTableViewWithoutFullyQualified() throws Exception {
		CatalogManager catalogManager = root()
			.builtin(
				database(
					"default",
					table("tab1")
				),
				database(
					"different",
					table("tab1"),
					view("view").withQuery("SELECT * FROM tab1")
				)
			).build();

		StreamTableTestUtil util = new StreamTableTestUtil(new Some<>(catalogManager));
		StreamTableEnvironmentImpl tableEnv = util.javaTableEnv();

		tableEnv.useCatalog("builtin");
		tableEnv.useDatabase("default");
		Table tab = tableEnv.scan("builtin", "different", "view").select($("*"));

		//Note: even though default path is set to builtin.default, the default path for view expansion
		//is the path of the view.
		util.verifyJavaTable(
			tab,
			source("builtin", "different", "tab1"));
	}

	@Test
	public void testTableViewWithOnlyDatabaseQualified() throws Exception {
		CatalogManager catalogManager = root()
			.builtin(
				database(
					"default",
					table("tab1")
				),
				database(
					"different_db",
					table("tab1"),
					view("view").withQuery("SELECT * FROM tab1")
				)
			).catalog(
				"different_cat",
				database(
					"default",
					table("tab1")
				),
				database(
					"different_db",
					table("tab1"),
					view("view").withQuery("SELECT * FROM different_db.tab1")
				)
			).build();

		StreamTableTestUtil util = new StreamTableTestUtil(new Some<>(catalogManager));
		StreamTableEnvironmentImpl tableEnv = util.javaTableEnv();

		tableEnv.useCatalog("builtin");
		tableEnv.useDatabase("default");
		Table tab = tableEnv.scan("different_cat", "different_db", "view").select($("*"));

		//Note: even though default path is set to builtin.default, the default catalog for the view expansion
		//is the catalog of the view.
		util.verifyJavaTable(
			tab,
			source("different_cat", "different_db", "tab1"));
	}

	private String source(String... path) {
		return String.format(
			"StreamTableSourceScan(table=[[%s]], fields=[], source=[isTemporary=[false]])",
			String.join(", ", path));
	}
}
