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

import org.apache.flink.table.operations.CatalogTableOperation;

import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.annotation.Nullable;

import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.flink.table.catalog.CatalogStructureBuilder.BUILTIN_CATALOG_NAME;
import static org.apache.flink.table.catalog.CatalogStructureBuilder.database;
import static org.apache.flink.table.catalog.CatalogStructureBuilder.extCatalog;
import static org.apache.flink.table.catalog.CatalogStructureBuilder.root;
import static org.apache.flink.table.catalog.CatalogStructureBuilder.table;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link CatalogManager#resolveTable(String...)}.
 */
@RunWith(Parameterized.class)
public class CatalogManagerPathResolutionTest {
	@Parameters(name = "{index}: {0}=[path: {2}, expectedPath: {3}]")
	public static List<Object[]> dataTypes() throws Exception {
		return asList(
			new Object[][]{

				{
					"sameTableAtThreePositions",
					sameTableAtThreePositions(null, null),
					asList("tab1"),
					asList(BUILTIN_CATALOG_NAME, "default", "tab1")
				},

				{
					"sameTableAtThreePositions",
					sameTableAtThreePositions(null, null),
					asList("cat1", "db2", "tab1"),
					asList("cat1", "db2", "tab1")
				},

				{
					"sameTableAtThreePositions(cat1)",
					sameTableAtThreePositions("cat1", null),
					asList("tab1"),
					asList("cat1", "db1", "tab1")
				},

				{
					"sameTableAtThreePositions(cat1, db2)",
					sameTableAtThreePositions("cat1", "db2"),
					asList("tab1"),
					asList("cat1", "db2", "tab1")
				},

				{
					"externalCatalog",
					externalCatalog(),
					asList("extCat1", "tab1"),
					asList("extCat1", "tab1")
				},

				{
					"externalCatalog",
					externalCatalog(),
					asList("extCat1", "extCat2", "tab1"),
					asList("extCat1", "extCat2", "tab1")
				},

				{
					"externalCatalog",
					externalCatalog(),
					asList("extCat1", "extCat2", "extCat3", "tab1"),
					asList("extCat1", "extCat2", "extCat3", "tab1")
				}
			}
		);
	}

	private static CatalogManager sameTableAtThreePositions(
			@Nullable String defaultCatalog,
			@Nullable String defaultDb) throws Exception {
		CatalogManager manager = root()
			.builtin(
				database(
					"default",
					table("tab1")
				)
			)
			.catalog(
				"cat1",
				database(
					"db1",
					table("tab1")
				),
				database(
					"db2",
					table("tab1")
				)
			).build();

		if (defaultCatalog != null) {
			manager.setCurrentCatalog(defaultCatalog);
		}

		if (defaultDb != null) {
			manager.setCurrentDatabase(defaultDb);
		}
		return manager;
	}

	private static CatalogManager externalCatalog() throws Exception {
		return root()
			.builtin(
				database(
					"default",
					table("tab1"),
					table("tab2")
				)
			)
			.externalCatalog(
				"extCat1",
				table("tab1"),
				extCatalog(
					"extCat2",
					extCatalog("extCat3",
						table("tab1")
					),
					table("tab1"))
			).build();
	}

	@Parameter
	public String testLabel;

	@Parameter(1)
	public CatalogManager manager;

	@Parameter(2)
	public List<String> pathToLookup;

	@Parameter(3)
	public List<String> expectedPath;

	@Test
	public void testPathResolution() {
		CatalogTableOperation tab = manager.resolveTable(pathToLookup.toArray(new String[0])).get();
		assertThat(tab.getTablePath(), CoreMatchers.equalTo(expectedPath));
	}
}
