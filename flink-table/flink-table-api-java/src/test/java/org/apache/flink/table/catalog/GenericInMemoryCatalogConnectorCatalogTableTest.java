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

import org.junit.BeforeClass;

import java.util.HashMap;

/**
 * Test for {@link ConnectorCatalogTable} in {@link GenericInMemoryCatalog}.
 */
public class GenericInMemoryCatalogConnectorCatalogTableTest
		extends ConnectorCatalogTableTestBase {

	@BeforeClass
	public static void init() {
		catalog = new GenericInMemoryCatalog(TEST_CATALOG_NAME);
		catalog.open();
	}

	@Override
	public CatalogDatabase createDb() {
		return new GenericCatalogDatabase(
			new HashMap<String, String>() {{
				put("k1", "v1");
			}},
			null);
	}
}
