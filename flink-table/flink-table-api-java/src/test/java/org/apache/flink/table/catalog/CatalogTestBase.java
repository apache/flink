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

import java.util.HashMap;

/**
 * Base of tests for any catalog implementations, like GenericInMemoryCatalog and HiveCatalog.
 */
public abstract class CatalogTestBase extends CatalogTest {

	@Override
	public CatalogDatabase createDb() {
		return new CatalogDatabaseImpl(
			new HashMap<String, String>() {{
				put("k1", "v1");
			}},
			TEST_COMMENT
		);
	}

	@Override
	public CatalogDatabase createAnotherDb() {
		return new CatalogDatabaseImpl(
			new HashMap<String, String>() {{
				put("k2", "v2");
			}},
			TEST_COMMENT
		);
	}
}
