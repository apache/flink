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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogTestBase;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.functions.hive.HiveGenericUDF;
import org.apache.flink.table.functions.hive.HiveSimpleUDF;

import org.junit.Assume;
import org.junit.Test;

/**
 * Base class for testing HiveCatalog.
 */
public abstract class HiveCatalogMetadataTestBase extends CatalogTestBase {

	// ------ table and column stats ------

	@Override
	@Test
	public void testAlterTableStats() throws Exception {
		String hiveVersion = ((HiveCatalog) catalog).getHiveVersion();
		Assume.assumeTrue(hiveVersion.compareTo("1.2.1") >= 0);
		super.testAlterTableStats();
	}

	// ------ functions ------

	@Test
	public void testCreateFunctionCaseInsensitive() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		String functionName = "myUdf";
		ObjectPath functionPath = new ObjectPath(db1, functionName);
		catalog.createFunction(functionPath, createFunction(), false);
		// make sure we can get the function
		catalog.getFunction(functionPath);

		catalog.dropFunction(functionPath, false);
	}

	@Override
	protected CatalogFunction createFunction() {
		return new CatalogFunctionImpl(HiveSimpleUDF.class.getCanonicalName());
	}

	@Override
	protected CatalogFunction createAnotherFunction() {
		return new CatalogFunctionImpl(HiveGenericUDF.class.getCanonicalName());
	}

	@Override
	protected CatalogFunction createPythonFunction() {
		return new CatalogFunctionImpl("test.func1", FunctionLanguage.PYTHON);
	}
}
