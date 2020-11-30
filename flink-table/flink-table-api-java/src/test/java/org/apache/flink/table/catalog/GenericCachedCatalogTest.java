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

import org.apache.flink.table.functions.TestGenericUDF;
import org.apache.flink.table.functions.TestSimpleUDF;

import org.junit.BeforeClass;

import java.time.Duration;

import static org.apache.flink.table.catalog.GenericInMemoryCatalog.DEFAULT_DB;

/**
 * Test for GeneralCachedCatalog.
 */
public class GenericCachedCatalogTest extends CatalogTestBase {

	public static final String TEST_CACHED_CATALOG_NAME = "test-cached-catalog";

	@BeforeClass
	public static void init() {
		GenericInMemoryCatalog delegate = new GenericInMemoryCatalog(TEST_CATALOG_NAME);
		delegate.open();
		catalog = new GenericCachedCatalog(delegate, TEST_CACHED_CATALOG_NAME, DEFAULT_DB,
			false, 0, Duration.ofSeconds(5), Duration.ofSeconds(2), 100);
		catalog.open();
	}

	@Override
	protected boolean isGeneric() {
		return true;
	}

	@Override
	protected CatalogFunction createFunction() {
		return new CatalogFunctionImpl(TestGenericUDF.class.getCanonicalName());
	}

	@Override
	protected CatalogFunction createPythonFunction() {
		return new CatalogFunctionImpl("test.func1", FunctionLanguage.PYTHON);
	}

	@Override
	protected CatalogFunction createAnotherFunction() {
		return new CatalogFunctionImpl(TestSimpleUDF.class.getCanonicalName(), FunctionLanguage.SCALA);
	}
}
