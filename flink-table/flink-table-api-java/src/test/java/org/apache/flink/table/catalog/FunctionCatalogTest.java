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

import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.module.ModuleManager;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link FunctionCatalog}.
 */
public class FunctionCatalogTest {
	private FunctionCatalog functionCatalog;
	private Catalog catalog;

	@Before
	public void init() throws DatabaseAlreadyExistException {
		catalog = new GenericInMemoryCatalog("test");
		catalog.createDatabase("test", new CatalogDatabaseImpl(Collections.EMPTY_MAP, null), false);
		functionCatalog = new FunctionCatalog(
			new CatalogManager("test", catalog), new ModuleManager());
	}

	@Test
	public void testGetBuiltInFunctions() {
		Set<String> actual = new HashSet<>();
		Collections.addAll(actual, functionCatalog.getFunctions());

		Set<String> expected = new ModuleManager().listFunctions();

		assertTrue(actual.containsAll(expected));
	}

	@Test
	public void testPreciseFunctionReference() throws FunctionAlreadyExistException, DatabaseNotExistException {
		ObjectIdentifier oi = ObjectIdentifier.of("test", "test", "test_function");

		// test no function is found
		assertFalse(functionCatalog.lookupFunction(FunctionIdentifier.of(oi)).isPresent());

		// test catalog function is found
		catalog.createFunction(
			oi.toObjectPath(),
			new CatalogFunctionImpl(TestFunction1.class.getName(), Collections.emptyMap()), false);

		FunctionLookup.Result result = functionCatalog.lookupFunction(FunctionIdentifier.of(oi)).get();

		assertFalse(result.getFunctionIdentifier().getSimpleName().isPresent());
		assertEquals(oi, result.getFunctionIdentifier().getIdentifier().get());
		assertNotNull(result.getFunctionDefinition());
		assertTrue(((ScalarFunctionDefinition) result.getFunctionDefinition()).getScalarFunction() instanceof TestFunction1);

		// test temp catalog function is found
		functionCatalog.registerTempCatalogScalarFunction(
			oi,
			new TestFunction2()
		);

		result = functionCatalog.lookupFunction(FunctionIdentifier.of(oi)).get();

		assertFalse(result.getFunctionIdentifier().getSimpleName().isPresent());
		assertEquals(oi, result.getFunctionIdentifier().getIdentifier().get());
		assertNotNull(result.getFunctionDefinition());
		assertTrue(((ScalarFunctionDefinition) result.getFunctionDefinition()).getScalarFunction() instanceof TestFunction2);
	}

	/**
	 * Testing function.
	 */
	public static class TestFunction1 extends ScalarFunction {

	}

	/**
	 * Testing function.
	 */
	public static class TestFunction2 extends ScalarFunction {

	}
}
