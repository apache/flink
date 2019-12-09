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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.module.ModuleManager;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link FunctionCatalog}.
 */
public class FunctionCatalogTest {
	private FunctionCatalog functionCatalog;
	private Catalog catalog;
	private ModuleManager moduleManager;

	private final String testCatalogName = "test";
	private final ObjectIdentifier oi = ObjectIdentifier.of(
		testCatalogName,
		GenericInMemoryCatalog.DEFAULT_DB,
		TEST_FUNCTION_NAME);

	private static final String TEST_FUNCTION_NAME = "test_function";

	@Before
	public void init() throws DatabaseAlreadyExistException {
		catalog = new GenericInMemoryCatalog(testCatalogName);
		moduleManager = new ModuleManager();
		functionCatalog = new FunctionCatalog(
			TableConfig.getDefault(),
			new CatalogManager(testCatalogName, catalog), moduleManager);
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
		ObjectIdentifier oi = ObjectIdentifier.of(
			testCatalogName,
			GenericInMemoryCatalog.DEFAULT_DB,
			TEST_FUNCTION_NAME);
		UnresolvedIdentifier identifier = UnresolvedIdentifier.of(
			testCatalogName,
			GenericInMemoryCatalog.DEFAULT_DB,
			TEST_FUNCTION_NAME);

		// test no function is found
		assertFalse(functionCatalog.lookupFunction(identifier).isPresent());

		// test catalog function is found
		catalog.createFunction(
			oi.toObjectPath(),
			new CatalogFunctionImpl(TestFunction1.class.getName()), false);

		FunctionLookup.Result result = functionCatalog.lookupFunction(identifier).get();

		assertFalse(result.getFunctionIdentifier().getSimpleName().isPresent());
		assertEquals(oi, result.getFunctionIdentifier().getIdentifier().get());
		assertTrue(((ScalarFunctionDefinition) result.getFunctionDefinition()).getScalarFunction() instanceof TestFunction1);

		// test temp catalog function is found
		functionCatalog.registerTempCatalogScalarFunction(
			oi,
			new TestFunction2()
		);

		result = functionCatalog.lookupFunction(identifier).get();

		assertFalse(result.getFunctionIdentifier().getSimpleName().isPresent());
		assertEquals(oi, result.getFunctionIdentifier().getIdentifier().get());
		assertTrue(((ScalarFunctionDefinition) result.getFunctionDefinition()).getScalarFunction() instanceof TestFunction2);
	}

	@Test
	public void testAmbiguousFunctionReference() throws FunctionAlreadyExistException, DatabaseNotExistException {
		ObjectIdentifier oi = ObjectIdentifier.of(
			testCatalogName,
			GenericInMemoryCatalog.DEFAULT_DB,
			TEST_FUNCTION_NAME);

		// test no function is found
		assertFalse(functionCatalog.lookupFunction(UnresolvedIdentifier.of(TEST_FUNCTION_NAME)).isPresent());

		// test catalog function is found
		catalog.createFunction(
			oi.toObjectPath(),
			new CatalogFunctionImpl(TestFunction1.class.getName()), false);

		FunctionLookup.Result result = functionCatalog.lookupFunction(UnresolvedIdentifier.of(TEST_FUNCTION_NAME)).get();

		assertFalse(result.getFunctionIdentifier().getSimpleName().isPresent());
		assertEquals(oi, result.getFunctionIdentifier().getIdentifier().get());
		assertTrue(((ScalarFunctionDefinition) result.getFunctionDefinition()).getScalarFunction() instanceof TestFunction1);

		// test temp catalog function is found
		functionCatalog.registerTempCatalogScalarFunction(
			oi,
			new TestFunction2()
		);

		result = functionCatalog.lookupFunction(UnresolvedIdentifier.of(TEST_FUNCTION_NAME)).get();

		assertFalse(result.getFunctionIdentifier().getSimpleName().isPresent());
		assertEquals(oi, result.getFunctionIdentifier().getIdentifier().get());
		assertTrue(((ScalarFunctionDefinition) result.getFunctionDefinition()).getScalarFunction() instanceof TestFunction2);

		// test system function is found
		moduleManager.loadModule("test_module", new TestModule());

		result = functionCatalog.lookupFunction(UnresolvedIdentifier.of(TEST_FUNCTION_NAME)).get();

		assertEquals(TEST_FUNCTION_NAME, result.getFunctionIdentifier().getSimpleName().get());
		assertTrue(((ScalarFunctionDefinition) result.getFunctionDefinition()).getScalarFunction() instanceof TestFunction3);

		// test temp system function is found
		functionCatalog.registerTempSystemScalarFunction(TEST_FUNCTION_NAME, new TestFunction4());

		result = functionCatalog.lookupFunction(UnresolvedIdentifier.of(TEST_FUNCTION_NAME)).get();

		assertEquals(TEST_FUNCTION_NAME, result.getFunctionIdentifier().getSimpleName().get());
		assertTrue(((ScalarFunctionDefinition) result.getFunctionDefinition()).getScalarFunction() instanceof TestFunction4);
	}

	private static class TestModule implements Module {
		@Override
		public Set<String> listFunctions() {
			return new HashSet<String>() {{
				add(TEST_FUNCTION_NAME);
			}};
		}

		@Override
		public Optional<FunctionDefinition> getFunctionDefinition(String name) {
			return Optional.of(new ScalarFunctionDefinition(TEST_FUNCTION_NAME, new TestFunction3()));
		}
	}

	@Test
	public void testRegisterAndDropTempSystemFunction() {
		assertFalse(Arrays.asList(functionCatalog.getUserDefinedFunctions()).contains(TEST_FUNCTION_NAME));

		functionCatalog.registerTempSystemScalarFunction(TEST_FUNCTION_NAME, new TestFunction1());
		assertTrue(Arrays.asList(functionCatalog.getUserDefinedFunctions()).contains(TEST_FUNCTION_NAME));

		functionCatalog.dropTempSystemFunction(TEST_FUNCTION_NAME, false);
		assertFalse(Arrays.asList(functionCatalog.getUserDefinedFunctions()).contains(TEST_FUNCTION_NAME));

		functionCatalog.dropTempSystemFunction(TEST_FUNCTION_NAME, true);
		assertFalse(Arrays.asList(functionCatalog.getUserDefinedFunctions()).contains(TEST_FUNCTION_NAME));
	}

	@Test
	public void testRegisterAndDropTempCatalogFunction() {
		assertFalse(Arrays.asList(functionCatalog.getUserDefinedFunctions()).contains(TEST_FUNCTION_NAME));

		functionCatalog.registerTempCatalogScalarFunction(oi, new TestFunction1());
		assertTrue(Arrays.asList(functionCatalog.getUserDefinedFunctions()).contains(oi.getObjectName()));

		functionCatalog.dropTempCatalogFunction(oi, false);
		assertFalse(Arrays.asList(functionCatalog.getUserDefinedFunctions()).contains(oi.getObjectName()));

		functionCatalog.dropTempCatalogFunction(oi, true);
		assertFalse(Arrays.asList(functionCatalog.getUserDefinedFunctions()).contains(oi.getObjectName()));
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

	/**
	 * Testing function.
	 */
	public static class TestFunction3 extends ScalarFunction {

	}

	/**
	 * Testing function.
	 */
	public static class TestFunction4 extends ScalarFunction {

	}
}
