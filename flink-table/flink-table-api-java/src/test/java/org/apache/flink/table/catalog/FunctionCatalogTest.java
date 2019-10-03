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

import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

/**
 * Test for {@link FunctionCatalog}.
 */
public class FunctionCatalogTest {

	@Test
	public void testGetBuiltInFunctions() {
		FunctionCatalog functionCatalog = new FunctionCatalog(
			new CatalogManager("test", new GenericInMemoryCatalog("test")));

		Set<String> actual = new HashSet<>();
		Collections.addAll(actual, functionCatalog.getFunctions());

		Set<String> expected = BuiltInFunctionDefinitions.getDefinitions()
			.stream()
			.map(f -> FunctionCatalog.normalizeName(f.getName()))
			.collect(Collectors.toSet());

		assertTrue(actual.containsAll(expected));
	}
}
