/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.test.exampleRecordPrograms;

import eu.stratosphere.api.java.record.operators.CollectionDataSource;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test the input field validation of CollectionDataSource
 */
public class CollectionValidationTest {

	@Test
	public void TestArrayInputValidation() throws Exception {

		/*
		 * valid array input
		 */
		try {
			new CollectionDataSource("test_1d_valid_array", "a", "b", "c");
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}

		try {
			new CollectionDataSource("test_2d_valid_array", new Object[][] { { 1, "a" },
				{ 2, "b" }, { 3, "c" } });
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}

		/*
		 * invalid array input
		 */
		try {
			new CollectionDataSource("test_1d_invalid_array", 1, "b", "c");
			Assert.fail("input type is different");
		} catch (Exception e) {
		}

		try {
			new CollectionDataSource("test_2d_invalid_array", new Object[][] {
				{ 1, "a" }, { 2, "b" }, { 3, 4 } });
			Assert.fail("input type is different");
		} catch (Exception e) {
		}
	}

	@Test
	public void TestCollectionInputValidation() throws Exception {
		/*
		 * valid collection input
		 */
		try {
			List<Object> tmp = new ArrayList<Object>();
			for (int i = 0; i < 100; i++) {
				tmp.add(i);
			}
			new CollectionDataSource(tmp, "test_valid_collection");
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}

		try {
			List<Object> tmp = new ArrayList<Object>();
			for (int i = 0; i < 100; i++) {
				List<Object> inner = new ArrayList<Object>();
				inner.add(i);
				inner.add('a' + i);
				tmp.add(inner);
			}
			new CollectionDataSource(tmp, "test_valid_double_collection");
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}

		/*
		 * invalid collection input
		 */
		try {
			List<Object> tmp = new ArrayList<Object>();
			for (int i = 0; i < 100; i++) {
				tmp.add(i);
			}
			tmp.add("a");
			new CollectionDataSource(tmp, "test_invalid_collection");
			Assert.fail("input type is different");
		} catch (Exception e) {
		}

		try {
			List<Object> tmp = new ArrayList<Object>();
			for (int i = 0; i < 100; i++) {
				List<Object> inner = new ArrayList<Object>();
				inner.add(i);
				inner.add('a' + i);
				tmp.add(inner);
			}
			List<Object> inner = new ArrayList<Object>();
			inner.add('a');
			inner.add('a');
			tmp.add(inner);
			new CollectionDataSource(tmp, "test_invalid_double_collection");
			Assert.fail("input type is different");
		} catch (Exception e) {
		}
	}
}
