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

package org.apache.flink.table.sources.decorator;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

/**
 * CachedLookupTableSourceTest.
 */
public class CachedLookupFunctionDecoratorTest {
	private final long maximnCacheSize = 1 * 1024 * 1024L;
	List<Row> result = new ArrayList<>();
	Collector<Row> testCollector = new Collector<Row>() {
		@Override
		public void collect(Row record) {
			result.add(record);
		}

		@Override
		public void close() {

		}
	};

	@Test
	public void testEvalVariableObjectKey() throws Exception {
		result.clear();
		CachedLookupFunctionDecorator<Row> cachedLookupTableSource = new CachedLookupFunctionDecorator<>(new TestLookupFucntion(),
			maximnCacheSize);
		cachedLookupTableSource.setCollector(testCollector);
		cachedLookupTableSource.open(mock(FunctionContext.class));

		Cache<Row, List<Row>> cache = cachedLookupTableSource.getCache();
		//cache still have no data.
		Assert.assertEquals(0, cache.size());
		cachedLookupTableSource.eval("1");
		//load into cache and emit correctly.
		Assert.assertEquals(1, cache.size());
		Assert.assertEquals(5, result.size());
		Assert.assertEquals(cache.getIfPresent(Row.of("1")), result);
		List<Row> expected = Lists.newArrayList(Row.of("1", "0"),
			Row.of("1", "1"),
			Row.of("1", "2"),
			Row.of("1", "3"),
			Row.of("1", "4"));
		Assert.assertEquals(expected, result);

		// cache hit.
		cachedLookupTableSource.eval("1");
		Assert.assertEquals(1, cache.size());
		Assert.assertEquals(10, result.size());
		List<Row> expected2 = cache.getIfPresent(Row.of("1"));
		expected2.addAll(cache.getIfPresent(Row.of("1")));
		Assert.assertEquals(expected2, result);

		cachedLookupTableSource.eval("2");
		Assert.assertEquals(2, cache.size());
		Assert.assertEquals(15, result.size());
		expected2.addAll(cache.getIfPresent(Row.of("2")));
		Assert.assertEquals(expected2, result);

		cachedLookupTableSource.eval("3", "4");
		Assert.assertEquals(3, cache.size());
		Assert.assertEquals(20, result.size());
		expected2.addAll(cache.getIfPresent(Row.of("3", "4")));
		Assert.assertEquals(expected2, result);

		expected = Lists.newArrayList(Row.of("3", "4", "0"),
			Row.of("3", "4", "1"),
			Row.of("3", "4", "2"),
			Row.of("3", "4", "3"),
			Row.of("3", "4", "4"));
		List<Row> multKey = cache.getIfPresent(Row.of("3", "4"));
		Assert.assertEquals(expected, multKey);
		cachedLookupTableSource.close();

	}

	@Test
	public void testWithMaxSize() throws Exception {
		result.clear();
		//set maxSize is 1, that means only can hold one key in cache.
		CachedLookupFunctionDecorator<Row> cachedLookupTableSource = new CachedLookupFunctionDecorator<>(new TestLookupFucntion(),
			1);
		cachedLookupTableSource.setCollector(testCollector);
		cachedLookupTableSource.open(mock(FunctionContext.class));

		Cache<Row, List<Row>> cache = cachedLookupTableSource.getCache();
		//cache still have no data.
		Assert.assertEquals(0, cache.size());
		cachedLookupTableSource.eval("1");
		//load into cache and emit correctly.
		Assert.assertEquals(1, cache.size());
		Assert.assertEquals(5, result.size());
		Assert.assertEquals(cache.getIfPresent(Row.of("1")), result);
		List<Row> expected = Lists.newArrayList(Row.of("1", "0"),
			Row.of("1", "1"),
			Row.of("1", "2"),
			Row.of("1", "3"),
			Row.of("1", "4"));
		Assert.assertEquals(expected, result);

		// cache hit.
		cachedLookupTableSource.eval("1");
		Assert.assertEquals(1, cache.size());
		Assert.assertEquals(10, result.size());
		List<Row> expected2 = cache.getIfPresent(Row.of("1"));
		expected2.addAll(cache.getIfPresent(Row.of("1")));
		Assert.assertEquals(expected2, result);

		cachedLookupTableSource.eval("2");
		Assert.assertEquals(1, cache.size());
		Assert.assertEquals(15, result.size());
		expected2.addAll(cache.getIfPresent(Row.of("2")));
		Assert.assertEquals(expected2, result);
	}

	@Test
	public void testEvalOneObjectKey() throws Exception {
		result.clear();
		CachedLookupFunctionDecorator<Row> cachedLookupTableSource = new CachedLookupFunctionDecorator<>(new TestLookupFucntionOneKey(),
			maximnCacheSize);
		cachedLookupTableSource.setCollector(testCollector);
		cachedLookupTableSource.open(mock(FunctionContext.class));

		Cache<Row, List<Row>> cache = cachedLookupTableSource.getCache();
		//cache still have no data.
		Assert.assertEquals(0, cache.size());
		cachedLookupTableSource.eval("1");
		//load into cache and emit correctly.
		Assert.assertEquals(1, cache.size());
		Assert.assertEquals(2, result.size());
		Assert.assertEquals(cache.getIfPresent(Row.of("1")), result);
		List<Row> expected = Lists.newArrayList(Row.of("1", "0"), Row.of("1", "1"));
		Assert.assertEquals(expected, result);

		cachedLookupTableSource.eval("2");
		Assert.assertEquals(2, cache.size());
		Assert.assertEquals(4, result.size());
		List<Row> expectedCache = cache.getIfPresent(Row.of("1"));
		expectedCache.addAll(cache.getIfPresent(Row.of("2")));
		Assert.assertEquals(expectedCache, result);

		List<Row> expectedFinal = Lists.newArrayList(Row.of("1", "0"),
			Row.of("1", "1"),
			Row.of("2", "0"),
			Row.of("2", "1"));
		Assert.assertEquals(expectedFinal, result);
	}

	@Test
	public void testLongKeyCantMatch() throws Exception {
		result.clear();
		CachedLookupFunctionDecorator<Row> cachedLookupTableSource = new CachedLookupFunctionDecorator<>(new TableFunction<Row>() {
			public void eval(long... keys) {
				for (int i = 0; i < 5; i++) {
					Row row = new Row(keys.length + 1);
					for (int j = 0; j < keys.length; j++) {
						row.setField(j, keys[j]);

					}
					row.setField(keys.length, "" + i);
					collect(row);
				}
			}
		}, maximnCacheSize);
		cachedLookupTableSource.open(mock(FunctionContext.class));

		String exception = "WANT_EXCEPTION";
		try {
			cachedLookupTableSource.eval(1L);
		} catch (Exception e) {
			exception = e.getMessage();
		}
		Assert.assertEquals(
			"CachedLookupTableSource exception:java.lang.NoSuchMethodException: org.apache.flink.table.sources.decorator.CachedLookupFunctionDecoratorTest$2.eval([Ljava.lang.Object;)",
			exception);
	}

	private class TestLookupFucntion extends TableFunction<Row> {
		@Override
		public void open(FunctionContext context) throws Exception {
		}

		@Override
		public void close() throws Exception {
		}

		public void eval(Object... keys) {
			for (int i = 0; i < 5; i++) {
				Row row = new Row(keys.length + 1);
				for (int j = 0; j < keys.length; j++) {
					row.setField(j, keys[j]);

				}
				row.setField(keys.length, "" + i);
				collect(row);
			}
		}
	}

	private class TestLookupFucntionOneKey extends TableFunction<Row> {
		@Override
		public void open(FunctionContext context) throws Exception {
		}

		@Override
		public void close() throws Exception {
		}

		public void eval(Object key) {
			for (int i = 0; i < 2; i++) {
				collect(Row.of(key, "" + i));
			}
		}
	}

}
