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

package org.apache.flink.runtime.codegeneration.utils;

import freemarker.template.TemplateException;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.codegeneration.SorterFactory;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;

public class CodeGenerationSorterBaseTest {
	
	protected static final long SEED = 649180756312423613L;

	protected static final long SEED2 = 97652436586326573L;

	protected static final int KEY_MAX = Integer.MAX_VALUE;

	protected static final int VALUE_LENGTH = 118;

	private static final int MEMORY_SIZE = 1024 * 1024 * 64;
	
	private static final int MEMORY_PAGE_SIZE = 32 * 1024;

	private static final long MAXIMUM_RECORDS = 559273;

	private static final QuickSort quickSort = new QuickSort();

	protected MemoryManager memoryManager;
	protected SorterFactory sorterFactory;

	protected ExecutionConfig executionConfig = new ExecutionConfig(){
		{
			setCodeGenerationForSorterEnabled(true);
		}
	};


	@Before
	public void beforeTest() throws IOException {
		this.memoryManager = new MemoryManager(MEMORY_SIZE, 1, MEMORY_PAGE_SIZE, MemoryType.HEAP, true);
		this.sorterFactory = SorterFactory.getInstance();

		Assert.assertTrue("Code generation for sorter is enabled", executionConfig.isCodeGenerationForSorterEnabled());
	}

	@After
	public void afterTest() {
		if (!this.memoryManager.verifyEmpty()) {
			Assert.fail("Memory Leak: Some memory has not been returned to the memory manager.");
		}
		
		if (this.memoryManager != null) {
			this.memoryManager.shutdown();
			this.memoryManager = null;
		}
	}


	protected InMemorySorter createSorter(TypeSerializer serializer, TypeComparator comparator, List<MemorySegment> memory) throws IllegalAccessException, TemplateException, IOException, InstantiationException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, MemoryAllocationException {
		return this.sorterFactory.createSorter(
			executionConfig,
			serializer,
			comparator,
			memory
		);
	}

	protected List<MemorySegment> createMemory() throws MemoryAllocationException {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);

		return memory;
	}

	protected <K,V> void testSorting(SorterTestDataGenerator generator, InMemorySorter sorter, TypeComparator comparator, int keyPos) throws IOException {

		// Fill data phrase
		HashMap<Object, Integer> expectedKeyCounts = new HashMap<>();

		long totalRecords = 0;

		Tuple2 reusedRecord = new Tuple2();
		while(totalRecords < MAXIMUM_RECORDS){
			generator.generate(reusedRecord);

			if( sorter.write(reusedRecord) ) {
				int count = expectedKeyCounts.getOrDefault(reusedRecord.getField(keyPos), 0);
				expectedKeyCounts.put(reusedRecord.getField(keyPos), count+1);
				totalRecords++;
			} else {
				break;
			}
		}

		// Sorting phrase
		quickSort.sort(sorter);

		MutableObjectIterator<Tuple2<K,V>> iter = sorter.getIterator();
		HashMap<Object, Integer> actualKeyCounts = new HashMap<>();
		int actualTotalRecords = 0;

		Tuple2<K,V> readTarget	= iter.next();
		K last = (K)readTarget.getField(keyPos);
		actualKeyCounts.put(last, 1);
		actualTotalRecords++;

		// Verify order
		while ((readTarget = iter.next()) != null) {
			K current = (K)readTarget.getField(keyPos);

			int count   = actualKeyCounts.getOrDefault(current, 0);
			actualKeyCounts.put(current, count+1);


			final int cmp = comparator.compare(last, current);
			if (cmp > 0) {
				Assert.fail("Next key is not larger or equal to previous key.");
			}

			last = current;
			actualTotalRecords++;
		}

		// Verify whether all data is remained the result.
		Assert.assertEquals("Total Records", totalRecords, actualTotalRecords);

		Assert.assertEquals("Total Keys", expectedKeyCounts.keySet().size(), actualKeyCounts.keySet().size());
		for( Object k : expectedKeyCounts.keySet() ) {
			Assert.assertEquals("Key '" + k + "' has the same count after sorting", expectedKeyCounts.get(k), actualKeyCounts.get(k) );
		}

	}
}

