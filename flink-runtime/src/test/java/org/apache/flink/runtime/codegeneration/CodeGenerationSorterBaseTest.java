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

package org.apache.flink.runtime.codegeneration;

import freemarker.template.TemplateException;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;


public class CodeGenerationSorterBaseTest {
	
	private static final long SEED = 649180756312423613L;
	
	private static final long SEED2 = 97652436586326573L;

	private static final int KEY_MAX = Integer.MAX_VALUE;

	private static final int VALUE_LENGTH = 118;

	private static final int MEMORY_SIZE = 1024 * 1024 * 64;
	
	private static final int MEMORY_PAGE_SIZE = 32 * 1024; 

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
}
