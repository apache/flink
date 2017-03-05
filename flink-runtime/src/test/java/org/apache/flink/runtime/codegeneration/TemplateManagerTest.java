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
import org.apache.commons.math.random.RandomGenerator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.codegeneration.utils.CodeGenerationSorterBaseTest;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.util.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Random;

public class TemplateManagerTest extends CodeGenerationSorterBaseTest {
	@Test
	public void testSorterIsGeneratedOnlyOnceForSameComparator() throws MemoryAllocationException, IllegalAccessException, TemplateException, IOException, InstantiationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {

		List<MemorySegment> memory = createMemory();
		TypeSerializer serializer  = TestData.getIntStringTupleSerializer();
		TypeComparator comparator  = TestData.getIntStringTupleComparator();

		// 1st creation for this comparator
		createSorter(serializer, comparator, memory);

		SorterTemplateModel templateModel = new SorterTemplateModel(comparator);

		String sorterName = templateModel.getSorterName();
		Path filePath     = Paths.get(TemplateManager.GENERATING_PATH + "/" + sorterName + ".java");

		Random randomGenerator = new Random();

		// write a unique token to created sorter
		String token = "// Testing token:" + randomGenerator.nextInt();
		Files.write(filePath, token.getBytes(), StandardOpenOption.APPEND);

		// 2nd creation for this comparator
		createSorter(serializer, comparator, memory);

		// read that file back and check whether the token is still there.
		byte[] data = Files.readAllBytes(filePath);
		String str = new String(data, "UTF-8");

		Assert.assertTrue("TemplateManager serves sorter from cache for the 2nd call", str.contains(token) );

		this.memoryManager.release(memory);
	}
}
