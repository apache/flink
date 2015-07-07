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

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.api.common.operators.base.OuterJoinOperatorBase.OuterJoinType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.Test;

public class NonReusingSortMergeOuterJoinIteratorITCase extends AbstractSortMergeOuterJoinIteratorITCase {

	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	protected <T1, T2, T3> AbstractMergeOuterJoinIterator createOuterJoinIterator(OuterJoinType outerJoinType, MutableObjectIterator<T1> input1,
																			  MutableObjectIterator<T2> input2, TypeSerializer<T1> serializer1,
																			  TypeComparator<T1> comparator1, TypeSerializer<T2> serializer2, TypeComparator<T2> comparator2,
																			  TypePairComparator<T1, T2> pairComparator, MemoryManager memoryManager, IOManager ioManager,
																			  int numMemoryPages, AbstractInvokable parentTask) throws Exception {
		return new NonReusingMergeOuterJoinIterator(outerJoinType, input1, input2, serializer1, comparator1,
				serializer2, comparator2, pairComparator, memoryManager, ioManager, numMemoryPages, parentTask);
	}

	@Test
	public void testFullOuterWithSample() throws Exception {
		super.testFullOuterWithSample();
	}

	@Test
	public void testLeftOuterWithSample() throws Exception {
		super.testLeftOuterWithSample();
	}

	@Test
	public void testRightOuterWithSample() throws Exception {
		super.testRightOuterWithSample();
	}

	@Test
	public void testRightSideEmpty() throws Exception {
		super.testRightSideEmpty();
	}

	@Test
	public void testLeftSideEmpty() throws Exception {
		super.testLeftSideEmpty();
	}

	@Test
	public void testFullOuterJoinWithHighNumberOfCommonKeys() {
		testOuterJoinWithHighNumberOfCommonKeys(OuterJoinType.FULL, 200, 500, 2048, 0.02f, 200, 500, 2048, 0.02f);
	}

	@Test
	public void testLeftOuterJoinWithHighNumberOfCommonKeys() {
		testOuterJoinWithHighNumberOfCommonKeys(OuterJoinType.LEFT, 200, 10, 4096, 0.02f, 100, 4000, 2048, 0.02f);
	}

	@Test
	public void testRightOuterJoinWithHighNumberOfCommonKeys() {
		testOuterJoinWithHighNumberOfCommonKeys(OuterJoinType.RIGHT, 100, 10, 2048, 0.02f, 200, 4000, 4096, 0.02f);
	}
}
