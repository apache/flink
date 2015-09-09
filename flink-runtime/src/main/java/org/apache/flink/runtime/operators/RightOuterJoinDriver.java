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

package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.AbstractMergeOuterJoinIterator.OuterJoinType;
import org.apache.flink.runtime.operators.sort.NonReusingMergeOuterJoinIterator;
import org.apache.flink.runtime.operators.sort.ReusingMergeOuterJoinIterator;
import org.apache.flink.runtime.operators.util.JoinTaskIterator;
import org.apache.flink.util.MutableObjectIterator;

/**
 * The right outer join driver implements the logic of an outer join operator at runtime. It instantiates a sort-merge based strategy to find
 * joining pairs of records or joins records from the right side with null if no match is found.
 */
public class RightOuterJoinDriver<IT1, IT2, OT> extends AbstractOuterJoinDriver<IT1, IT2, OT> {
	
	@Override
	protected JoinTaskIterator<IT1, IT2, OT> getReusingOuterJoinIterator(
			DriverStrategy driverStrategy,
			MutableObjectIterator<IT1> in1,
			MutableObjectIterator<IT2> in2,
			TypeSerializer<IT1> serializer1,
			TypeComparator<IT1> comparator1,
			TypeSerializer<IT2> serializer2,
			TypeComparator<IT2> comparator2,
			TypePairComparatorFactory<IT1, IT2> pairComparatorFactory,
			MemoryManager memoryManager,
			IOManager ioManager,
			int numPages
	) throws Exception {
		switch (driverStrategy) {
			case RIGHT_OUTER_MERGE:
				return new ReusingMergeOuterJoinIterator<>(
						OuterJoinType.RIGHT,
						in1,
						in2,
						serializer1,
						comparator1,
						serializer2,
						comparator2,
						pairComparatorFactory.createComparator12(comparator1, comparator2),
						memoryManager,
						ioManager,
						numPages,
						super.taskContext.getOwningNepheleTask()
				);
			default:
				throw new Exception("Unsupported driver strategy for right outer join driver: " + driverStrategy.name());
		}
	}
	
	@Override
	protected JoinTaskIterator<IT1, IT2, OT> getNonReusingOuterJoinIterator(
			DriverStrategy driverStrategy,
			MutableObjectIterator<IT1> in1,
			MutableObjectIterator<IT2> in2,
			TypeSerializer<IT1> serializer1,
			TypeComparator<IT1> comparator1,
			TypeSerializer<IT2> serializer2,
			TypeComparator<IT2> comparator2,
			TypePairComparatorFactory<IT1, IT2> pairComparatorFactory,
			MemoryManager memoryManager,
			IOManager ioManager,
			int numPages
	) throws Exception {
		switch (driverStrategy) {
			case RIGHT_OUTER_MERGE:
				return new NonReusingMergeOuterJoinIterator<>(
						OuterJoinType.RIGHT,
						in1,
						in2,
						serializer1,
						comparator1,
						serializer2,
						comparator2,
						pairComparatorFactory.createComparator12(comparator1, comparator2),
						memoryManager,
						ioManager,
						numPages,
						super.taskContext.getOwningNepheleTask()
				);
			default:
				throw new Exception("Unsupported driver strategy for right outer join driver: " + driverStrategy.name());
		}
	}
}