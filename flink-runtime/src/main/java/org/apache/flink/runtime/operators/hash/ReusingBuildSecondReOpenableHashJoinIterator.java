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


package org.apache.flink.runtime.operators.hash;

import java.io.IOException;
import java.util.List;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.MutableObjectIterator;

public class ReusingBuildSecondReOpenableHashJoinIterator<V1, V2, O> extends ReusingBuildSecondHashJoinIterator<V1, V2, O> {

	
	private final ReOpenableMutableHashTable<V2, V1> reopenHashTable;
	
	public ReusingBuildSecondReOpenableHashJoinIterator(
			MutableObjectIterator<V1> firstInput,
			MutableObjectIterator<V2> secondInput,
			TypeSerializer<V1> serializer1,
			TypeComparator<V1> comparator1,
			TypeSerializer<V2> serializer2,
			TypeComparator<V2> comparator2,
			TypePairComparator<V1, V2> pairComparator,
			MemoryManager memManager,
			IOManager ioManager,
			AbstractInvokable ownerTask,
			double memoryFraction,
			boolean probeSideOuterJoin,
			boolean buildSideOuterJoin,
			boolean useBitmapFilters) throws MemoryAllocationException {
		
		super(firstInput, secondInput, serializer1, comparator1, serializer2,
				comparator2, pairComparator, memManager, ioManager, ownerTask,
				memoryFraction, probeSideOuterJoin, buildSideOuterJoin, useBitmapFilters);
		
		reopenHashTable = (ReOpenableMutableHashTable<V2, V1>) hashJoin;
	}

	@Override
	public <BT, PT> MutableHashTable<BT, PT> getHashJoin(
			TypeSerializer<BT> buildSideSerializer, TypeComparator<BT> buildSideComparator,
			TypeSerializer<PT> probeSideSerializer, TypeComparator<PT> probeSideComparator,
			TypePairComparator<PT, BT> pairComparator,
			MemoryManager memManager, IOManager ioManager,
			AbstractInvokable ownerTask,
			double memoryFraction,
			boolean useBitmapFilters) throws MemoryAllocationException {
		
		final int numPages = memManager.computeNumberOfPages(memoryFraction);
		final List<MemorySegment> memorySegments = memManager.allocatePages(ownerTask, numPages);
		
		return new ReOpenableMutableHashTable<BT, PT>(buildSideSerializer, probeSideSerializer,
				buildSideComparator, probeSideComparator, pairComparator,
				memorySegments, ioManager, useBitmapFilters);
	}
	
	/**
	 * Set new input for probe side
	 * @throws IOException 
	 */
	public void reopenProbe(MutableObjectIterator<V1> probeInput) throws IOException {
		reopenHashTable.reopenProbe(probeInput);
	}
}
