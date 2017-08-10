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

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import java.util.Iterator;

/**
 * An implementation of the {@link org.apache.flink.runtime.operators.util.JoinTaskIterator} that realizes the
 * joining through a sort-merge join strategy.
 */
public abstract class AbstractMergeInnerJoinIterator<T1, T2, O> extends AbstractMergeIterator<T1, T2, O> {

	public AbstractMergeInnerJoinIterator(
			MutableObjectIterator<T1> input1, MutableObjectIterator<T2> input2,
			TypeSerializer<T1> serializer1, TypeComparator<T1> comparator1,
			TypeSerializer<T2> serializer2, TypeComparator<T2> comparator2,
			TypePairComparator<T1, T2> pairComparator,
			MemoryManager memoryManager,
			IOManager ioManager,
			int numMemoryPages,
			AbstractInvokable parentTask)
			throws MemoryAllocationException {
		super(input1, input2, serializer1, comparator1, serializer2, comparator2, pairComparator, memoryManager, ioManager, numMemoryPages, parentTask);
	}

	/**
	 * Calls the <code>JoinFunction#join()</code> method for all two key-value pairs that share the same key and come
	 * from different inputs. The output of the <code>join()</code> method is forwarded.
	 * <p>
	 * This method first zig-zags between the two sorted inputs in order to find a common
	 * key, and then calls the join stub with the cross product of the values.
	 *
	 * @throws Exception Forwards all exceptions from the user code and the I/O system.
	 * @see org.apache.flink.runtime.operators.util.JoinTaskIterator#callWithNextKey(org.apache.flink.api.common.functions.FlatJoinFunction, org.apache.flink.util.Collector)
	 */
	@Override
	public boolean callWithNextKey(final FlatJoinFunction<T1, T2, O> joinFunction, final Collector<O> collector)
			throws Exception {
		if (!this.iterator1.nextKey() || !this.iterator2.nextKey()) {
			// consume all remaining keys (hack to prevent remaining inputs during iterations, lets get rid of this soon)
			while (this.iterator1.nextKey()) {
			}
			while (this.iterator2.nextKey()) {
			}

			return false;
		}

		final TypePairComparator<T1, T2> comparator = this.pairComparator;
		comparator.setReference(this.iterator1.getCurrent());
		T2 current2 = this.iterator2.getCurrent();

		// zig zag
		while (true) {
			// determine the relation between the (possibly composite) keys
			final int comp = comparator.compareToReference(current2);

			if (comp == 0) {
				break;
			}

			if (comp < 0) {
				if (!this.iterator2.nextKey()) {
					return false;
				}
				current2 = this.iterator2.getCurrent();
			} else {
				if (!this.iterator1.nextKey()) {
					return false;
				}
				comparator.setReference(this.iterator1.getCurrent());
			}
		}

		// here, we have a common key! call the join function with the cross product of the
		// values
		final Iterator<T1> values1 = this.iterator1.getValues();
		final Iterator<T2> values2 = this.iterator2.getValues();

		crossMatchingGroup(values1, values2, joinFunction, collector);
		return true;
	}
}
