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
import org.apache.flink.api.common.operators.base.OuterJoinOperatorBase.OuterJoinType;
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
 * outer join through a sort-merge join strategy.
 */
public abstract class AbstractMergeOuterJoinIterator<T1, T2, O> extends AbstractMergeIterator<T1, T2, O> {

	private final OuterJoinType outerJoinType;

	private boolean initialized = false;
	private boolean it1Empty = false;
	private boolean it2Empty = false;


	public AbstractMergeOuterJoinIterator(
			OuterJoinType outerJoinType,
			MutableObjectIterator<T1> input1,
			MutableObjectIterator<T2> input2,
			TypeSerializer<T1> serializer1, TypeComparator<T1> comparator1,
			TypeSerializer<T2> serializer2, TypeComparator<T2> comparator2,
			TypePairComparator<T1, T2> pairComparator,
			MemoryManager memoryManager,
			IOManager ioManager,
			int numMemoryPages,
			AbstractInvokable parentTask)
			throws MemoryAllocationException {
		super(input1, input2, serializer1, comparator1, serializer2, comparator2, pairComparator, memoryManager, ioManager, numMemoryPages, parentTask);

		this.outerJoinType = outerJoinType;
	}

	/**
	 * Calls the <code>JoinFunction#join()</code> method for all two key-value pairs that share the same key and come
	 * from different inputs. Furthermore, depending on the outer join type (LEFT, RIGHT, FULL), all key-value pairs where no
	 * matching partner from the other input exists are joined with null.
	 * The output of the <code>join()</code> method is forwarded.
	 *
	 * @throws Exception Forwards all exceptions from the user code and the I/O system.
	 * @see org.apache.flink.runtime.operators.util.JoinTaskIterator#callWithNextKey(org.apache.flink.api.common.functions.FlatJoinFunction, org.apache.flink.util.Collector)
	 */
	@Override
	public boolean callWithNextKey(final FlatJoinFunction<T1, T2, O> joinFunction, final Collector<O> collector) throws Exception {
		if (!initialized) {
			//first run, set iterators to first elements
			it1Empty = !this.iterator1.nextKey();
			it2Empty = !this.iterator2.nextKey();
			initialized = true;
		}

		if (it1Empty && it2Empty) {
			return false;
		} else if (it2Empty) {
			if (outerJoinType == OuterJoinType.LEFT || outerJoinType == OuterJoinType.FULL) {
				joinLeftKeyValuesWithNull(iterator1.getValues(), joinFunction, collector);
				it1Empty = !iterator1.nextKey();
				return true;
			} else {
				//consume rest of left side
				while (iterator1.nextKey()) {
				}
				it1Empty = true;
				return false;
			}
		} else if (it1Empty) {
			if (outerJoinType == OuterJoinType.RIGHT || outerJoinType == OuterJoinType.FULL) {
				joinRightKeyValuesWithNull(iterator2.getValues(), joinFunction, collector);
				it2Empty = !iterator2.nextKey();
				return true;
			} else {
				//consume rest of right side
				while (iterator2.nextKey()) {
				}
				it2Empty = true;
				return false;
			}
		} else {
			final TypePairComparator<T1, T2> comparator = super.pairComparator;
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
					//right key < left key
					if (outerJoinType == OuterJoinType.RIGHT || outerJoinType == OuterJoinType.FULL) {
						//join right key values with null in case of right or full outer join
						joinRightKeyValuesWithNull(iterator2.getValues(), joinFunction, collector);
						it2Empty = !iterator2.nextKey();
						return true;
					} else {
						//skip this right key if it is a left outer join
						if (!this.iterator2.nextKey()) {
							//if right side is empty, join current left key values with null
							joinLeftKeyValuesWithNull(iterator1.getValues(), joinFunction, collector);
							it1Empty = !iterator1.nextKey();
							it2Empty = true;
							return true;
						}
						current2 = this.iterator2.getCurrent();
					}
				} else {
					//right key > left key
					if (outerJoinType == OuterJoinType.LEFT || outerJoinType == OuterJoinType.FULL) {
						//join left key values with null in case of left or full outer join
						joinLeftKeyValuesWithNull(iterator1.getValues(), joinFunction, collector);
						it1Empty = !iterator1.nextKey();
						return true;
					} else {
						//skip this left key if it is a right outer join
						if (!this.iterator1.nextKey()) {
							//if right side is empty, join current right key values with null
							joinRightKeyValuesWithNull(iterator2.getValues(), joinFunction, collector);
							it1Empty = true;
							it2Empty = !iterator2.nextKey();
							return true;
						}
						comparator.setReference(this.iterator1.getCurrent());
					}
				}
			}

			// here, we have a common key! call the join function with the cross product of the
			// values
			final Iterator<T1> values1 = this.iterator1.getValues();
			final Iterator<T2> values2 = this.iterator2.getValues();

			crossMatchingGroup(values1, values2, joinFunction, collector);
			it1Empty = !iterator1.nextKey();
			it2Empty = !iterator2.nextKey();
			return true;
		}
	}

	private void joinLeftKeyValuesWithNull(Iterator<T1> values, FlatJoinFunction<T1, T2, O> joinFunction, Collector<O> collector) throws Exception {
		while (values.hasNext()) {
			T1 next = values.next();
			this.copy1 = createCopy(serializer1, next, copy1);
			joinFunction.join(copy1, null, collector);
		}
	}

	private void joinRightKeyValuesWithNull(Iterator<T2> values, FlatJoinFunction<T1, T2, O> joinFunction, Collector<O> collector) throws Exception {
		while (values.hasNext()) {
			T2 next = values.next();
			this.copy2 = createCopy(serializer2, next, copy2);
			joinFunction.join(null, copy2, collector);
		}
	}

}
