/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Comparator;

/**
 * This class is an adapter between {@link PriorityComparator} and a full {@link Comparator} that respects the
 * contract between {@link Comparator#compare(Object, Object)} and {@link Object#equals(Object)}. This is currently
 * needed for implementations of
 * {@link org.apache.flink.runtime.state.heap.CachingInternalPriorityQueueSet.OrderedSetCache} that are implemented
 * on top of a data structure that relies on the this contract, e.g. a tree set. We should replace this in the near
 * future.
 *
 * @param <T> type of the compared elements.
 */
public class TieBreakingPriorityComparator<T> implements Comparator<T>, PriorityComparator<T> {

	/** The {@link PriorityComparator} to which we delegate in a first step. */
	@Nonnull
	private final PriorityComparator<T> priorityComparator;

	/** Serializer for instances of the compared objects. */
	@Nonnull
	private final TypeSerializer<T> serializer;

	/** Stream that we use in serialization. */
	@Nonnull
	private final ByteArrayOutputStreamWithPos outStream;

	/** {@link org.apache.flink.core.memory.DataOutputView} around outStream. */
	@Nonnull
	private final DataOutputViewStreamWrapper outView;

	public TieBreakingPriorityComparator(
		@Nonnull PriorityComparator<T> priorityComparator,
		@Nonnull TypeSerializer<T> serializer,
		@Nonnull ByteArrayOutputStreamWithPos outStream,
		@Nonnull DataOutputViewStreamWrapper outView) {

		this.priorityComparator = priorityComparator;
		this.serializer = serializer;
		this.outStream = outStream;
		this.outView = outView;
	}

	@SuppressWarnings("unchecked")
	@Override
	public int compare(T o1, T o2) {

		// first we compare priority, this should be the most commonly hit case
		int cmp = priorityComparator.comparePriority(o1, o2);

		if (cmp != 0) {
			return cmp;
		}

		// here we start tie breaking and do our best to comply with the compareTo/equals contract, first we try
		// to simply find an existing way to fully compare.
		if (o1 instanceof Comparable && o1.getClass().equals(o2.getClass())) {
			return ((Comparable<T>) o1).compareTo(o2);
		}

//		// we catch this case before moving to more expensive tie breaks.
//		if (o1.equals(o2)) {
//			return 0;
//		}

		// if objects are not equal, their serialized form should somehow differ as well. this can be costly, and...
		// TODO we should have an alternative approach in the future, e.g. a cache that does not rely on compare to check equality.
		try {
			outStream.reset();
			serializer.serialize(o1, outView);
			int leftLen = outStream.getPosition();
			serializer.serialize(o2, outView);
			int rightLen = outStream.getPosition() - leftLen;
			return compareBytes(outStream.getBuf(), 0, leftLen, leftLen, rightLen);
		} catch (IOException ex) {
			throw new FlinkRuntimeException("Serializer problem in comparator.", ex);
		}
	}

	@Override
	public int comparePriority(T left, T right) {
		return priorityComparator.comparePriority(left, right);
	}

	public static int compareBytes(byte[] bytes, int offLeft, int leftLen, int offRight, int rightLen) {
		int maxLen = Math.min(leftLen, rightLen);
		for (int i = 0; i < maxLen; ++i) {
			int cmp = bytes[offLeft + i] - bytes[offRight + i];
			if (cmp != 0) {
				return cmp;
			}
		}
		return leftLen - rightLen;
	}
}
