/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.iterative.io;

import java.io.EOFException;
import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.pact.runtime.hash.HashPartition;
import eu.stratosphere.util.MutableObjectIterator;

/**
 * {@link Iterator} over the buildside entries of a {@link HashPartition}
 * 
 * @param <BT>
 */
public class HashPartitionIterator<BT, PT> implements MutableObjectIterator<BT> {

	private final Iterator<HashPartition<BT, PT>> partitions;

	private final TypeSerializer<BT> serializer;

	private HashPartition<BT, PT> currentPartition;

	public HashPartitionIterator(Iterator<HashPartition<BT, PT>> partitions, TypeSerializer<BT> serializer) {
		this.partitions = partitions;
		this.serializer = serializer;
		currentPartition = null;
	}

	@Override
	public BT next(BT reuse) throws IOException {
		if (currentPartition == null) {
			if (!partitions.hasNext()) {
				return null;
			}
			currentPartition = partitions.next();
			currentPartition.setReadPosition(0);
		}

		try {
			reuse = serializer.deserialize(reuse, currentPartition);
		} catch (EOFException e) {
			reuse =  advanceAndRead(reuse);
		}

		return reuse;
	}

	/* jump to the next partition and continue reading from that */
	private BT advanceAndRead(BT reuse) throws IOException {
		if (!partitions.hasNext()) {
			return null;
		}
		currentPartition = partitions.next();
		currentPartition.setReadPosition(0);

		try {
			reuse = serializer.deserialize(reuse, currentPartition);
		} catch (EOFException e) {
			reuse = advanceAndRead(reuse);
		}
		return reuse;
	}

}
