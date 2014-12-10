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

package org.apache.flink.test.iterative.nephele.customdanglingpagerank.types;

import java.io.IOException;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.base.LongComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

public final class VertexWithRankAndDanglingComparator extends TypeComparator<VertexWithRankAndDangling> {

	private static final long serialVersionUID = 1L;
	
	private long reference;

	@SuppressWarnings("rawtypes")
	private TypeComparator[] comparators = new TypeComparator[]{new LongComparator(true)};

	@Override
	public int hash(VertexWithRankAndDangling record) {
		final long value = record.getVertexID();
		return 43 + (int) (value ^ value >>> 32);
	}

	@Override
	public void setReference(VertexWithRankAndDangling toCompare) {
		this.reference = toCompare.getVertexID();
	}

	@Override
	public boolean equalToReference(VertexWithRankAndDangling candidate) {
		return candidate.getVertexID() == this.reference;
	}

	@Override
	public int compareToReference(TypeComparator<VertexWithRankAndDangling> referencedComparator) {
		VertexWithRankAndDanglingComparator comp = (VertexWithRankAndDanglingComparator) referencedComparator;
		final long diff = comp.reference - this.reference;
		return diff < 0 ? -1 : diff > 0 ? 1 : 0;
	}
	
	@Override
	public int compare(VertexWithRankAndDangling first, VertexWithRankAndDangling second) {
		final long diff = first.getVertexID() - second.getVertexID();
		return diff < 0 ? -1 : diff > 0 ? 1 : 0;
	}

	@Override
	public int compareSerialized(DataInputView source1, DataInputView source2) throws IOException {
		final long diff = source1.readLong() - source2.readLong();
		return diff < 0 ? -1 : diff > 0 ? 1 : 0;
	}

	@Override
	public boolean supportsNormalizedKey() {
		return true;
	}

	@Override
	public int getNormalizeKeyLen() {
		return 8;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return keyBytes < 8;
	}

	@Override
	public void putNormalizedKey(VertexWithRankAndDangling record, MemorySegment target, int offset, int len) {
		final long value = record.getVertexID() - Long.MIN_VALUE;
		
		// see IntValue for an explanation of the logic
		if (len == 8) {
			// default case, full normalized key
			target.putLongBigEndian(offset, value);
		}
		else if (len <= 0) {
		}
		else if (len < 8) {
			for (int i = 0; len > 0; len--, i++) {
				target.put(offset + i, (byte) ((value >>> ((3-i)<<3)) & 0xff));
			}
		}
		else {
			target.putLongBigEndian(offset, value);
			for (int i = 8; i < len; i++) {
				target.put(offset + i, (byte) 0);
			}
		}
	}

	@Override
	public boolean invertNormalizedKey() {
		return false;
	}
	
	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return true;
	}

	@Override
	public void writeWithKeyNormalization(VertexWithRankAndDangling record, DataOutputView target) throws IOException {
		target.writeLong(record.getVertexID() - Long.MIN_VALUE);
		target.writeDouble(record.getRank());
		target.writeBoolean(record.isDangling());
	}

	@Override
	public VertexWithRankAndDangling readWithKeyDenormalization(VertexWithRankAndDangling reuse, DataInputView source) throws IOException {
		reuse.setVertexID(source.readLong() + Long.MIN_VALUE);
		reuse.setRank(source.readDouble());
		reuse.setDangling(source.readBoolean());
		return reuse;
	}

	@Override
	public VertexWithRankAndDanglingComparator duplicate() {
		return new VertexWithRankAndDanglingComparator();
	}

	@Override
	public int extractKeys(Object record, Object[] target, int index) {
		target[index] = ((VertexWithRankAndDangling) record).getVertexID();
		return 1;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public TypeComparator[] getFlatComparators() {
		return comparators;
	}
}
