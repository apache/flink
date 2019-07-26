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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDate;

/**
 * This class can not extend {@link BasicTypeComparator}, because LocalDate is a
 * Comparable of ChronoLocalDate instead of Comparable of LocalDate.
 */
@Internal
public final class LocalDateComparator extends TypeComparator<LocalDate> implements Serializable {

	private transient LocalDate reference;

	protected final boolean ascendingComparison;

	// For use by getComparators
	@SuppressWarnings("rawtypes")
	private final LocalDateComparator[] comparators = new LocalDateComparator[] {this};

	public LocalDateComparator(boolean ascending) {
		this.ascendingComparison = ascending;
	}

	@Override
	public int hash(LocalDate value) {
		return value.hashCode();
	}

	@Override
	public void setReference(LocalDate toCompare) {
		this.reference = toCompare;
	}

	@Override
	public boolean equalToReference(LocalDate candidate) {
		return candidate.equals(reference);
	}

	@Override
	public int compareToReference(TypeComparator<LocalDate> referencedComparator) {
		int comp = ((LocalDateComparator) referencedComparator).reference.compareTo(reference);
		return ascendingComparison ? comp : -comp;
	}

	@Override
	public int compare(LocalDate first, LocalDate second) {
		int cmp = first.compareTo(second);
		return ascendingComparison ? cmp : -cmp;
	}

	@Override
	public boolean invertNormalizedKey() {
		return !ascendingComparison;
	}

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public void writeWithKeyNormalization(LocalDate record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int extractKeys(Object record, Object[] target, int index) {
		target[index] = record;
		return 1;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public TypeComparator[] getFlatComparators() {
		return comparators;
	}

	@Override
	public LocalDate readWithKeyDenormalization(LocalDate reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		return compareSerializedLocalDate(firstSource, secondSource, ascendingComparison);
	}

	@Override
	public boolean supportsNormalizedKey() {
		return true;
	}

	@Override
	public int getNormalizeKeyLen() {
		return 6;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return keyBytes < getNormalizeKeyLen();
	}

	@Override
	public void putNormalizedKey(LocalDate record, MemorySegment target, int offset, int numBytes) {
		putNormalizedKeyLocalDate(record, target, offset, numBytes);
	}

	@Override
	public LocalDateComparator duplicate() {
		return new LocalDateComparator(ascendingComparison);
	}

	// --------------------------------------------------------------------------------------------
	//                           Static Helpers for Date Comparison
	// --------------------------------------------------------------------------------------------

	public static int compareSerializedLocalDate(DataInputView firstSource, DataInputView secondSource,
			boolean ascendingComparison) throws IOException {
		int cmp = firstSource.readInt() - secondSource.readInt();
		if (cmp == 0) {
			cmp = firstSource.readByte() - secondSource.readByte();
			if (cmp == 0) {
				cmp = firstSource.readByte() - secondSource.readByte();
			}
		}
		return ascendingComparison ? cmp : -cmp;
	}

	public static void putNormalizedKeyLocalDate(LocalDate record, MemorySegment target, int offset, int numBytes) {
		int year = record.getYear();
		int unsignedYear = year - Integer.MIN_VALUE;
		if (numBytes >= 4) {
			target.putIntBigEndian(offset, unsignedYear);
			numBytes -= 4;
			offset += 4;
		} else if (numBytes > 0) {
			for (int i = 0; numBytes > 0; numBytes--, i++) {
				target.put(offset + i, (byte) (unsignedYear >>> ((3 - i) << 3)));
			}
			return;
		}

		int month = record.getMonthValue();
		if (numBytes > 0) {
			target.put(offset, (byte) (month & 0xff - Byte.MIN_VALUE));
			numBytes -= 1;
			offset += 1;
		}

		int day = record.getDayOfMonth();
		if (numBytes > 0) {
			target.put(offset, (byte) (day & 0xff - Byte.MIN_VALUE));
			numBytes -= 1;
			offset += 1;
		}

		for (int i = 0; i < numBytes; i++) {
			target.put(offset + i, (byte) 0);
		}
	}
}
