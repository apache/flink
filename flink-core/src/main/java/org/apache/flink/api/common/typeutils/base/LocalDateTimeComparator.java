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
import java.time.LocalDateTime;

/**
 * This class can not extend {@link BasicTypeComparator}, because LocalDateTime is a
 * Comparable of ChronoLocalDateTime instead of Comparable of LocalDateTime.
 */
@Internal
public final class LocalDateTimeComparator extends TypeComparator<LocalDateTime> implements Serializable {

	private transient LocalDateTime reference;

	protected final boolean ascendingComparison;
	protected final LocalDateComparator dateComparator;
	protected final LocalTimeComparator timeComparator;

	// For use by getComparators
	@SuppressWarnings("rawtypes")
	private final LocalDateTimeComparator[] comparators = new LocalDateTimeComparator[] {this};

	public LocalDateTimeComparator(boolean ascending) {
		this.ascendingComparison = ascending;
		this.dateComparator = new LocalDateComparator(ascending);
		this.timeComparator = new LocalTimeComparator(ascending);
	}

	@Override
	public int hash(LocalDateTime value) {
		return value.hashCode();
	}

	@Override
	public void setReference(LocalDateTime toCompare) {
		this.reference = toCompare;
	}

	@Override
	public boolean equalToReference(LocalDateTime candidate) {
		return candidate.equals(reference);
	}

	@Override
	public int compareToReference(TypeComparator<LocalDateTime> referencedComparator) {
		int comp = ((LocalDateTimeComparator) referencedComparator).reference.compareTo(reference);
		return ascendingComparison ? comp : -comp;
	}

	@Override
	public int compare(LocalDateTime first, LocalDateTime second) {
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
	public void writeWithKeyNormalization(LocalDateTime record, DataOutputView target) throws IOException {
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
	public LocalDateTime readWithKeyDenormalization(LocalDateTime reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		int cmp = dateComparator.compareSerialized(firstSource, secondSource);
		if (cmp == 0) {
			cmp = timeComparator.compareSerialized(firstSource, secondSource);
		}
		return cmp;
	}

	@Override
	public boolean supportsNormalizedKey() {
		return true;
	}

	@Override
	public int getNormalizeKeyLen() {
		return dateComparator.getNormalizeKeyLen() + timeComparator.getNormalizeKeyLen();
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return keyBytes < getNormalizeKeyLen();
	}

	@Override
	public void putNormalizedKey(LocalDateTime record, MemorySegment target, int offset, int numBytes) {
		int dateNKLen = dateComparator.getNormalizeKeyLen();
		if (numBytes <= dateNKLen) {
			dateComparator.putNormalizedKey(record.toLocalDate(), target, offset, numBytes);
		} else {
			dateComparator.putNormalizedKey(record.toLocalDate(), target, offset, dateNKLen);
			timeComparator.putNormalizedKey(
					record.toLocalTime(), target, offset + dateNKLen, numBytes - dateNKLen);
		}
	}

	@Override
	public LocalDateTimeComparator duplicate() {
		return new LocalDateTimeComparator(ascendingComparison);
	}
}
