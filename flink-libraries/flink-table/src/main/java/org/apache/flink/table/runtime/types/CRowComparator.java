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

package org.apache.flink.table.runtime.types;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * For CRow Types Comparator.
 */
@PublicEvolving
public class CRowComparator extends TypeComparator<CRow> {

	TypeComparator<Row> rowComp;

	public CRowComparator(TypeComparator<Row> rowComp) {
		this.rowComp = rowComp;
	}

	@Override
	public int hash(CRow record) {
		return rowComp.hash(record.row);
	}

	@Override
	public void setReference(CRow toCompare) {
		rowComp.setReference(toCompare.row);
	}

	@Override
	public boolean equalToReference(CRow candidate) {
		return rowComp.equalToReference(candidate.row);
	}

	@Override
	public int compareToReference(TypeComparator<CRow> otherComp) {
		CRowComparator otherCRowComp = (CRowComparator) otherComp;
		return rowComp.compareToReference(otherCRowComp.rowComp);
	}

	@Override
	public int compare(CRow first, CRow second) {
		return rowComp.compare(first.row, second.row);
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		return rowComp.compareSerialized(firstSource, secondSource);
	}

	@Override
	public boolean supportsNormalizedKey() {
		return rowComp.supportsNormalizedKey();
	}

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return rowComp.supportsSerializationWithKeyNormalization();
	}

	@Override
	public boolean supportsCompareAgainstReference() {
		return super.supportsCompareAgainstReference();
	}

	@Override
	public int getNormalizeKeyLen() {
		return rowComp.getNormalizeKeyLen();
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return rowComp.isNormalizedKeyPrefixOnly(keyBytes);
	}

	@Override
	public void putNormalizedKey(CRow record, MemorySegment target, int offset, int numBytes) {
		rowComp.putNormalizedKey(record.row, target, offset, numBytes);
	}

	@Override
	public void writeWithKeyNormalization(CRow record, DataOutputView target) throws IOException {
		rowComp.writeWithKeyNormalization(record.row, target);
		target.writeBoolean(record.change);
	}

	@Override
	public CRow readWithKeyDenormalization(CRow reuse, DataInputView source) throws IOException {
		Row row = rowComp.readWithKeyDenormalization(reuse.row, source);
		reuse.row = row;
		reuse.change = source.readBoolean();
		return reuse;
	}

	@Override
	public boolean invertNormalizedKey() {
		return rowComp.invertNormalizedKey();
	}

	@Override
	public TypeComparator<CRow> duplicate() {
		return new CRowComparator(rowComp.duplicate());
	}

	@Override
	public int extractKeys(Object record, Object[] target, int index) {
		return rowComp.extractKeys(((CRow) record).row, target, index);
	}

	@Override
	public TypeComparator[] getFlatComparators() {
		return rowComp.getFlatComparators();
	}
}
