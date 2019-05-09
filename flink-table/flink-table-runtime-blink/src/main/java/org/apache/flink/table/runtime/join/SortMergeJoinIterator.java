/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.join;

import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.generated.Projection;
import org.apache.flink.table.generated.RecordComparator;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;
import org.apache.flink.table.typeutils.BinaryRowSerializer;
import org.apache.flink.util.MutableObjectIterator;

import java.io.Closeable;
import java.io.IOException;

/**
 * Gets probeRow and match rows for inner/left/right join.
 */
public abstract class SortMergeJoinIterator implements Closeable {

	private final Projection<BaseRow, BinaryRow> probeProjection;
	private final Projection<BaseRow, BinaryRow> bufferedProjection;
	protected final RecordComparator keyComparator;
	private final MutableObjectIterator<BaseRow> probeIterator;
	private final MutableObjectIterator<BinaryRow> bufferedIterator;

	private BaseRow probeRow;
	protected BinaryRow probeKey;
	protected BinaryRow bufferedRow;
	protected BinaryRow bufferedKey;

	protected BinaryRow matchKey;
	protected ResettableExternalBuffer matchBuffer;
	private final int[] nullFilterKeys;
	private final boolean nullSafe;
	private final boolean filterAllNulls;

	public SortMergeJoinIterator(
			BinaryRowSerializer probeSerializer,
			BinaryRowSerializer bufferedSerializer,
			Projection<BaseRow, BinaryRow> probeProjection,
			Projection<BaseRow, BinaryRow> bufferedProjection,
			RecordComparator keyComparator,
			MutableObjectIterator<BaseRow> probeIterator,
			MutableObjectIterator<BinaryRow> bufferedIterator,
			ResettableExternalBuffer buffer,
			boolean[] filterNulls) throws IOException {
		this.probeProjection = probeProjection;
		this.bufferedProjection = bufferedProjection;
		this.keyComparator = keyComparator;
		this.probeIterator = probeIterator;
		this.bufferedIterator = bufferedIterator;

		this.probeRow = probeSerializer.createInstance();
		this.bufferedRow = bufferedSerializer.createInstance();
		this.matchBuffer = buffer;
		this.nullFilterKeys = NullAwareJoinHelper.getNullFilterKeys(filterNulls);
		this.nullSafe = nullFilterKeys.length == 0;
		this.filterAllNulls = nullFilterKeys.length == filterNulls.length;

		advanceNextSuitableBufferedRow(); // advance first buffered row to compare with probe key.
	}

	protected boolean advanceNextSuitableProbeRow() throws IOException {
		while (nextProbe() && shouldFilter(probeKey)) {}
		return probeRow != null;
	}

	protected boolean advanceNextSuitableBufferedRow() throws IOException {
		while (nextBuffered() && shouldFilter(bufferedKey)) {}
		return bufferedRow != null;
	}

	private boolean shouldFilter(BinaryRow key) {
		return NullAwareJoinHelper.shouldFilter(nullSafe, filterAllNulls, nullFilterKeys, key);
	}

	protected boolean nextProbe() throws IOException {
		if ((probeRow = probeIterator.next(probeRow)) != null) {
			probeKey = probeProjection.apply(probeRow);
			return true;
		} else {
			probeRow = null;
			probeKey = null;
			return false;
		}
	}

	private boolean nextBuffered() throws IOException {
		if ((bufferedRow = bufferedIterator.next(bufferedRow)) != null) {
			bufferedKey = bufferedProjection.apply(bufferedRow);
			return true;
		} else {
			bufferedRow = null;
			bufferedKey = null;
			return false;
		}
	}

	protected void bufferMatchingRows() throws IOException {
		matchKey = probeKey.copy();
		matchBuffer.reset();
		do {
			matchBuffer.add(bufferedRow);
		} while (advanceNextSuitableBufferedRow()
				&& keyComparator.compare(probeKey, bufferedKey) == 0);
		matchBuffer.complete();
	}

	public BaseRow getProbeRow() {
		return probeRow;
	}

	public BinaryRow getMatchKey() {
		return matchKey;
	}

	public ResettableExternalBuffer getMatchBuffer() {
		return matchBuffer;
	}

	@Override
	public void close() {
		matchBuffer.close();
	}
}
