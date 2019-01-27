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

package org.apache.flink.table.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.base.BasicTypeComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.util.BinaryRowUtil;

import java.io.IOException;

/**
 * Comparator for {@link BinaryString}.
 */
@Internal
public final class BinaryStringComparator extends BasicTypeComparator<BinaryString> {

	private static final long serialVersionUID = 1L;

	public BinaryStringComparator(boolean ascending) {
		super(ascending);
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		BinaryString s1 = BinaryStringSerializer.deserializeInternal(firstSource);
		BinaryString s2 = BinaryStringSerializer.deserializeInternal(secondSource);
		int comp = s1.compareTo(s2);
		return ascendingComparison ? comp : -comp;
	}

	@Override
	public boolean supportsNormalizedKey() {
		return true;
	}

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public int getNormalizeKeyLen() {
		return Integer.MAX_VALUE;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return true;
	}

	@Override
	public void putNormalizedKey(BinaryString record, MemorySegment target, int offset, int len) {
		BinaryRowUtil.putBinaryStringNormalizedKey(record, target, offset, len);
	}

	@Override
	public BinaryStringComparator duplicate() {
		return new BinaryStringComparator(ascendingComparison);
	}
}
