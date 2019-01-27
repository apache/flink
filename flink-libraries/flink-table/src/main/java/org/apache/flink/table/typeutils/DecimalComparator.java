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
import org.apache.flink.table.dataformat.Decimal;

import java.io.IOException;

/**
 * Comparator for {@link Decimal}.
 */
@Internal
public final class DecimalComparator extends BasicTypeComparator<Decimal> {

	private static final long serialVersionUID = 1L;

	final DecimalSerializer serializer;
	private int precision;
	private int scale;

	public DecimalComparator(boolean ascending, int precision, int scale) {
		super(ascending);
		this.serializer = new DecimalSerializer(precision, scale);
		this.precision = precision;
		this.scale = scale;
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		Decimal d1 = serializer.deserialize(firstSource);
		Decimal d2 = serializer.deserialize(secondSource);
		int comp = d1.compareTo(d2);
		return ascendingComparison ? comp : -comp;
	}

	@Override
	public boolean supportsNormalizedKey() {
		return precision <= Decimal.MAX_COMPACT_PRECISION;
	}

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
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
	public void putNormalizedKey(Decimal record, MemorySegment target, int offset, int len) {
		throw new RuntimeException("please use codeGen!");
	}

	@Override
	public DecimalComparator duplicate() {
		return new DecimalComparator(ascendingComparison, serializer.precision, serializer.scale);
	}
}
