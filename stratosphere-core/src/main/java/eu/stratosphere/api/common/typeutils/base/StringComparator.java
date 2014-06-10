/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.common.typeutils.base;

import java.io.IOException;

import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.types.StringValue;



public final class StringComparator extends BasicTypeComparator<String> {

	private static final long serialVersionUID = 1L;
	
	private static final int HIGH_BIT = 0x1 << 7;
	
	private static final int HIGH_BIT2 = 0x1 << 13;
	
	private static final int HIGH_BIT2_MASK = 0x3 << 6;
	
	
	public StringComparator(boolean ascending) {
		super(ascending);
	}

	@Override
	public int compare(DataInputView firstSource, DataInputView secondSource) throws IOException {
		int comp = StringValue.compareUnicode(firstSource, secondSource);
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
	public void putNormalizedKey(String record, MemorySegment target, int offset, int len) {;
		final int limit = offset + len;
		final int end = record.length();
		int pos = 0;
		
		while (pos < end && offset < limit) {
			char c = record.charAt(pos++);
			if (c < HIGH_BIT) {
				target.put(offset++, (byte) c);
			}
			else if (c < HIGH_BIT2) {
				target.put(offset++, (byte) ((c >>> 7) | HIGH_BIT));
				if (offset < limit) {
					target.put(offset++, (byte) c);
				}
			}
			else {
				target.put(offset++, (byte) ((c >>> 10) | HIGH_BIT2_MASK));
				if (offset < limit) {
					target.put(offset++, (byte) (c >>> 2));
				}
				if (offset < limit) {
					target.put(offset++, (byte) c);
				}
			}
		}
		while (offset < limit) {
			target.put(offset++, (byte) 0);
		}
	}


	@Override
	public StringComparator duplicate() {
		return new StringComparator(ascendingComparison);
	}
}
