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

import java.io.IOException;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;

@Internal
public final class CharComparator extends BasicTypeComparator<Character> {

	private static final long serialVersionUID = 1L;

	
	public CharComparator(boolean ascending) {
		super(ascending);
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		char c1 = firstSource.readChar();
		char c2 = secondSource.readChar();
		int comp = (c1 < c2 ? -1 : (c1 == c2 ? 0 : 1)); 
		return ascendingComparison ? comp : -comp; 
	}

	@Override
	public boolean supportsNormalizedKey() {
		return true;
	}

	@Override
	public int getNormalizeKeyLen() {
		return 2;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return keyBytes < 2;
	}

	@Override
	public void putNormalizedKey(Character value, MemorySegment target, int offset, int numBytes) {
		// note that the char is an unsigned data type in java and consequently needs
		// no code that transforms the signed representation to an offsetted representation
		// that is equivalent to unsigned, when compared byte by byte
		if (numBytes == 2) {
			// default case, full normalized key
			target.put(offset,     (byte) ((value >>> 8) & 0xff));
			target.put(offset + 1, (byte) ((value      ) & 0xff));
		}
		else if (numBytes <= 0) {
		}
		else if (numBytes == 1) {
			target.put(offset,     (byte) ((value >>> 8) & 0xff));
		}
		else {
			target.put(offset,     (byte) ((value >>> 8) & 0xff));
			target.put(offset + 1, (byte) ((value      ) & 0xff));
			for (int i = 2; i < numBytes; i++) {
				target.put(offset + i, (byte) 0);
			}
		}
	}

	@Override
	public CharComparator duplicate() {
		return new CharComparator(ascendingComparison);
	}
}
