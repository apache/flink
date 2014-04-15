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



public final class ByteComparator extends BasicTypeComparator<Byte> {

	private static final long serialVersionUID = 1L;

	
	public ByteComparator(boolean ascending) {
		super(ascending);
	}

	@Override
	public int compare(DataInputView firstSource, DataInputView secondSource) throws IOException {
		byte b1 = firstSource.readByte();
		byte b2 = secondSource.readByte();
		int comp = (b1 < b2 ? -1 : (b1 == b2 ? 0 : 1)); 
		return ascendingComparison ? comp : -comp; 
	}


	@Override
	public boolean supportsNormalizedKey() {
		return true;
	}

	@Override
	public int getNormalizeKeyLen() {
		return 1;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return keyBytes < 1;
	}

	@Override
	public void putNormalizedKey(Byte value, MemorySegment target, int offset, int numBytes) {
		if (numBytes == 1) {
			// default case, full normalized key. need to explicitly convert to int to
			// avoid false results due to implicit type conversion to int when subtracting
			// the min byte value
			int highByte = value & 0xff;
			highByte -= Byte.MIN_VALUE;
			target.put(offset, (byte) highByte);
		}
		else if (numBytes <= 0) {
		}
		else {
			int highByte = value & 0xff;
			highByte -= Byte.MIN_VALUE;
			target.put(offset, (byte) highByte);
			for (int i = 1; i < numBytes; i++) {
				target.put(offset + i, (byte) 0);
			}
		}
	}

	@Override
	public ByteComparator duplicate() {
		return new ByteComparator(ascendingComparison);
	}
}
