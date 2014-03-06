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



public final class IntComparator extends BasicTypeComparator<Integer> {

	private static final long serialVersionUID = 1L;

	
	public IntComparator(boolean ascending) {
		super(ascending);
	}

	@Override
	public int compare(DataInputView firstSource, DataInputView secondSource) throws IOException {
		int i1 = firstSource.readInt();
		int i2 = secondSource.readInt();
		return (i1 < i2 ? -1 : (i1 == i2 ? 0 : 1));
	}


	@Override
	public boolean supportsNormalizedKey() {
		return true;
	}

	@Override
	public int getNormalizeKeyLen() {
		return 4;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return keyBytes < 4;
	}

	@Override
	public void putNormalizedKey(Integer iValue, MemorySegment target, int offset, int numBytes) {
		int value = iValue.intValue() - Integer.MIN_VALUE;
		
		// see IntValue for an explanation of the logic
		if (numBytes == 4) {
			// default case, full normalized key
			target.putIntBigEndian(offset, value);
		}
		else if (numBytes <= 0) {
		}
		else if (numBytes < 4) {
			for (int i = 0; numBytes > 0; numBytes--, i++) {
				target.put(offset + i, (byte) (value >>> ((3-i)<<3)));
			}
		}
		else {
			target.putLongBigEndian(offset, value);
			for (int i = 4; i < numBytes; i++) {
				target.put(offset + i, (byte) 0);
			}
		}
	}

	@Override
	public IntComparator duplicate() {
		return new IntComparator(ascendingComparison);
	}
}
