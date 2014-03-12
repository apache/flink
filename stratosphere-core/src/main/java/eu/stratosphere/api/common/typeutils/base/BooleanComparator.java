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



public final class BooleanComparator extends BasicTypeComparator<Boolean> {

	private static final long serialVersionUID = 1L;

	
	public BooleanComparator(boolean ascending) {
		super(ascending);
	}

	@Override
	public int compare(DataInputView firstSource, DataInputView secondSource) throws IOException {
		final int fs = firstSource.readBoolean() ? 1 : 0;
		final int ss = secondSource.readBoolean() ? 1 : 0;
		return ss - fs;
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
	public void putNormalizedKey(Boolean value, MemorySegment target, int offset, int numBytes) {
		if (numBytes > 0) {
			target.put(offset, (byte) (value.booleanValue() ? 1 : 0));
			
			for (offset = offset + 1; numBytes > 1; numBytes--) {
				target.put(offset++, (byte) 0);
			}
		}
	}

	@Override
	public BooleanComparator duplicate() {
		return new BooleanComparator(ascendingComparison);
	}
}
