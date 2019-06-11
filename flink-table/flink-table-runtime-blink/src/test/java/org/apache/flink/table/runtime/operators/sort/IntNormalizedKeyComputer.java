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

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;

/**
 * Example for int {@link NormalizedKeyComputer}.
 */
public class IntNormalizedKeyComputer implements NormalizedKeyComputer {

	public static final IntNormalizedKeyComputer INSTANCE = new IntNormalizedKeyComputer();

	@Override
	public void putKey(BaseRow record, MemorySegment target, int offset) {
		// write first null byte.
		if (record.isNullAt(0)) {
			SortUtil.minNormalizedKey(target, offset, 5);
		} else {
			target.put(offset, (byte) 1);
			SortUtil.putIntNormalizedKey(record.getInt(0), target, offset + 1, 4);
		}

		// revert 4 bytes to compare easier.
		target.putInt(offset, Integer.reverseBytes(target.getInt(offset)));
	}

	@Override
	public int compareKey(MemorySegment segI, int offsetI, MemorySegment segJ, int offsetJ) {
		int int1 = segI.getInt(offsetI);
		int int2 = segJ.getInt(offsetJ);
		if (int1 != int2) {
			return (int1 < int2) ^ (int1 < 0) ^ (int2 < 0) ? -1 : 1;
		}

		byte byte1 = segI.get(offsetI + 4);
		byte byte2 = segJ.get(offsetJ + 4);
		if (byte1 != byte2) {
			return (byte1 < byte2) ^ (byte1 < 0) ^ (byte2 < 0) ? -1 : 1;
		}
		return 0;
	}

	@Override
	public void swapKey(MemorySegment segI, int offsetI, MemorySegment segJ, int offsetJ) {

		int temp0 = segI.getInt(offsetI);
		segI.putInt(offsetI, segJ.getInt(offsetJ));
		segJ.putInt(offsetJ, temp0);

		byte temp1 = segI.get(offsetI + 4);
		segI.put(offsetI + 4, segJ.get(offsetJ + 4));
		segJ.put(offsetJ + 4, temp1);

	}

	@Override
	public int getNumKeyBytes() {
		return 5;
	}

	@Override
	public boolean isKeyFullyDetermines() {
		return true;
	}

	@Override
	public boolean invertKey() {
		return false;
	}

}
