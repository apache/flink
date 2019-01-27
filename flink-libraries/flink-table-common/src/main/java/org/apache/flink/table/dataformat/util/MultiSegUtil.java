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

package org.apache.flink.table.dataformat.util;

import org.apache.flink.core.memory.MemorySegment;

import java.nio.ByteOrder;

/**
 * Multi memory segments utils.
 */
public class MultiSegUtil {

	public static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
	private static final int BIT_BYTE_POSITION_MASK = 0xfffffff8;
	private static final int BIT_BYTE_INDEX_MASK = 0x00000007;

	public static void bitUnSet(MemorySegment[] segments, int baseOffset, int index) {
		if (segments.length == 1) {
			MemorySegment segment = segments[0];
			int offset = baseOffset + ((index & BIT_BYTE_POSITION_MASK) >>> 3);
			byte current = segment.get(offset);
			current &= ~(1 << (index & BIT_BYTE_INDEX_MASK));
			segment.put(offset, current);
		} else {
			bitUnSetMultiSeg(segments, baseOffset, index);
		}
	}

	private static void bitUnSetMultiSeg(MemorySegment[] segments, int baseOffset, int index) {
		int offset = baseOffset + ((index & BIT_BYTE_POSITION_MASK) >>> 3);
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %
		MemorySegment segment = segments[segIndex];

		byte current = segment.get(segOffset);
		current &= ~(1 << (index & BIT_BYTE_INDEX_MASK));
		segment.put(segOffset, current);
	}

	public static void bitSet(MemorySegment[] segments, int baseOffset, int index) {
		if (segments.length == 1) {
			int offset = baseOffset + ((index & BIT_BYTE_POSITION_MASK) >>> 3);
			MemorySegment segment = segments[0];
			byte current = segment.get(offset);
			current |= (1 << (index & BIT_BYTE_INDEX_MASK));
			segment.put(offset, current);
		} else {
			bitSetMultiSeg(segments, baseOffset, index);
		}
	}

	private static void bitSetMultiSeg(MemorySegment[] segments, int baseOffset, int index) {
		int offset = baseOffset + ((index & BIT_BYTE_POSITION_MASK) >>> 3);
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %
		MemorySegment segment = segments[segIndex];

		byte current = segment.get(segOffset);
		current |= (1 << (index & BIT_BYTE_INDEX_MASK));
		segment.put(segOffset, current);
	}

	public static boolean bitGet(MemorySegment[] segments, int baseOffset, int index) {
		int offset = baseOffset + ((index & BIT_BYTE_POSITION_MASK) >>> 3);
		byte current = getByte(segments, offset);
		return (current & (1 << (index & BIT_BYTE_INDEX_MASK))) != 0;
	}

	public static boolean getBoolean(MemorySegment[] segments, int offset) {
		if (segments.length == 1) {
			return segments[0].getBoolean(offset);
		} else {
			return getBooleanMultiSeg(segments, offset);
		}
	}

	private static boolean getBooleanMultiSeg(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %
		return segments[segIndex].getBoolean(segOffset);
	}

	public static void setBoolean(MemorySegment[] segments, int offset, boolean value) {
		if (segments.length == 1) {
			segments[0].putBoolean(offset, value);
		} else {
			setBooleanMultiSeg(segments, offset, value);
		}
	}

	private static void setBooleanMultiSeg(MemorySegment[] segments, int offset, boolean value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %
		segments[segIndex].putBoolean(segOffset, value);
	}

	public static byte getByte(MemorySegment[] segments, int offset) {
		if (segments.length == 1) {
			return segments[0].get(offset);
		} else {
			return getByteMultiSeg(segments, offset);
		}
	}

	private static byte getByteMultiSeg(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %
		return segments[segIndex].get(segOffset);
	}

	public static void setByte(MemorySegment[] segments, int offset, byte value) {
		if (segments.length == 1) {
			segments[0].put(offset, value);
		} else {
			setByteMultiSeg(segments, offset, value);
		}
	}

	private static void setByteMultiSeg(MemorySegment[] segments, int offset, byte value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %
		segments[segIndex].put(segOffset, value);
	}

	public static int getInt(MemorySegment[] segments, int offset) {
		if (segments.length == 1) {
			return segments[0].getInt(offset);
		} else {
			return getIntMultiSeg(segments, offset);
		}
	}

	private static int getIntMultiSeg(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 3) {
			return segments[segIndex].getInt(segOffset);
		} else {
			return getIntSlowly(segments, segSize, segIndex, segOffset);
		}
	}

	private static int getIntSlowly(
			MemorySegment[] segments, int segSize, int segNum, int segOffset) {
		MemorySegment segment = segments[segNum];
		int ret = 0;
		for (int i = 0; i < 4; i++) {
			if (segOffset == segSize) {
				segment = segments[++segNum];
				segOffset = 0;
			}
			int unsignedByte = segment.get(segOffset) & 0xff;
			if (LITTLE_ENDIAN) {
				ret |= (unsignedByte << (i * 8));
			} else {
				ret |= (unsignedByte << ((3 - i) * 8));
			}
			segOffset++;
		}
		return ret;
	}

	public static void setInt(MemorySegment[] segments, int offset, int value) {
		if (segments.length == 1) {
			segments[0].putInt(offset, value);
		} else {
			setIntMultiSeg(segments, offset, value);
		}
	}

	private static void setIntMultiSeg(MemorySegment[] segments, int offset, int value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 3) {
			segments[segIndex].putInt(segOffset, value);
		} else {
			setIntSlowly(segments, segSize, segIndex, segOffset, value);
		}
	}

	private static void setIntSlowly(
			MemorySegment[] segments, int segSize, int segNum, int segOffset, int value) {
		MemorySegment segment = segments[segNum];
		for (int i = 0; i < 4; i++) {
			if (segOffset == segSize) {
				segment = segments[++segNum];
				segOffset = 0;
			}
			int unsignedByte;
			if (LITTLE_ENDIAN) {
				unsignedByte = value >> (i * 8);
			} else {
				unsignedByte = value >> ((3 - i) * 8);
			}
			segment.put(segOffset, (byte) unsignedByte);
			segOffset++;
		}
	}

	public static long getLong(MemorySegment[] segments, int offset) {
		if (segments.length == 1) {
			return segments[0].getLong(offset);
		} else {
			return getLongMultiSeg(segments, offset);
		}
	}

	private static long getLongMultiSeg(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 7) {
			return segments[segIndex].getLong(segOffset);
		} else {
			return getLongSlowly(segments, segSize, segIndex, segOffset);
		}
	}

	private static long getLongSlowly(
			MemorySegment[] segments, int segSize, int segNum, int segOffset) {
		MemorySegment segment = segments[segNum];
		long ret = 0;
		for (int i = 0; i < 8; i++) {
			if (segOffset == segSize) {
				segment = segments[++segNum];
				segOffset = 0;
			}
			long unsignedByte = segment.get(segOffset) & 0xff;
			if (LITTLE_ENDIAN) {
				ret |= (unsignedByte << (i * 8));
			} else {
				ret |= (unsignedByte << ((7 - i) * 8));
			}
			segOffset++;
		}
		return ret;
	}

	public static void setLong(MemorySegment[] segments, int offset, long value) {
		if (segments.length == 1) {
			segments[0].putLong(offset, value);
		} else {
			setLongMultiSeg(segments, offset, value);
		}
	}

	private static void setLongMultiSeg(MemorySegment[] segments, int offset, long value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 7) {
			segments[segIndex].putLong(segOffset, value);
		} else {
			setLongSlowly(segments, segSize, segIndex, segOffset, value);
		}
	}

	private static void setLongSlowly(
			MemorySegment[] segments, int segSize, int segNum, int segOffset, long value) {
		MemorySegment segment = segments[segNum];
		for (int i = 0; i < 8; i++) {
			if (segOffset == segSize) {
				segment = segments[++segNum];
				segOffset = 0;
			}
			long unsignedByte;
			if (LITTLE_ENDIAN) {
				unsignedByte = value >> (i * 8);
			} else {
				unsignedByte = value >> ((7 - i) * 8);
			}
			segment.put(segOffset, (byte) unsignedByte);
			segOffset++;
		}
	}

	public static short getShort(MemorySegment[] segments, int offset) {
		if (segments.length == 1) {
			return segments[0].getShort(offset);
		} else {
			return getShortMultiSeg(segments, offset);
		}
	}

	private static short getShortMultiSeg(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 1) {
			return segments[segIndex].getShort(segOffset);
		} else {
			return (short) get2ByteSlowly(segments, segSize, segIndex, segOffset);
		}
	}

	public static void setShort(MemorySegment[] segments, int offset, short value) {
		if (segments.length == 1) {
			segments[0].putShort(offset, value);
		} else {
			setShortMultiSeg(segments, offset, value);
		}
	}

	private static void setShortMultiSeg(MemorySegment[] segments, int offset, short value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 1) {
			segments[segIndex].putShort(segOffset, value);
		} else {
			set2ByteSlowly(segments, segSize, segIndex, segOffset, value, value >> 8);
		}
	}

	public static float getFloat(MemorySegment[] segments, int offset) {
		if (segments.length == 1) {
			return segments[0].getFloat(offset);
		} else {
			return getFloatMultiSeg(segments, offset);
		}
	}

	private static float getFloatMultiSeg(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 3) {
			return segments[segIndex].getFloat(segOffset);
		} else {
			return Float.intBitsToFloat(getIntSlowly(segments, segSize, segIndex, segOffset));
		}
	}

	public static void setFloat(MemorySegment[] segments, int offset, float value) {
		if (segments.length == 1) {
			segments[0].putFloat(offset, value);
		} else {
			setFloatMultiSeg(segments, offset, value);
		}
	}

	private static void setFloatMultiSeg(MemorySegment[] segments, int offset, float value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 3) {
			segments[segIndex].putFloat(segOffset, value);
		} else {
			setIntSlowly(segments, segSize, segIndex, segOffset, Float.floatToRawIntBits(value));
		}
	}

	public static double getDouble(MemorySegment[] segments, int offset) {
		if (segments.length == 1) {
			return segments[0].getDouble(offset);
		} else {
			return getDoubleMultiSeg(segments, offset);
		}
	}

	private static double getDoubleMultiSeg(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 7) {
			return segments[segIndex].getDouble(segOffset);
		} else {
			return Double.longBitsToDouble(getLongSlowly(segments, segSize, segIndex, segOffset));
		}
	}

	public static void setDouble(MemorySegment[] segments, int offset, double value) {
		if (segments.length == 1) {
			segments[0].putDouble(offset, value);
		} else {
			setDoubleMultiSeg(segments, offset, value);
		}
	}

	private static void setDoubleMultiSeg(MemorySegment[] segments, int offset, double value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 7) {
			segments[segIndex].putDouble(segOffset, value);
		} else {
			setLongSlowly(segments, segSize, segIndex, segOffset, Double.doubleToRawLongBits(value));
		}
	}

	public static char getChar(MemorySegment[] segments, int offset) {
		if (segments.length == 1) {
			return segments[0].getChar(offset);
		} else {
			return getCharMultiSeg(segments, offset);
		}
	}

	private static char getCharMultiSeg(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 1) {
			return segments[segIndex].getChar(segOffset);
		} else {
			return (char) get2ByteSlowly(segments, segSize, segIndex, segOffset);
		}
	}

	private static int get2ByteSlowly(
			MemorySegment[] segments, int segSize, int segNum, int segOffset) {
		MemorySegment segment = segments[segNum];
		int ret = 0;
		for (int i = 0; i < 2; i++) {
			if (segOffset == segSize) {
				segment = segments[++segNum];
				segOffset = 0;
			}
			int unsignedByte = segment.get(segOffset) & 0xff;
			if (LITTLE_ENDIAN) {
				ret |= (unsignedByte << (i * 8));
			} else {
				ret |= (unsignedByte << ((1 - i) * 8));
			}
			segOffset++;
		}
		return ret;
	}

	public static void setChar(MemorySegment[] segments, int offset, char value) {
		if (segments.length == 1) {
			segments[0].putChar(offset, value);
		} else {
			setCharMultiSeg(segments, offset, value);
		}
	}

	private static void setCharMultiSeg(MemorySegment[] segments, int offset, char value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 3) {
			segments[segIndex].putChar(segOffset, value);
		} else {
			set2ByteSlowly(segments, segSize, segIndex, segOffset, value, value >> 8);
		}
	}

	private static void set2ByteSlowly(
			MemorySegment[] segments, int segSize, int segNum, int segOffset, int b1, int b2) {
		MemorySegment segment = segments[segNum];
		segment.put(segOffset, (byte) (LITTLE_ENDIAN ? b1 : b2));
		segOffset++;
		if (segOffset == segSize) {
			segment = segments[++segNum];
			segOffset = 0;
		}
		segment.put(segOffset, (byte) (LITTLE_ENDIAN ? b2 : b1));
	}

	/**
	 * Maybe not copied, if want copy, please use copyTo.
	 */
	public static byte[] getBytes(MemorySegment[] segments, int baseOffset, int sizeInBytes) {
		// avoid copy if `base` is `byte[]`
		if (segments.length == 1) {
			byte[] heapMemory = segments[0].getHeapMemory();
			if (baseOffset == 0
					&& heapMemory != null
					&& heapMemory.length == sizeInBytes) {
				return heapMemory;
			} else {
				byte[] bytes = new byte[sizeInBytes];
				segments[0].get(baseOffset, bytes, 0, sizeInBytes);
				return bytes;
			}
		} else {
			byte[] bytes = new byte[sizeInBytes];
			BinaryRowUtil.copySlow(segments, baseOffset, bytes, 0, sizeInBytes);
			return bytes;
		}
	}
}
