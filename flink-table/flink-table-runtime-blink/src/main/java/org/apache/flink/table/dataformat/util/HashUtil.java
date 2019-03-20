package org.apache.flink.table.dataformat.util;

import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.util.MurmurHashUtil;

import static org.apache.flink.core.memory.MemoryUtils.UNSAFE;

public class HashUtil {

	public static final int BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

	public static int hashInt(int value) {
		return Integer.hashCode(value);
	}

	public static int hashLong(long value) {
		return Long.hashCode(value);
	}

	public static int hashShort(short value) {
		return Short.hashCode(value);
	}

	public static int hashByte(byte value) {
		return Byte.hashCode(value);
	}

	public static int hashFloat(float value) {
		return Float.hashCode(value);
	}

	public static int hashDouble(double value) {
		return Double.hashCode(value);
	}

	public static int hashBoolean(boolean value) {
		return Boolean.hashCode(value);
	}

	public static int hashChar(char value) {
		return Character.hashCode(value);
	}

	public static int hashObject(Object value) {
		return value.hashCode();
	}

	public static int hashString(BinaryString value) {
		return value.hashCode();
	}

	public static int hashDecimal(Decimal value) {
		return value.hashCode();
	}

	public static int hashBinary(byte[] value) {
		return MurmurHashUtil.hashUnsafeBytes(value, BYTE_ARRAY_BASE_OFFSET, value.length);
	}
}
