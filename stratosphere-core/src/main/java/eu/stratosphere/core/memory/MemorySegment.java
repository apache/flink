/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.core.memory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This class represents a piece of memory allocated from the memory manager. The segment is backed
 * by a byte array and features random put and get methods for the basic types that are stored in a byte-wise
 * fashion in the memory.
 * 
 * <p>
 * 
 * Comments on the implementation: We make heavy use of operations that are supported by native
 * instructions, to achieve a high efficiency. Multi byte types (int, long, float, double, ...)
 * are read and written with "unsafe" native commands. Little-endian to big-endian conversion and
 * vice versa are done using the static <i>reverseBytes</i> methods in the boxing data types
 * (for example {@link Integer#reverseBytes(int)}). On x86/amd64, these are translated by the
 * jit compiler to <i>bswap</i> intrinsic commands.
 * 
 * Below is an example of the code generated for the {@link MemorySegment#putLongBigEndian(int, long)}
 * function by the just-in-time compiler. The code is grabbed from an oracle jvm 7 using the
 * hotspot disassembler library (hsdis32.dll) and the jvm command
 * <i>-XX:+UnlockDiagnosticVMOptions -XX:CompileCommand=print,*UnsafeMemorySegment.putLongBigEndian</i>.
 * Note that this code realizes both the byte order swapping and the reinterpret cast access to
 * get a long from the byte array.
 * 
 * <pre>
 * [Verified Entry Point]
 *   0x00007fc403e19920: sub    $0x18,%rsp
 *   0x00007fc403e19927: mov    %rbp,0x10(%rsp)    ;*synchronization entry
 *                                                 ; - eu.stratosphere.nephele.services.memorymanager.UnsafeMemorySegment::putLongBigEndian@-1 (line 652)
 *   0x00007fc403e1992c: mov    0xc(%rsi),%r10d    ;*getfield memory
 *                                                 ; - eu.stratosphere.nephele.services.memorymanager.UnsafeMemorySegment::putLong@4 (line 611)
 *                                                 ; - eu.stratosphere.nephele.services.memorymanager.UnsafeMemorySegment::putLongBigEndian@12 (line 653)
 *   0x00007fc403e19930: bswap  %rcx
 *   0x00007fc403e19933: shl    $0x3,%r10
 *   0x00007fc403e19937: movslq %edx,%r11
 *   0x00007fc403e1993a: mov    %rcx,0x10(%r10,%r11,1)  ;*invokevirtual putLong
 *                                                 ; - eu.stratosphere.nephele.services.memorymanager.UnsafeMemorySegment::putLong@14 (line 611)
 *                                                 ; - eu.stratosphere.nephele.services.memorymanager.UnsafeMemorySegment::putLongBigEndian@12 (line 653)
 *   0x00007fc403e1993f: add    $0x10,%rsp
 *   0x00007fc403e19943: pop    %rbp
 *   0x00007fc403e19944: test   %eax,0x5ba76b6(%rip)        # 0x00007fc4099c1000
 *                                                 ;   {poll_return}
 *   0x00007fc403e1994a: retq 
 * </pre>
 */
public class MemorySegment {
	
	// flag to enable / disable boundary checks. Note that the compiler eliminates the
	// code paths of the checks (as dead code) when this constant is set to false.
	private static final boolean CHECKED = true;
	
	/**
	 * The array in which the data is stored.
	 */
	protected byte[] memory;
	
	/**
	 * Wrapper for I/O requests.
	 */
	protected ByteBuffer wrapper;
	
	// -------------------------------------------------------------------------
	//                             Constructors
	// -------------------------------------------------------------------------

	/**
	 * Creates a new memory segment that represents the data in the given byte array.
	 * 
	 * @param memory The byte array that holds the data.
	 */
	public MemorySegment(byte[] memory) {
		this.memory = memory;
	}

	// -------------------------------------------------------------------------
	//                        MemorySegment Accessors
	// -------------------------------------------------------------------------
	
	/**
	 * Checks whether this memory segment has already been freed. In that case, the
	 * segment must not be used any more.
	 * 
	 * @return True, if the segment has been freed, false otherwise.
	 */
	public final boolean isFreed() {
		return this.memory == null;
	}

	public final void free() {
		this.wrapper = null;
		this.memory = null;
	}
	
	/**
	 * Gets the size of the memory segment, in bytes. Because segments
	 * are backed by arrays, they cannot be larger than two GiBytes.
	 * 
	 * @return The size in bytes.
	 */
	public final int size() {
		return this.memory.length;
	}

	/**
	 * Wraps the chunk of the underlying memory located between <tt>offset<tt> and 
	 * <tt>length</tt> in a NIO ByteBuffer.
	 * 
	 * @param offset The offset in the memory segment.
	 * @param length The number of bytes to be wrapped as a buffer.
	 * @return A <tt>ByteBuffer</tt> backed by the specified portion of the memory segment.
	 * @throws IndexOutOfBoundsException Thrown, if offset is negative or larger than the memory segment size,
	 *                                   or if the offset plus the length is larger than the segment size.
	 */
	public ByteBuffer wrap(int offset, int length) {
		if (offset > this.memory.length || offset > this.memory.length - length) {
			throw new IndexOutOfBoundsException();
		}
		
		if (this.wrapper == null) {
			this.wrapper = ByteBuffer.wrap(this.memory, offset, length);
		}
		else {
			this.wrapper.position(offset);
			this.wrapper.limit(offset + length);
		}
		
		return this.wrapper;
	}


	// ------------------------------------------------------------------------
	//                    Random Access get() and put() methods
	// ------------------------------------------------------------------------

	// --------------------------------------------------------------------------------------------
	// WARNING: Any code for range checking must take care to avoid integer overflows. The position
	// integer may go up to <code>Integer.MAX_VALUE</tt>. Range checks that work after the principle
	// <code>position + 3 &lt; end</code> may fail because <code>position + 3</code> becomes negative.
	// A safe solution is to subtract the delta from the limit, for example
	// <code>position &lt; end - 3</code>. Since all indices are always positive, and the integer domain
	// has one more negative value than positive values, this can never cause an underflow.
	// --------------------------------------------------------------------------------------------


	/**
	 * Reads the byte at the given position.
	 * 
	 * @param index The position from which the byte will be read
	 * @return The byte at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger or equal to the size of
	 *                                   the memory segment.
	 */
	public final byte get(int index) {
		return this.memory[index];
	}

	/**
	 * Writes the given byte into this buffer at the given position.
	 * 
	 * @param index The index at which the byte will be written.
	 * @param b The byte value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger or equal to the size of
	 *                                   the memory segment.
	 */
	public final void put(int index, byte b) {
		this.memory[index] = b;
	}

	/**
	 * Bulk get method. Copies dst.length memory from the specified position to
	 * the destination memory.
	 * 
	 * @param index The position at which the first byte will be read.
	 * @param dst The memory into which the memory will be copied.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or too large that the data between the 
	 *                                   index and the memory segment end is not enough to fill the destination array.
	 */
	public final void get(int index, byte[] dst) {
		get(index, dst, 0, dst.length);
	}

	/**
	 * Bulk put method. Copies src.length memory from the source memory into the
	 * memory segment beginning at the specified position.
	 * 
	 * @param index The index in the memory segment array, where the data is put.
	 * @param src The source array to copy the data from.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or too large such that the array 
	 *                                   size exceed the amount of memory between the index and the memory
	 *                                   segment's end. 
	 */
	public final void put(int index, byte[] src) {
		put(index, src, 0, src.length);
	}

	/**
	 * Bulk get method. Copies length memory from the specified position to the
	 * destination memory, beginning at the given offset
	 * 
	 * @param index The position at which the first byte will be read.
	 * @param dst The memory into which the memory will be copied.
	 * @param offset The copying offset in the destination memory.
	 * @param length The number of bytes to be copied.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or too large that the requested number of 
	 *                                   bytes exceed the amount of memory between the index and the memory
	 *                                   segment's end.
	 */
	public final void get(int index, byte[] dst, int offset, int length) {
		// system arraycopy does the boundary checks anyways, no need to check extra
		System.arraycopy(this.memory, index, dst, offset, length);
	}

	/**
	 * Bulk put method. Copies length memory starting at position offset from
	 * the source memory into the memory segment starting at the specified
	 * index.
	 * 
	 * @param index The position in the memory segment array, where the data is put.
	 * @param src The source array to copy the data from.
	 * @param offset The offset in the source array where the copying is started.
	 * @param length The number of bytes to copy.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or too large such that the array 
	 *                                   portion to copy exceed the amount of memory between the index and the memory
	 *                                   segment's end.
	 */
	public final void put(int index, byte[] src, int offset, int length) {
		// system arraycopy does the boundary checks anyways, no need to check extra
		System.arraycopy(src, offset, this.memory, index, length);
	}

	/**
	 * Reads one byte at the given position and returns its boolean
	 * representation.
	 * 
	 * @param index The position from which the memory will be read.
	 * @return The boolean value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 1.
	 */
	public final boolean getBoolean(int index) {
		return this.memory[index] != 0;
	}

	/**
	 * Writes one byte containing the byte value into this buffer at the given
	 * position.
	 * 
	 * @param index The position at which the memory will be written.
	 * @param value The char value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 1.
	 */
	public final void putBoolean(int index, boolean value) {
		this.memory[index] = (byte) (value ? 1 : 0);
	}

	/**
	 * Reads two memory at the given position, composing them into a char value
	 * according to the current byte order.
	 * 
	 * @param index The position from which the memory will be read.
	 * @return The char value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 2.
	 */
	public final char getChar(int index) {
		return (char) ( ((this.memory[index    ] & 0xff) << 8) | 
						(this.memory[index + 1] & 0xff) );
	}

	/**
	 * Writes two memory containing the given char value, in the current byte
	 * order, into this buffer at the given position.
	 * 
	 * @param index The position at which the memory will be written.
	 * @param value The char value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 2.
	 */
	public final void putChar(int index, char value) {
		this.memory[index    ] = (byte) (value >> 8);
		this.memory[index + 1] = (byte) value;
	}

	/**
	 * Reads two memory at the given position, composing them into a short value
	 * according to the current byte order.
	 * 
	 * @param index The position from which the memory will be read.
	 * @return The short value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 2.
	 */
	public final short getShort(int index) {
		return (short) (
				((this.memory[index    ] & 0xff) << 8) |
				((this.memory[index + 1] & 0xff)) );
	}

	/**
	 * Writes the given short value into this buffer at the given position, using
	 * the native byte order of the system.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The short value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 2.
	 */
	public final void putShort(int index, short value) {
		this.memory[index    ] = (byte) (value >> 8);
		this.memory[index + 1] = (byte) value;
	}
	
	/**
	 * Reads an int value (32bit, 4 bytes) from the given position, in the system's native byte order.
	 * This method offers the best speed for integer reading and should be used
	 * unless a specific byte order is required. In most cases, it suffices to know that the
	 * byte order in which the value is written is the same as the one in which it is read 
	 * (such as transient storage in memory, or serialization for I/O and network), making this
	 * method the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The int value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	@SuppressWarnings("restriction")
	public final int getInt(int index) {
		if (CHECKED) {
			if (index >= 0 && index <= this.memory.length - 4) {
				return UNSAFE.getInt(this.memory, BASE_OFFSET + index);
			} else {
				throw new IndexOutOfBoundsException();
			}
		} else {
			return UNSAFE.getInt(this.memory, BASE_OFFSET + index);
		}
	}
	
	/**
	 * Reads an int value (32bit, 4 bytes) from the given position, in little endian byte order.
	 * This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getInt(int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getInt(int)} is the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The int value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	public final int getIntLittleEndian(int index) {
		if (LITTLE_ENDIAN) {
			return getInt(index);
		} else {
			return Integer.reverseBytes(getInt(index));
		}
	}
	
	/**
	 * Reads an int value (32bit, 4 bytes) from the given position, in big endian byte order.
	 * This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getInt(int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getInt(int)} is the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The int value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	public final int getIntBigEndian(int index) {
		if (LITTLE_ENDIAN) {
			return Integer.reverseBytes(getInt(index));
		} else {
			return getInt(index);
		}
	}

	/**
	 * Writes the given int value (32bit, 4 bytes) to the given position in the system's native
	 * byte order. This method offers the best speed for integer writing and should be used
	 * unless a specific byte order is required. In most cases, it suffices to know that the
	 * byte order in which the value is written is the same as the one in which it is read 
	 * (such as transient storage in memory, or serialization for I/O and network), making this
	 * method the preferable choice.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The int value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	@SuppressWarnings("restriction")
	public final void putInt(int index, int value) {
		if (CHECKED) {
			if (index >= 0 && index <= this.memory.length - 4) {
				UNSAFE.putInt(this.memory, BASE_OFFSET + index, value);
			} else {
				throw new IndexOutOfBoundsException();
			}
		} else {
			UNSAFE.putInt(this.memory, BASE_OFFSET + index, value);
		}
	}
	
	/**
	 * Writes the given int value (32bit, 4 bytes) to the given position in little endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putInt(int, int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putInt(int, int)} is the preferable choice.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The int value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	public final void putIntLittleEndian(int index, int value) {
		if (LITTLE_ENDIAN) {
			putInt(index, value);
		} else {
			putInt(index, Integer.reverseBytes(value));
		}
	}
	
	/**
	 * Writes the given int value (32bit, 4 bytes) to the given position in big endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putInt(int, int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putInt(int, int)} is the preferable choice.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The int value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	public final void putIntBigEndian(int index, int value) {
		if (LITTLE_ENDIAN) {
			putInt(index, Integer.reverseBytes(value));
		} else {
			putInt(index, value);
		}
	}
	
	/**
	 * Reads a long value (64bit, 8 bytes) from the given position, in the system's native byte order.
	 * This method offers the best speed for long integer reading and should be used
	 * unless a specific byte order is required. In most cases, it suffices to know that the
	 * byte order in which the value is written is the same as the one in which it is read 
	 * (such as transient storage in memory, or serialization for I/O and network), making this
	 * method the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The long value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	@SuppressWarnings("restriction")
	public final long getLong(int index) {
		if (CHECKED) {
			if (index >= 0 && index <= this.memory.length - 8) {
				return UNSAFE.getLong(this.memory, BASE_OFFSET + index);
			} else {
				throw new IndexOutOfBoundsException();
			}
		} else {
			return UNSAFE.getLong(this.memory, BASE_OFFSET + index);
		}
	}
	
	/**
	 * Reads a long integer value (64bit, 8 bytes) from the given position, in little endian byte order.
	 * This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getLong(int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getLong(int)} is the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The long value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final long getLongLittleEndian(int index) {
		if (LITTLE_ENDIAN) {
			return getLong(index);
		} else {
			return Long.reverseBytes(getLong(index));
		}
	}
	
	/**
	 * Reads a long integer value (64bit, 8 bytes) from the given position, in big endian byte order.
	 * This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getLong(int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getLong(int)} is the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The long value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final long getLongBigEndian(int index) {
		if (LITTLE_ENDIAN) {
			return Long.reverseBytes(getLong(index));
		} else {
			return getLong(index);
		}
	}

	/**
	 * Writes the given long value (64bit, 8 bytes) to the given position in the system's native
	 * byte order. This method offers the best speed for long integer writing and should be used
	 * unless a specific byte order is required. In most cases, it suffices to know that the
	 * byte order in which the value is written is the same as the one in which it is read 
	 * (such as transient storage in memory, or serialization for I/O and network), making this
	 * method the preferable choice.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The long value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	@SuppressWarnings("restriction")
	public final void putLong(int index, long value) {
		if (CHECKED) {
			if (index >= 0 && index <= this.memory.length - 8) {
				UNSAFE.putLong(this.memory, BASE_OFFSET + index, value);
			} else {
				throw new IndexOutOfBoundsException();
			}
		} else {
			UNSAFE.putLong(this.memory, BASE_OFFSET + index, value);
		}
	}
	
	/**
	 * Writes the given long value (64bit, 8 bytes) to the given position in little endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putLong(int, long)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putLong(int, long)} is the preferable choice.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The long value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final void putLongLittleEndian(int index, long value) {
		if (LITTLE_ENDIAN) {
			putLong(index, value);
		} else {
			putLong(index, Long.reverseBytes(value));
		}
	}
	
	/**
	 * Writes the given long value (64bit, 8 bytes) to the given position in big endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putLong(int, long)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putLong(int, long)} is the preferable choice.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The long value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final void putLongBigEndian(int index, long value) {
		if (LITTLE_ENDIAN) {
			putLong(index, Long.reverseBytes(value));
		} else {
			putLong(index, value);
		}
	}
	
	/**
	 * Reads a single-precision floating point value (32bit, 4 bytes) from the given position, in the system's
	 * native byte order. This method offers the best speed for float reading and should be used
	 * unless a specific byte order is required. In most cases, it suffices to know that the
	 * byte order in which the value is written is the same as the one in which it is read 
	 * (such as transient storage in memory, or serialization for I/O and network), making this
	 * method the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The float value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	public final float getFloat(int index) {
		return Float.intBitsToFloat(getInt(index));
	}
	
	/**
	 * Reads a single-precision floating point value (32bit, 4 bytes) from the given position, in little endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getFloat(int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getFloat(int)} is the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The long value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final float getFloatLittleEndian(int index) {
		return Float.intBitsToFloat(getIntLittleEndian(index));
	}
	
	/**
	 * Reads a single-precision floating point value (32bit, 4 bytes) from the given position, in big endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getFloat(int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getFloat(int)} is the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The long value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final float getFloatBigEndian(int index) {
		return Float.intBitsToFloat(getIntBigEndian(index));
	}

	/**
	 * Writes the given single-precision float value (32bit, 4 bytes) to the given position in the system's native
	 * byte order. This method offers the best speed for float writing and should be used
	 * unless a specific byte order is required. In most cases, it suffices to know that the
	 * byte order in which the value is written is the same as the one in which it is read 
	 * (such as transient storage in memory, or serialization for I/O and network), making this
	 * method the preferable choice.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The float value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	public final void putFloat(int index, float value) {
		putInt(index, Float.floatToRawIntBits(value));
	}
	
	/**
	 * Writes the given single-precision float value (32bit, 4 bytes) to the given position in little endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putFloat(int, float)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putFloat(int, float)} is the preferable choice.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The long value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final void putFloatLittleEndian(int index, float value) {
		putIntLittleEndian(index, Float.floatToRawIntBits(value));
	}
	
	/**
	 * Writes the given single-precision float value (32bit, 4 bytes) to the given position in big endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putFloat(int, float)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putFloat(int, float)} is the preferable choice.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The long value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final void putFloatBigEndian(int index, float value) {
		putIntBigEndian(index, Float.floatToRawIntBits(value));
	}
	
	/**
	 * Reads a double-precision floating point value (64bit, 8 bytes) from the given position, in the system's
	 * native byte order. This method offers the best speed for double reading and should be used
	 * unless a specific byte order is required. In most cases, it suffices to know that the
	 * byte order in which the value is written is the same as the one in which it is read 
	 * (such as transient storage in memory, or serialization for I/O and network), making this
	 * method the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The double value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final double getDouble(int index) {
		return Double.longBitsToDouble(getLong(index));
	}
	
	/**
	 * Reads a double-precision floating point value (64bit, 8 bytes) from the given position, in little endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getDouble(int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getDouble(int)} is the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The long value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final double getDoubleLittleEndian(int index) {
		return Double.longBitsToDouble(getLongLittleEndian(index));
	}
	
	/**
	 * Reads a double-precision floating point value (64bit, 8 bytes) from the given position, in big endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getDouble(int)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getDouble(int)} is the preferable choice.
	 * 
	 * @param index The position from which the value will be read.
	 * @return The long value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final double getDoubleBigEndian(int index) {
		return Double.longBitsToDouble(getLongBigEndian(index));
	}

	/**
	 * Writes the given double-precision floating-point value (64bit, 8 bytes) to the given position in the
	 * system's native byte order. This method offers the best speed for double writing and should be used
	 * unless a specific byte order is required. In most cases, it suffices to know that the
	 * byte order in which the value is written is the same as the one in which it is read 
	 * (such as transient storage in memory, or serialization for I/O and network), making this
	 * method the preferable choice.
	 * 
	 * @param index The position at which the memory will be written.
	 * @param value The double value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final void putDouble(int index, double value) {
		putLong(index, Double.doubleToRawLongBits(value));
	}
	
	/**
	 * Writes the given double-precision floating-point value (64bit, 8 bytes) to the given position in little endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putDouble(int, double)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putDouble(int, double)} is the preferable choice.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The long value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final void putDoubleLittleEndian(int index, double value) {
		putLongLittleEndian(index, Double.doubleToRawLongBits(value));
	}
	
	/**
	 * Writes the given double-precision floating-point value (64bit, 8 bytes) to the given position in big endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putDouble(int, double)}. For most cases (such as 
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putDouble(int, double)} is the preferable choice.
	 * 
	 * @param index The position at which the value will be written.
	 * @param value The long value to be written.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	public final void putDoubleBigEndian(int index, double value) {
		putLongBigEndian(index, Double.doubleToRawLongBits(value));
	}
	
	// -------------------------------------------------------------------------
	//                     Bulk Read and Write Methods
	// -------------------------------------------------------------------------
	
	/**
	 * Bulk get method. Copies length memory from the specified offset to the
	 * provided <tt>DataOutput</tt>.
	 * 
	 * @param out The data output object to copy the data to.
	 * @param offset The first byte to by copied.
	 * @param length The number of bytes to copy.
	 * 
	 * @throws IOException Thrown, if the DataOutput encountered a problem upon writing.
	 */
	public final void get(DataOutput out, int offset, int length) throws IOException {
		out.write(this.memory, offset, length);
	}

	/**
	 * Bulk put method. Copies length memory from the given DataInput to the
	 * memory starting at position offset.
	 * 
	 * @param in The DataInput to get the data from.
	 * @param offset The position in the memory segment to copy the chunk to.
	 * @param length The number of bytes to get. 
	 * 
	 * @throws IOException Thrown, if the DataInput encountered a problem upon reading,
	 *                     such as an End-Of-File.
	 */
	public final void put(DataInput in, int offset, int length) throws IOException {
		in.readFully(this.memory, offset, length);
	}
	
	/**
	 * Bulk get method. Copies {@code numBytes} bytes from this memory segment, starting at position
	 * {@code offset} to the target {@code ByteBuffer}. The bytes will be put into the target buffer
	 * starting at the buffer's current position. If this method attempts to write more bytes than
	 * the target byte buffer has remaining (with respect to {@link ByteBuffer#remaining()}),
	 * this method will cause a {@link java.nio.BufferOverflowException}.
	 * 
	 * @param offset The position where the bytes are started to be read from in this memory segment.
	 * @param target The ByteBuffer to copy the bytes to.
	 * @param numBytes The number of bytes to copy.
	 * 
	 * @throws IndexOutOfBoundsException If the offset is invalid, or this segment does not
	 *           contain the given number of bytes (starting from offset), or the target byte buffer does
	 *           not have enough space for the bytes.
	 */
	public final void get(int offset, ByteBuffer target, int numBytes) {
		// ByteBuffer performs the boundy checks
		target.put(this.memory, offset, numBytes);
	}
	
	/**
	 * Bulk put method. Copies {@code numBytes} bytes from the given {@code ByteBuffer}, into
	 * this memory segment. The bytes will be read from the target buffer
	 * starting at the buffer's current position, and will be written to this memory segment starting
	 * at {@code offset}.
	 * If this method attempts to read more bytes than
	 * the target byte buffer has remaining (with respect to {@link ByteBuffer#remaining()}),
	 * this method will cause a {@link java.nio.BufferUnderflowException}.
	 * 
	 * @param offset The position where the bytes are started to be written to in this memory segment.
	 * @param source The ByteBuffer to copy the bytes from.
	 * @param numBytes The number of bytes to copy.
	 * 
	 * @throws IndexOutOfBoundsException If the offset is invalid, or the source buffer does not
	 *           contain the given number of bytes, or this segment does
	 *           not have enough space for the bytes (counting from offset).
	 */
	public final void put(int offset, ByteBuffer source, int numBytes) {
		// ByteBuffer performs the boundy checks
		source.get(this.memory, offset, numBytes);
	}
	
	/**
	 * Bulk copy method. Copies {@code numBytes} bytes from this memory segment, starting at position
	 * {@code offset} to the target memory segment. The bytes will be put into the target segment
	 * starting at position {@code targetOffset}.
	 * 
	 * @param offset The position where the bytes are started to be read from in this memory segment.
	 * @param target The memory segment to copy the bytes to.
	 * @param targetOffset The position in the target memory segment to copy the chunk to.
	 * @param numBytes The number of bytes to copy.
	 * 
	 * @throws IndexOutOfBoundsException If either of the offsets is invalid, or the source segment does not
	 *           contain the given number of bytes (starting from offset), or the target segment does
	 *           not have enough space for the bytes (counting from targetOffset).
	 */
	public final void copyTo(int offset, MemorySegment target, int targetOffset, int numBytes) {
		// system arraycopy does the boundary checks anyways, no need to check extra
		System.arraycopy(this.memory, offset, target.memory, targetOffset, numBytes);
	}
	
	// -------------------------------------------------------------------------
	//                      Comparisons & Swapping
	// -------------------------------------------------------------------------
	
	public static final int compare(MemorySegment seg1, MemorySegment seg2, int offset1, int offset2, int len) {
		final byte[] b1 = seg1.memory;
		final byte[] b2 = seg2.memory;
		
		int val = 0;
		for (int pos = 0; pos < len && (val = (b1[offset1 + pos] & 0xff) - (b2[offset2 + pos] & 0xff)) == 0; pos++);
		return val;
	}
	
	public static final void swapBytes(MemorySegment seg1, MemorySegment seg2, byte[] tempBuffer, int offset1, int offset2, int len) {
		// system arraycopy does the boundary checks anyways, no need to check extra
		System.arraycopy(seg1.memory, offset1, tempBuffer, 0, len);
		System.arraycopy(seg2.memory, offset2, seg1.memory, offset1, len);
		System.arraycopy(tempBuffer, 0, seg2.memory, offset2, len);
	}
	
	// --------------------------------------------------------------------------------------------
	//                     Utilities for native memory accesses and checks
	// --------------------------------------------------------------------------------------------
	
	@SuppressWarnings("restriction")
	private static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;
	
	@SuppressWarnings("restriction")
	private static final long BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
	
	private static final boolean LITTLE_ENDIAN = (MemoryUtils.NATIVE_BYTE_ORDER == ByteOrder.LITTLE_ENDIAN);
}
