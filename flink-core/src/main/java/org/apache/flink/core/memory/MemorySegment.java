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

package org.apache.flink.core.memory;

import org.apache.flink.annotation.Internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;

/**
 * This class represents a piece of memory managed by Flink.
 * The segment may be backed by heap memory (byte array) or by off-heap memory.
 *
 * <p>The methods for individual memory access are specialized in the classes
 * {@link org.apache.flink.core.memory.HeapMemorySegment} and
 * {@link org.apache.flink.core.memory.HybridMemorySegment}.
 * All methods that operate across two memory segments are implemented in this class,
 * to transparently handle the mixing of memory segment types.
 *
 * <p>This class fulfills conceptually a similar purpose as Java's {@link java.nio.ByteBuffer}.
 * We add this specialized class for various reasons:
 * <ul>
 *     <li>It offers additional binary compare, swap, and copy methods.</li>
 *     <li>It uses collapsed checks for range check and memory segment disposal.</li>
 *     <li>It offers absolute positioning methods for bulk put/get methods, to guarantee
 *         thread safe use.</li>
 *     <li>It offers explicit big-endian / little-endian access methods, rather than tracking internally
 *         a byte order.</li>
 *     <li>It transparently and efficiently moves data between on-heap and off-heap variants.</li>
 * </ul>
 *
 * <p><i>Comments on the implementation</i>:
 * We make heavy use of operations that are supported by native
 * instructions, to achieve a high efficiency. Multi byte types (int, long, float, double, ...)
 * are read and written with "unsafe" native commands.
 *
 * <p>Below is an example of the code generated for the {@link HeapMemorySegment#putLongBigEndian(int, long)}
 * function by the just-in-time compiler. The code is grabbed from an Oracle JVM 7 using the
 * hotspot disassembler library (hsdis32.dll) and the jvm command
 * <i>-XX:+UnlockDiagnosticVMOptions -XX:CompileCommand=print,*MemorySegment.putLongBigEndian</i>.
 * Note that this code realizes both the byte order swapping and the reinterpret cast access to
 * get a long from the byte array.
 *
 * <p><pre>
 * [Verified Entry Point]
 *   0x00007fc403e19920: sub    $0x18,%rsp
 *   0x00007fc403e19927: mov    %rbp,0x10(%rsp)    ;*synchronization entry
 *                                                 ; - org.apache.flink.runtime.memory.UnsafeMemorySegment::putLongBigEndian@-1 (line 652)
 *   0x00007fc403e1992c: mov    0xc(%rsi),%r10d    ;*getfield memory
 *                                                 ; - org.apache.flink.runtime.memory.UnsafeMemorySegment::putLong@4 (line 611)
 *                                                 ; - org.apache.flink.runtime.memory.UnsafeMemorySegment::putLongBigEndian@12 (line 653)
 *   0x00007fc403e19930: bswap  %rcx
 *   0x00007fc403e19933: shl    $0x3,%r10
 *   0x00007fc403e19937: movslq %edx,%r11
 *   0x00007fc403e1993a: mov    %rcx,0x10(%r10,%r11,1)  ;*invokevirtual putLong
 *                                                 ; - org.apache.flink.runtime.memory.UnsafeMemorySegment::putLong@14 (line 611)
 *                                                 ; - org.apache.flink.runtime.memory.UnsafeMemorySegment::putLongBigEndian@12 (line 653)
 *   0x00007fc403e1993f: add    $0x10,%rsp
 *   0x00007fc403e19943: pop    %rbp
 *   0x00007fc403e19944: test   %eax,0x5ba76b6(%rip)        # 0x00007fc4099c1000
 *                                                 ;   {poll_return}
 *   0x00007fc403e1994a: retq
 * </pre>
 *
 * <p><i>Note on efficiency</i>:
 * For best efficiency, the code that uses this class should make sure that only one
 * subclass is loaded, or that the methods that are abstract in this class are used only from one of the
 * subclasses (either the {@link org.apache.flink.core.memory.HeapMemorySegment}, or the
 * {@link org.apache.flink.core.memory.HybridMemorySegment}).
 *
 * <p>That way, all the abstract methods in the MemorySegment base class have only one loaded
 * actual implementation. This is easy for the JIT to recognize through class hierarchy analysis,
 * or by identifying that the invocations are monomorphic (all go to the same concrete
 * method implementation). Under these conditions, the JIT can perfectly inline methods.
 */
@Internal
public abstract class MemorySegment {

	/**
	 * The unsafe handle for transparent memory copied (heap / off-heap).
	 */
	@SuppressWarnings("restriction")
	protected static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;

	/**
	 * The beginning of the byte array contents, relative to the byte array object.
	 */
	@SuppressWarnings("restriction")
	protected static final long BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

	/**
	 * Constant that flags the byte order. Because this is a boolean constant, the JIT compiler can
	 * use this well to aggressively eliminate the non-applicable code paths.
	 */
	private static final boolean LITTLE_ENDIAN = (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);

	// ------------------------------------------------------------------------

	/**
	 * The heap byte array object relative to which we access the memory.
	 *
	 * <p>Is non-<tt>null</tt> if the memory is on the heap, and is <tt>null</tt>, if the memory if
	 * off the heap. If we have this buffer, we must never void this reference, or the memory
	 * segment will point to undefined addresses outside the heap and may in out-of-order execution
	 * cases cause segmentation faults.
	 */
	protected final byte[] heapMemory;

	/**
	 * The address to the data, relative to the heap memory byte array. If the heap memory byte
	 * array is <tt>null</tt>, this becomes an absolute memory address outside the heap.
	 */
	protected long address;

	/**
	 * The address one byte after the last addressable byte, i.e. <tt>address + size</tt> while the
	 * segment is not disposed.
	 */
	protected final long addressLimit;

	/**
	 * The size in bytes of the memory segment.
	 */
	protected final int size;

	/**
	 * Optional owner of the memory segment.
	 */
	private final Object owner;

	/**
	 * Creates a new memory segment that represents the memory of the byte array.
	 *
	 * <p>Since the byte array is backed by on-heap memory, this memory segment holds its
	 * data on heap. The buffer must be at least of size 8 bytes.
	 *
	 * @param buffer The byte array whose memory is represented by this memory segment.
	 */
	MemorySegment(byte[] buffer, Object owner) {
		if (buffer == null) {
			throw new NullPointerException("buffer");
		}

		this.heapMemory = buffer;
		this.address = BYTE_ARRAY_BASE_OFFSET;
		this.size = buffer.length;
		this.addressLimit = this.address + this.size;
		this.owner = owner;
	}

	/**
	 * Creates a new memory segment that represents the memory at the absolute address given
	 * by the pointer.
	 *
	 * @param offHeapAddress The address of the memory represented by this memory segment.
	 * @param size The size of this memory segment.
	 */
	MemorySegment(long offHeapAddress, int size, Object owner) {
		if (offHeapAddress <= 0) {
			throw new IllegalArgumentException("negative pointer or size");
		}
		if (offHeapAddress >= Long.MAX_VALUE - Integer.MAX_VALUE) {
			// this is necessary to make sure the collapsed checks are safe against numeric overflows
			throw new IllegalArgumentException("Segment initialized with too large address: " + offHeapAddress
					+ " ; Max allowed address is " + (Long.MAX_VALUE - Integer.MAX_VALUE - 1));
		}

		this.heapMemory = null;
		this.address = offHeapAddress;
		this.addressLimit = this.address + size;
		this.size = size;
		this.owner = owner;
	}

	// ------------------------------------------------------------------------
	// Memory Segment Operations
	// ------------------------------------------------------------------------

	/**
	 * Gets the size of the memory segment, in bytes.
	 *
	 * @return The size of the memory segment.
	 */
	public int size() {
		return size;
	}

	/**
	 * Checks whether the memory segment was freed.
	 *
	 * @return <tt>true</tt>, if the memory segment has been freed, <tt>false</tt> otherwise.
	 */
	public boolean isFreed() {
		return address > addressLimit;
	}

	/**
	 * Frees this memory segment.
	 *
	 * <p>After this operation has been called, no further operations are possible on the memory
	 * segment and will fail. The actual memory (heap or off-heap) will only be released after this
	 * memory segment object has become garbage collected.
	 */
	public void free() {
		// this ensures we can place no more data and trigger
		// the checks for the freed segment
		address = addressLimit + 1;
	}

	/**
	 * Checks whether this memory segment is backed by off-heap memory.
	 *
	 * @return <tt>true</tt>, if the memory segment is backed by off-heap memory, <tt>false</tt> if
	 * it is backed by heap memory.
	 */
	public boolean isOffHeap() {
		return heapMemory == null;
	}

	/**
	 * Returns the byte array of on-heap memory segments.
	 *
	 * @return underlying byte array
	 *
	 * @throws IllegalStateException
	 * 		if the memory segment does not represent on-heap memory
	 */
	public byte[] getArray() {
		if (heapMemory != null) {
			return heapMemory;
		} else {
			throw new IllegalStateException("Memory segment does not represent heap memory");
		}
	}

	/**
	 * Returns the memory address of off-heap memory segments.
	 *
	 * @return absolute memory address outside the heap
	 *
	 * @throws IllegalStateException
	 * 		if the memory segment does not represent off-heap memory
	 */
	public long getAddress() {
		if (heapMemory == null) {
			return address;
		} else {
			throw new IllegalStateException("Memory segment does not represent off heap memory");
		}
	}

	/**
	 * Wraps the chunk of the underlying memory located between <tt>offset</tt> and
	 * <tt>length</tt> in a NIO ByteBuffer.
	 *
	 * @param offset The offset in the memory segment.
	 * @param length The number of bytes to be wrapped as a buffer.
	 *
	 * @return A <tt>ByteBuffer</tt> backed by the specified portion of the memory segment.
	 * @throws IndexOutOfBoundsException Thrown, if offset is negative or larger than the memory segment size,
	 *                                   or if the offset plus the length is larger than the segment size.
	 */
	public abstract ByteBuffer wrap(int offset, int length);

	/**
	 * Gets the owner of this memory segment. Returns null, if the owner was not set.
	 *
	 * @return The owner of the memory segment, or null, if it does not have an owner.
	 */
	public Object getOwner() {
		return owner;
	}


	// ------------------------------------------------------------------------
	//                    Random Access get() and put() methods
	// ------------------------------------------------------------------------

	//------------------------------------------------------------------------
	// Notes on the implementation: We try to collapse as many checks as
	// possible. We need to obey the following rules to make this safe
	// against segfaults:
	//
	//  - Grab mutable fields onto the stack before checking and using. This
	//    guards us against concurrent modifications which invalidate the
	//    pointers
	//  - Use subtractions for range checks, as they are tolerant
	//------------------------------------------------------------------------

	/**
	 * Reads the byte at the given position.
	 *
	 * @param index The position from which the byte will be read
	 * @return The byte at the given position.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger or equal to the size of
	 *                                   the memory segment.
	 */
	public abstract byte get(int index);

	/**
	 * Writes the given byte into this buffer at the given position.
	 *
	 * @param index The index at which the byte will be written.
	 * @param b The byte value to be written.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger or equal to the size of
	 *                                   the memory segment.
	 */
	public abstract void put(int index, byte b);

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
	public abstract void get(int index, byte[] dst);

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
	public abstract void put(int index, byte[] src);

	/**
	 * Bulk get method. Copies length memory from the specified position to the
	 * destination memory, beginning at the given offset.
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
	public abstract void get(int index, byte[] dst, int offset, int length);

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
	public abstract void put(int index, byte[] src, int offset, int length);

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
	public abstract boolean getBoolean(int index);

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
	public abstract void putBoolean(int index, boolean value);

	/**
	 * Reads a char value from the given position, in the system's native byte order.
	 *
	 * @param index The position from which the memory will be read.
	 * @return The char value at the given position.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 2.
	 */
	@SuppressWarnings("restriction")
	public final char getChar(int index) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 2) {
			return UNSAFE.getChar(heapMemory, pos);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("This segment has been freed.");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	/**
	 * Reads a character value (16 bit, 2 bytes) from the given position, in little-endian byte order.
	 * This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getChar(int)}. For most cases (such as
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getChar(int)} is the preferable choice.
	 *
	 * @param index The position from which the value will be read.
	 * @return The character value at the given position.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment size minus 2.
	 */
	public final char getCharLittleEndian(int index) {
		if (LITTLE_ENDIAN) {
			return getChar(index);
		} else {
			return Character.reverseBytes(getChar(index));
		}
	}

	/**
	 * Reads a character value (16 bit, 2 bytes) from the given position, in big-endian byte order.
	 * This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getChar(int)}. For most cases (such as
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getChar(int)} is the preferable choice.
	 *
	 * @param index The position from which the value will be read.
	 * @return The character value at the given position.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment size minus 2.
	 */
	public final char getCharBigEndian(int index) {
		if (LITTLE_ENDIAN) {
			return Character.reverseBytes(getChar(index));
		} else {
			return getChar(index);
		}
	}

	/**
	 * Writes a char value to the given position, in the system's native byte order.
	 *
	 * @param index The position at which the memory will be written.
	 * @param value The char value to be written.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 2.
	 */
	@SuppressWarnings("restriction")
	public final void putChar(int index, char value) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 2) {
			UNSAFE.putChar(heapMemory, pos, value);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("segment has been freed");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	/**
	 * Writes the given character (16 bit, 2 bytes) to the given position in little-endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putChar(int, char)}. For most cases (such as
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putChar(int, char)} is the preferable choice.
	 *
	 * @param index The position at which the value will be written.
	 * @param value The char value to be written.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment size minus 2.
	 */
	public final void putCharLittleEndian(int index, char value) {
		if (LITTLE_ENDIAN) {
			putChar(index, value);
		} else {
			putChar(index, Character.reverseBytes(value));
		}
	}

	/**
	 * Writes the given character (16 bit, 2 bytes) to the given position in big-endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putChar(int, char)}. For most cases (such as
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putChar(int, char)} is the preferable choice.
	 *
	 * @param index The position at which the value will be written.
	 * @param value The char value to be written.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment size minus 2.
	 */
	public final void putCharBigEndian(int index, char value) {
		if (LITTLE_ENDIAN) {
			putChar(index, Character.reverseBytes(value));
		} else {
			putChar(index, value);
		}
	}

	/**
	 * Reads a short integer value (16 bit, 2 bytes) from the given position, composing them into a short value
	 * according to the current byte order.
	 *
	 * @param index The position from which the memory will be read.
	 * @return The short value at the given position.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 2.
	 */
	public final short getShort(int index) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 2) {
			return UNSAFE.getShort(heapMemory, pos);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("segment has been freed");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	/**
	 * Reads a short integer value (16 bit, 2 bytes) from the given position, in little-endian byte order.
	 * This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getShort(int)}. For most cases (such as
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getShort(int)} is the preferable choice.
	 *
	 * @param index The position from which the value will be read.
	 * @return The short value at the given position.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment size minus 2.
	 */
	public final short getShortLittleEndian(int index) {
		if (LITTLE_ENDIAN) {
			return getShort(index);
		} else {
			return Short.reverseBytes(getShort(index));
		}
	}

	/**
	 * Reads a short integer value (16 bit, 2 bytes) from the given position, in big-endian byte order.
	 * This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getShort(int)}. For most cases (such as
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getShort(int)} is the preferable choice.
	 *
	 * @param index The position from which the value will be read.
	 * @return The short value at the given position.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment size minus 2.
	 */
	public final short getShortBigEndian(int index) {
		if (LITTLE_ENDIAN) {
			return Short.reverseBytes(getShort(index));
		} else {
			return getShort(index);
		}
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
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 2) {
			UNSAFE.putShort(heapMemory, pos, value);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("segment has been freed");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	/**
	 * Writes the given short integer value (16 bit, 2 bytes) to the given position in little-endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putShort(int, short)}. For most cases (such as
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putShort(int, short)} is the preferable choice.
	 *
	 * @param index The position at which the value will be written.
	 * @param value The short value to be written.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment size minus 2.
	 */
	public final void putShortLittleEndian(int index, short value) {
		if (LITTLE_ENDIAN) {
			putShort(index, value);
		} else {
			putShort(index, Short.reverseBytes(value));
		}
	}

	/**
	 * Writes the given short integer value (16 bit, 2 bytes) to the given position in big-endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putShort(int, short)}. For most cases (such as
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putShort(int, short)} is the preferable choice.
	 *
	 * @param index The position at which the value will be written.
	 * @param value The short value to be written.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment size minus 2.
	 */
	public final void putShortBigEndian(int index, short value) {
		if (LITTLE_ENDIAN) {
			putShort(index, Short.reverseBytes(value));
		} else {
			putShort(index, value);
		}
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
	public final int getInt(int index) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 4) {
			return UNSAFE.getInt(heapMemory, pos);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("segment has been freed");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	/**
	 * Reads an int value (32bit, 4 bytes) from the given position, in little-endian byte order.
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
	 * Reads an int value (32bit, 4 bytes) from the given position, in big-endian byte order.
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
	public final void putInt(int index, int value) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 4) {
			UNSAFE.putInt(heapMemory, pos, value);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("segment has been freed");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
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
	public final long getLong(int index) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 8) {
			return UNSAFE.getLong(heapMemory, pos);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("segment has been freed");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
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
	public final void putLong(int index, long value) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 8) {
			UNSAFE.putLong(heapMemory, pos, value);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("segment has been freed");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
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

	public abstract void get(DataOutput out, int offset, int length) throws IOException;

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
	public abstract void put(DataInput in, int offset, int length) throws IOException;

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
	 * @throws ReadOnlyBufferException If the target buffer is read-only.
	 */
	public abstract void get(int offset, ByteBuffer target, int numBytes);

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
	public abstract void put(int offset, ByteBuffer source, int numBytes);

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
		final byte[] thisHeapRef = this.heapMemory;
		final byte[] otherHeapRef = target.heapMemory;
		final long thisPointer = this.address + offset;
		final long otherPointer = target.address + targetOffset;

		if ((numBytes | offset | targetOffset) >= 0 &&
				thisPointer <= this.addressLimit - numBytes && otherPointer <= target.addressLimit - numBytes) {
			UNSAFE.copyMemory(thisHeapRef, thisPointer, otherHeapRef, otherPointer, numBytes);
		}
		else if (this.address > this.addressLimit) {
			throw new IllegalStateException("this memory segment has been freed.");
		}
		else if (target.address > target.addressLimit) {
			throw new IllegalStateException("target memory segment has been freed.");
		}
		else {
			throw new IndexOutOfBoundsException(
					String.format("offset=%d, targetOffset=%d, numBytes=%d, address=%d, targetAddress=%d",
					offset, targetOffset, numBytes, this.address, target.address));
		}
	}

	// -------------------------------------------------------------------------
	//                      Comparisons & Swapping
	// -------------------------------------------------------------------------

	/**
	 * Compares two memory segment regions.
	 *
	 * @param seg2 Segment to compare this segment with
	 * @param offset1 Offset of this segment to start comparing
	 * @param offset2 Offset of seg2 to start comparing
	 * @param len Length of the compared memory region
	 *
	 * @return 0 if equal, -1 if seg1 &lt; seg2, 1 otherwise
	 */
	public final int compare(MemorySegment seg2, int offset1, int offset2, int len) {
		while (len >= 8) {
			long l1 = this.getLongBigEndian(offset1);
			long l2 = seg2.getLongBigEndian(offset2);

			if (l1 != l2) {
				return (l1 < l2) ^ (l1 < 0) ^ (l2 < 0) ? -1 : 1;
			}

			offset1 += 8;
			offset2 += 8;
			len -= 8;
		}
		while (len > 0) {
			int b1 = this.get(offset1) & 0xff;
			int b2 = seg2.get(offset2) & 0xff;
			int cmp = b1 - b2;
			if (cmp != 0) {
				return cmp;
			}
			offset1++;
			offset2++;
			len--;
		}
		return 0;
	}

	/**
	 * Swaps bytes between two memory segments, using the given auxiliary buffer.
	 *
	 * @param tempBuffer The auxiliary buffer in which to put data during triangle swap.
	 * @param seg2 Segment to swap bytes with
	 * @param offset1 Offset of this segment to start swapping
	 * @param offset2 Offset of seg2 to start swapping
	 * @param len Length of the swapped memory region
	 */
	public final void swapBytes(byte[] tempBuffer, MemorySegment seg2, int offset1, int offset2, int len) {
		if ((offset1 | offset2 | len | (tempBuffer.length - len)) >= 0) {
			final long thisPos = this.address + offset1;
			final long otherPos = seg2.address + offset2;

			if (thisPos <= this.addressLimit - len && otherPos <= seg2.addressLimit - len) {
				// this -> temp buffer
				UNSAFE.copyMemory(this.heapMemory, thisPos, tempBuffer, BYTE_ARRAY_BASE_OFFSET, len);

				// other -> this
				UNSAFE.copyMemory(seg2.heapMemory, otherPos, this.heapMemory, thisPos, len);

				// temp buffer -> other
				UNSAFE.copyMemory(tempBuffer, BYTE_ARRAY_BASE_OFFSET, seg2.heapMemory, otherPos, len);
				return;
			}
			else if (this.address > this.addressLimit) {
				throw new IllegalStateException("this memory segment has been freed.");
			}
			else if (seg2.address > seg2.addressLimit) {
				throw new IllegalStateException("other memory segment has been freed.");
			}
		}

		// index is in fact invalid
		throw new IndexOutOfBoundsException(
					String.format("offset1=%d, offset2=%d, len=%d, bufferSize=%d, address1=%d, address2=%d",
							offset1, offset2, len, tempBuffer.length, this.address, seg2.address));
	}
}
