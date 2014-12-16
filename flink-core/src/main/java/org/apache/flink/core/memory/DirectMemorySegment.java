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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

//
//  TESTS NEEDED
//
//  - All get and put method for primitives
//    - in range aligned address
//    - in range unaligned address
//    - out of range (< 0 pos and >= size pos)
//    - on disposed buffer in range
//    - on disposed buffer out of range (< 0 pos and >= size pos)
// 
//  - get and put methods with byte buffers :
//    - with direct byte buffers
//    - with heap byte buffers
//    - with sliced direct byte buffers
//    - with sliced heap byte buffers
//    - out of range and in range
//
//
//

/**
 * This class uses in parts code from Java's direct byte buffer API.
 * 
 * The use in this class two crucial additions:
 *  - It uses collapsed checks for range check and memory segment disposal.
 *  - It offers absolute positioning methods for byte array put/get methods, to guarantee thread safe use.
 *  
 * In addition, the code that uses this class should make sure that only one implementation class is ever loaded -
 * Either the {@link HeapMemorySegment}, or this DirectMemorySegment. That way, all the abstract methods in the
 * MemorySegment base class have only one loaded actual implementation. This is easy for the JIT to recognize through
 * class hierarchy analysis, or by identifying that the invocations are monomorphic (all go to the same concrete
 * method implementation). Under this precondition, the JIT can perfectly inline methods.
 * 
 * This is harder to do and control with byte buffers, where different code paths use different versions of the class
 * (heap, direct, mapped) and thus virtual method invocations are polymorphic and are not as easily inlined.
 */
public final class DirectMemorySegment extends MemorySegment {
	
	/** The direct byte buffer that allocated the memory */
	private final ByteBuffer buffer;
	
	/** The address to the off-heap data */
	private long address;
	
	/** The address one byte after the last addressable byte.
	 *  This is address + size while the segment is not disposed */
	private final long addressLimit;
	
	/** The size in bytes of the memory segment */
	private final int size;
	
	// -------------------------------------------------------------------------
	//                             Constructors
	// -------------------------------------------------------------------------

	public DirectMemorySegment(int size) {
		this(ByteBuffer.allocateDirect(size));
	}

	public DirectMemorySegment(ByteBuffer buffer) {
		if (buffer == null || !buffer.isDirect()) {
			throw new IllegalArgumentException();
		}
		
		this.buffer = buffer;
		this.size = buffer.capacity();
		this.address = getAddress(buffer);
		this.addressLimit = this.address + size;
		
		if (address >= Long.MAX_VALUE - Integer.MAX_VALUE) {
			throw new RuntimeException("Segment initialized with too large address: " + address);
		}
	}

	// -------------------------------------------------------------------------
	//                        MemorySegment Accessors
	// -------------------------------------------------------------------------
	

	@Override
	public final boolean isFreed() {
		return this.address <= this.addressLimit;
	}

	public final void free() {
		// this ensures we can place no more data and trigger
		// the checks for the freed segment
		this.address = this.addressLimit + 1;
	}
	
	@Override
	public final int size() {
		return this.size;
	}

	@Override
	public ByteBuffer wrap(int offset, int length) {
		if (offset < 0 || offset > this.size || offset > this.size - length) {
			throw new IndexOutOfBoundsException();
		}
		
		this.buffer.limit(offset + length);
		this.buffer.position(offset);
		
		return this.buffer;
	}


	// ------------------------------------------------------------------------
	//                    Random Access get() and put() methods
	// ------------------------------------------------------------------------

	@Override
	@SuppressWarnings("restriction")
	public final byte get(int index) {
		
		final long pos = address + index;
		if (index >= 0 && pos < addressLimit) {
			return UNSAFE.getByte(pos);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("disposed");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	@SuppressWarnings("restriction")
	public final void put(int index, byte b) {
		
		final long pos = address + index;
		if (index >= 0 && pos < addressLimit) {
			UNSAFE.putByte(pos, b);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("disposed");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public final void get(int index, byte[] dst) {
		get(index, dst, 0, dst.length);
	}

	@Override
	public final void put(int index, byte[] src) {
		put(index, src, 0, src.length);
	}

	@Override
	@SuppressWarnings("restriction")
	public final void get(int index, byte[] dst, int offset, int length) {
		
		// check the byte array offset and length
		if ((offset | length | (offset + length) | (dst.length - (offset + length))) < 0) {
			throw new IndexOutOfBoundsException();
		}
		
		long pos = address + index;
		
		if (index >= 0 && pos < addressLimit - length) {
			long arrayAddress = BYTE_ARRAY_BASE_OFFSET + offset;
			
			// the copy must proceed in batches not too large, because the JVM may
			// poll for points that are safe for GC (moving the array and changing its address)
			while (length > 0) {
				long toCopy = (length > COPY_PER_BATCH) ? COPY_PER_BATCH : length;
				UNSAFE.copyMemory(null, pos, dst, arrayAddress, toCopy);
				length -= toCopy;
				pos += toCopy;
				arrayAddress += toCopy;
			}
		}
		else if (address <= 0) {
			throw new IllegalStateException("disposed");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	@SuppressWarnings("restriction")
	public final void put(int index, byte[] src, int offset, int length) {
		// check the byte array offset and length
		if ((offset | length | (offset + length) | (src.length - (offset + length))) < 0) {
			throw new IndexOutOfBoundsException();
		}
		
		long pos = address + index;
		
		if (index >= 0 && pos < addressLimit - length) {
		
			long arrayAddress = BYTE_ARRAY_BASE_OFFSET + offset;
			while (length > 0) {
				long toCopy = (length > COPY_PER_BATCH) ? COPY_PER_BATCH : length;
				UNSAFE.copyMemory(src, arrayAddress, null, pos, toCopy);
				length -= toCopy;
				pos += toCopy;
				arrayAddress += toCopy;
			}
		}
		else if (address <= 0) {
			throw new IllegalStateException("disposed");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	public final boolean getBoolean(int index) {
		return get(index) != 0;
	}

	@Override
	public final void putBoolean(int index, boolean value) {
		put(index, (byte) (value ? 1 : 0));
	}

	@Override
	@SuppressWarnings("restriction")
	public final char getChar(int index) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 2) {
			return UNSAFE.getChar(pos);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("disposed");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}
	
	@Override
	@SuppressWarnings("restriction")
	public final void putChar(int index, char value) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 2) {
			UNSAFE.putChar(pos, value);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("disposed");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	@SuppressWarnings("restriction")
	public final short getShort(int index) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 2) {
			return UNSAFE.getShort(pos);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("disposed");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	@SuppressWarnings("restriction")
	public final void putShort(int index, short value) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 2) {
			UNSAFE.putShort(pos, value);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("disposed");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}
	
	@Override
	@SuppressWarnings("restriction")
	public final int getInt(int index) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 4) {
			return UNSAFE.getInt(pos);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("disposed");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	@SuppressWarnings("restriction")
	public final void putInt(int index, int value) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 4) {
			UNSAFE.putInt(pos, value);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("disposed");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}
	
	@Override
	@SuppressWarnings("restriction")
	public final long getLong(int index) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 8) {
			return UNSAFE.getLong(pos);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("disposed");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}

	@Override
	@SuppressWarnings("restriction")
	public final void putLong(int index, long value) {
		final long pos = address + index;
		if (index >= 0 && pos <= addressLimit - 8) {
			UNSAFE.putLong(pos, value);
		}
		else if (address > addressLimit) {
			throw new IllegalStateException("disposed");
		}
		else {
			// index is in fact invalid
			throw new IndexOutOfBoundsException();
		}
	}
	
	// -------------------------------------------------------------------------
	//                     Bulk Read and Write Methods
	// -------------------------------------------------------------------------
	
	@Override
	public final void get(DataOutput out, int offset, int length) throws IOException {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public final void put(DataInput in, int offset, int length) throws IOException {
		throw new UnsupportedOperationException("not implemented");
	}
	
	@Override
	@SuppressWarnings("restriction")
	public final void get(int offset, ByteBuffer target, int numBytes) {
		
		// check the byte array offset and length
		if ((offset | numBytes | (offset + numBytes) | (size - (offset + numBytes))) < 0) {
			throw new IndexOutOfBoundsException();
		}
		
		final int targetOffset = target.position();
		final int remaining = target.remaining();
		
		if (remaining < numBytes) {
			throw new BufferOverflowException();
		}
		
		if (target.isDirect()) {
			// copy to the target memory directly
			final long targetPointer = getAddress(target) + targetOffset;
			final long sourcePointer = address + offset;
			
			if (sourcePointer <= addressLimit - numBytes) {
				UNSAFE.copyMemory(sourcePointer, targetPointer, numBytes);
			}
			else if (address > addressLimit) {
				throw new IllegalStateException("disposed");
			}
			else {
				throw new IndexOutOfBoundsException();
			}
		}
		else if (target.hasArray()) {
			// move directly into the byte array
			get(offset, target.array(), targetOffset + target.arrayOffset(), numBytes);
			
			// this must be after the get() call to ensue that the byte buffer is not
			// modified in case the call fails
			target.position(targetOffset + numBytes);
		}
		else {
			// neither heap buffer nor direct buffer
			while (target.hasRemaining()) {
				target.put(get(offset++));
			}
		}
	}
	
	@Override
	@SuppressWarnings("restriction")
	public final void put(int offset, ByteBuffer source, int numBytes) {
		
		// check the byte array offset and length
		if ((offset | numBytes | (offset + numBytes) | (size - (offset + numBytes))) < 0) {
			throw new IndexOutOfBoundsException();
		}
		
		final int sourceOffset = source.position();
		final int remaining = source.remaining();
		
		if (remaining < numBytes) {
			throw new BufferUnderflowException();
		}
		
		if (source.isDirect()) {
			// copy to the target memory directly
			final long sourcePointer = getAddress(source) + sourceOffset;
			final long targetPointer = address + offset;
			
			if (sourcePointer <= addressLimit - numBytes) {
				UNSAFE.copyMemory(sourcePointer, targetPointer, numBytes);
			}
			else if (address > addressLimit) {
				throw new IllegalStateException("disposed");
			}
			else {
				throw new IndexOutOfBoundsException();
			}
		}
		else if (source.hasArray()) {
			// move directly into the byte array
			put(offset, source.array(), sourceOffset + source.arrayOffset(), numBytes);
			
			// this must be after the get() call to ensue that the byte buffer is not
			// modified in case the call fails
			source.position(sourceOffset + numBytes);
		}
		else {
			// neither heap buffer nor direct buffer
			while (source.hasRemaining()) {
				put(offset++, source.get());
			}
		}
	}
	
	@Override
	@SuppressWarnings("restriction")
	public final void copyTo(int offset, MemorySegment target, int targetOffset, int numBytes) {
		if (target.getClass() == DirectMemorySegment.class) {
			DirectMemorySegment directOther = (DirectMemorySegment) target;
			
			final long thisPointer = address + offset;
			final long otherPointer = directOther.address + targetOffset;
			
			if (numBytes >= 0 && thisPointer <= addressLimit - numBytes && otherPointer <= directOther.addressLimit - numBytes) {
				UNSAFE.copyMemory(thisPointer, otherPointer, numBytes);
			}
			else if (address > addressLimit || directOther.address > directOther.addressLimit) {
				throw new IllegalStateException("disposed");
			}
			else {
				throw new IndexOutOfBoundsException();
			}
		}
		else {
			throw new IllegalArgumentException("Can only copy to other direct memory segments");
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                     Utilities for native memory accesses and checks
	// --------------------------------------------------------------------------------------------
	
	@SuppressWarnings("restriction")
	private static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;
	
	@SuppressWarnings("restriction")
	private static final long BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
	
	private static final long COPY_PER_BATCH = 1024 * 1024;
	
	private static final Field ADDRESS_FIELD;
	
	static {
		try {
			ADDRESS_FIELD = java.nio.Buffer.class.getDeclaredField("address");
			ADDRESS_FIELD.setAccessible(true);
		}
		catch (Throwable t) {
			throw new RuntimeException("Cannot initialize DirectMemorySegment - direct memory not supported by Flink");
		}
	}
	
	private static long getAddress(ByteBuffer buf) {
		try {
			return (Long) ADDRESS_FIELD.get(buf);
		} catch (Throwable t) {
			throw new RuntimeException("Could not access direct byte buffer address.", t);
		}
	}
}
