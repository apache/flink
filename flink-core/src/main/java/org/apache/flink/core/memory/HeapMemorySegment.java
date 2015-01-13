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
import java.nio.ByteBuffer;

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
 * Below is an example of the code generated for the {@link HeapMemorySegment#putLongBigEndian(int, long)}
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
 */
public class HeapMemorySegment extends MemorySegment {
	
	// flag to enable / disable boundary checks. Note that the compiler eliminates the
	// code paths of the checks (as dead code) when this constant is set to false.
	private static final boolean CHECKED = true;
	
	/** The array in which the data is stored. */
	protected byte[] memory;
	
	/** Wrapper for I/O requests. */
	protected ByteBuffer wrapper;
	
	// -------------------------------------------------------------------------
	//                             Constructors
	// -------------------------------------------------------------------------

	/**
	 * Creates a new memory segment that represents the data in the given byte array.
	 * 
	 * @param memory The byte array that holds the data.
	 */
	public HeapMemorySegment(byte[] memory) {
		this.memory = memory;
	}

	// -------------------------------------------------------------------------
	//                        MemorySegment Accessors
	// -------------------------------------------------------------------------
	

	@Override
	public final boolean isFreed() {
		return this.memory == null;
	}

	public final void free() {
		this.wrapper = null;
		this.memory = null;
	}
	
	@Override
	public final int size() {
		return this.memory.length;
	}

	@Override
	public ByteBuffer wrap(int offset, int length) {
		if (offset > this.memory.length || offset > this.memory.length - length) {
			throw new IndexOutOfBoundsException();
		}
		
		if (this.wrapper == null) {
			this.wrapper = ByteBuffer.wrap(this.memory, offset, length);
		}
		else {
			this.wrapper.limit(offset + length);
			this.wrapper.position(offset);
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


	@Override
	public final byte get(int index) {
		return this.memory[index];
	}

	@Override
	public final void put(int index, byte b) {
		this.memory[index] = b;
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
	public final void get(int index, byte[] dst, int offset, int length) {
		// system arraycopy does the boundary checks anyways, no need to check extra
		System.arraycopy(this.memory, index, dst, offset, length);
	}

	@Override
	public final void put(int index, byte[] src, int offset, int length) {
		// system arraycopy does the boundary checks anyways, no need to check extra
		System.arraycopy(src, offset, this.memory, index, length);
	}

	@Override
	public final boolean getBoolean(int index) {
		return this.memory[index] != 0;
	}

	@Override
	public final void putBoolean(int index, boolean value) {
		this.memory[index] = (byte) (value ? 1 : 0);
	}

	@Override
	@SuppressWarnings("restriction")
	public final char getChar(int index) {
		if (CHECKED) {
			if (index >= 0 && index <= this.memory.length - 2) {
				return UNSAFE.getChar(this.memory, BASE_OFFSET + index);
			} else {
				throw new IndexOutOfBoundsException();
			}
		} else {
			return UNSAFE.getChar(this.memory, BASE_OFFSET + index);
		}
	}
	
	@Override
	@SuppressWarnings("restriction")
	public final void putChar(int index, char value) {
		if (CHECKED) {
			if (index >= 0 && index <= this.memory.length - 2) {
				UNSAFE.putChar(this.memory, BASE_OFFSET + index, value);
			} else {
				throw new IndexOutOfBoundsException();
			}
		} else {
			UNSAFE.putChar(this.memory, BASE_OFFSET + index, value);
		}
	}

	@Override
	@SuppressWarnings("restriction")
	public final short getShort(int index) {
		if (CHECKED) {
			if (index >= 0 && index <= this.memory.length - 2) {
				return UNSAFE.getShort(this.memory, BASE_OFFSET + index);
			} else {
				throw new IndexOutOfBoundsException();
			}
		} else {
			return UNSAFE.getShort(this.memory, BASE_OFFSET + index);
		}
	}

	@Override
	@SuppressWarnings("restriction")
	public final void putShort(int index, short value) {
		if (CHECKED) {
			if (index >= 0 && index <= this.memory.length - 2) {
				UNSAFE.putShort(this.memory, BASE_OFFSET + index, value);
			} else {
				throw new IndexOutOfBoundsException();
			}
		} else {
			UNSAFE.putShort(this.memory, BASE_OFFSET + index, value);
		}
	}
	
	@Override
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

	@Override
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
	
	@Override
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

	@Override
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
	
	// -------------------------------------------------------------------------
	//                     Bulk Read and Write Methods
	// -------------------------------------------------------------------------
	
	@Override
	public final void get(DataOutput out, int offset, int length) throws IOException {
		out.write(this.memory, offset, length);
	}

	@Override
	public final void put(DataInput in, int offset, int length) throws IOException {
		in.readFully(this.memory, offset, length);
	}
	
	@Override
	public final void get(int offset, ByteBuffer target, int numBytes) {
		// ByteBuffer performs the boundy checks
		target.put(this.memory, offset, numBytes);
	}
	
	@Override
	public final void put(int offset, ByteBuffer source, int numBytes) {
		// ByteBuffer performs the boundy checks
		source.get(this.memory, offset, numBytes);
	}
	
	@Override
	public final void copyTo(int offset, MemorySegment target, int targetOffset, int numBytes) {
		// system arraycopy does the boundary checks anyways, no need to check extra
		System.arraycopy(this.memory, offset, ((HeapMemorySegment) target).memory, targetOffset, numBytes);
	}
	
	// -------------------------------------------------------------------------
	//                      Comparisons & Swapping
	// -------------------------------------------------------------------------
	
	public static int compare(MemorySegment seg1, MemorySegment seg2, int offset1, int offset2, int len) {
		final byte[] b1 = ((HeapMemorySegment)seg1).memory;
		final byte[] b2 = ((HeapMemorySegment)seg2).memory;

		int val = 0;
		for (int pos = 0; pos < len && (val = (b1[offset1 + pos] & 0xff) - (b2[offset2 + pos] & 0xff)) == 0; pos++);
		return val;
	}

	public static void swapBytes(MemorySegment seg1, MemorySegment seg2, byte[] tempBuffer, int offset1, int offset2, int len) {
		final byte[] b1 = ((HeapMemorySegment)seg1).memory;
		final byte[] b2 = ((HeapMemorySegment)seg2).memory;
		// system arraycopy does the boundary checks anyways, no need to check extra
		System.arraycopy(b1, offset1, tempBuffer, 0, len);
		System.arraycopy(b2, offset2, b1, offset1, len);
		System.arraycopy(tempBuffer, 0, b2, offset2, len);
	}
	
	// --------------------------------------------------------------------------------------------
	//                     Utilities for native memory accesses and checks
	// --------------------------------------------------------------------------------------------
	
	@SuppressWarnings("restriction")
	private static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;
	
	@SuppressWarnings("restriction")
	private static final long BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
}
