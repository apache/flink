/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.services.memorymanager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface RandomAccessView {
	/**
	 * The size of this segment.
	 * 
	 * @return
	 */
	int size();

	/**
	 * Reads the byte at the given position.
	 * 
	 * @param position
	 *        The position from which the byte will be read
	 * @return The byte at the given position
	 * @throws IndexOutOfBoundsException
	 */
	byte get(int index);

	/**
	 * Writes the given byte into this buffer at the given position.
	 * 
	 * @param position
	 *        The position at which the byte will be written
	 * @param b
	 *        The byte value to be written
	 * @throws IndexOutOfBoundsException
	 */
	RandomAccessView put(int index, byte b);

	/**
	 * Bulk get method. Copies dst.length memory from the specified position to
	 * the destination memory.
	 * 
	 * @param position
	 *        The position at which the first byte will be read
	 * @param dst
	 *        The memory into which the memory will be copied
	 * @throws IndexOutOfBoundsException
	 */
	RandomAccessView get(int index, byte[] dst);

	/**
	 * Bulk get method. Copies length memory from the specified position to the
	 * destination memory, beginning at the given offset
	 * 
	 * @param position
	 *        The position at which the first byte will be read
	 * @param dst
	 *        The memory into which the memory will be copied
	 * @param offset
	 *        The copying offset in the destination memory
	 * @param length
	 *        The length of the copied memory
	 * @throws IndexOutOfBoundsException
	 */
	RandomAccessView get(int index, byte[] dst, int offset, int length);

	/**
	 * Bulk get method. Copies length memory from the specified offset to the
	 * provided DataOutput stream.
	 * 
	 * @param out
	 * @param offset
	 * @param length
	 * @return
	 */
	RandomAccessView get(DataOutput out, int offset, int length) throws IOException;

	/**
	 * Bulk put method. Copies src.length memory from the source memory into the
	 * memory segment beginning at the specified position.
	 * 
	 * @param position
	 * @param src
	 */
	RandomAccessView put(int index, byte[] src);

	/**
	 * Bulk put method. Copies length memory starting at position offset from
	 * the source memory into the memory segment starting at the specified
	 * position.
	 * 
	 * @param position
	 * @param src
	 * @param offset
	 * @param length
	 */
	RandomAccessView put(int index, byte[] src, int offset, int length);

	/**
	 * Bulk put method. Copies length memory from the given DataInput to the
	 * memory starting at position offset.
	 * 
	 * @param in
	 * @param offset
	 * @param length
	 * @return
	 */
	RandomAccessView put(DataInput in, int offset, int length) throws IOException;

	/**
	 * Reads one byte at the given position and returns its boolean
	 * representation.
	 * 
	 * @param position
	 *        The position from which the memory will be read
	 * @return The char value at the given position
	 * @throws IndexOutOfBoundsException
	 */
	boolean getBoolean(int index);

	/**
	 * Writes one byte containing the byte value into this buffer at the given
	 * position.
	 * 
	 * @param position
	 *        The position at which the memory will be written
	 * @param value
	 *        The char value to be written
	 * @throws IndexOutOfBoundsException
	 */
	RandomAccessView putBoolean(int index, boolean value);

	/**
	 * Reads two memory at the given position, composing them into a char value
	 * according to the current byte order.
	 * 
	 * @param position
	 *        The position from which the memory will be read
	 * @return The char value at the given position
	 * @throws IndexOutOfBoundsException
	 */
	char getChar(int index);

	/**
	 * Writes two memory containing the given char value, in the current byte
	 * order, into this buffer at the given position.
	 * 
	 * @param position
	 *        The position at which the memory will be written
	 * @param value
	 *        The char value to be written
	 * @throws IndexOutOfBoundsException
	 */
	RandomAccessView putChar(int index, char value);

	/**
	 * Reads two memory at the given position, composing them into a short value
	 * according to the current byte order.
	 * 
	 * @param position
	 *        The position from which the memory will be read
	 * @return The short value at the given position
	 * @throws IndexOutOfBoundsException
	 */
	short getShort(int index);

	/**
	 * Writes two memory containing the given short value, in the current byte
	 * order, into this buffer at the given position.
	 * 
	 * @param position
	 *        The position at which the memory will be written
	 * @param value
	 *        The short value to be written
	 * @throws IndexOutOfBoundsException
	 */
	RandomAccessView putShort(int index, short value);

	/**
	 * Reads four memory at the given position, composing them into a int value
	 * according to the current byte order.
	 * 
	 * @param position
	 *        The position from which the memory will be read
	 * @return The int value at the given position
	 * @throws IndexOutOfBoundsException
	 */
	int getInt(int index);

	/**
	 * Writes four memory containing the given int value, in the current byte
	 * order, into this buffer at the given position.
	 * 
	 * @param position
	 *        The position at which the memory will be written
	 * @param value
	 *        The int value to be written
	 * @throws IndexOutOfBoundsException
	 */
	RandomAccessView putInt(int index, int value);

	/**
	 * Reads eight memory at the given position, composing them into a long
	 * value according to the current byte order.
	 * 
	 * @param position
	 *        The position from which the memory will be read
	 * @return The long value at the given position
	 * @throws IndexOutOfBoundsException
	 */
	long getLong(int index);

	/**
	 * Writes eight memory containing the given long value, in the current byte
	 * order, into this buffer at the given position.
	 * 
	 * @param position
	 *        The position at which the memory will be written
	 * @param value
	 *        The long value to be written
	 * @throws IndexOutOfBoundsException
	 */
	RandomAccessView putLong(int index, long value);

	/**
	 * Reads four memory at the given position, composing them into a float
	 * value according to the current byte order.
	 * 
	 * @param position
	 *        The position from which the memory will be read
	 * @return The float value at the given position
	 * @throws IndexOutOfBoundsException
	 */
	float getFloat(int index);

	/**
	 * Writes four memory containing the given float value, in the current byte
	 * order, into this buffer at the given position.
	 * 
	 * @param position
	 *        The position at which the memory will be written
	 * @param value
	 *        The float value to be written
	 * @throws IndexOutOfBoundsException
	 */
	RandomAccessView putFloat(int index, float value);

	/**
	 * Reads eight memory at the given position, composing them into a double
	 * value according to the current byte order.
	 * 
	 * @param position
	 *        The position from which the memory will be read
	 * @return The double value at the given position
	 * @throws IndexOutOfBoundsException
	 */
	double getDouble(int index);

	/**
	 * Writes eight memory containing the given double value, in the current
	 * byte order, into this buffer at the given position.
	 * 
	 * @param position
	 *        The position at which the memory will be written
	 * @param value
	 *        The double value to be written
	 * @throws IndexOutOfBoundsException
	 */
	RandomAccessView putDouble(int index, double value);

	public byte[] getBackingArray();

	public int translateOffset(int offset);
}
