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


/**
 * This interface defines a view over a {@link eu.stratosphere.nephele.services.memorymanager.MemorySegment} that
 * can be used to write elements to the memory at random positions.
 *
 * @author Alexander Alexandrov
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface RandomAccessView
{
	/**
	 * The size of this segment.
	 * 
	 * @return The size of the underlying memory segment.
	 */
	int size();

	/**
	 * Reads the byte at the given position.
	 * 
	 * @param position The position from which the byte will be read
	 * @return The byte at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger or equal to the size of
	 *                                   the memory segment.
	 */
	byte get(int index);

	/**
	 * Writes the given byte into this buffer at the given position.
	 * 
	 * @param position The position at which the byte will be written.
	 * @param b The byte value to be written.
	 * @return This view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger or equal to the size of
	 *                                   the memory segment.
	 */
	RandomAccessView put(int index, byte b);

	/**
	 * Bulk get method. Copies dst.length memory from the specified position to
	 * the destination memory.
	 * 
	 * @param position The position at which the first byte will be read.
	 * @param dst The memory into which the memory will be copied.
	 * @return This view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or too large that the data between the 
	 *                                   index and the memory segment end is not enough to fill the destination array.
	 */
	RandomAccessView get(int index, byte[] dst);

	/**
	 * Bulk get method. Copies length memory from the specified position to the
	 * destination memory, beginning at the given offset
	 * 
	 * @param position
	 *        The position at which the first byte will be read.
	 * @param dst
	 *        The memory into which the memory will be copied.
	 * @param offset
	 *        The copying offset in the destination memory.
	 * @param length
	 *        The number of bytes to be copied.
	 * @return This view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or too large that the requested number of 
	 *                                   bytes exceed the amount of memory between the index and the memory
	 *                                   segment's end.
	 */
	RandomAccessView get(int index, byte[] dst, int offset, int length);

	/**
	 * Bulk get method. Copies length memory from the specified offset to the
	 * provided <tt>DataOutput</tt>.
	 * 
	 * @param out The data output object to copy the data to.
	 * @param offset The first byte to by copied.
	 * @param length The number of bytes to copy.
	 * @return This view itself.
	 * 
	 * @throws IOException Thrown, if the DataOutput encountered a problem upon writing.
	 */
	RandomAccessView get(DataOutput out, int offset, int length) throws IOException;

	/**
	 * Bulk put method. Copies src.length memory from the source memory into the
	 * memory segment beginning at the specified position.
	 * 
	 * @param index The position in the memory segment array, where the data is put.
	 * @param src The source array to copy the data from.
	 * @return This random access view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or too large such that the array 
	 *                                   size exceed the amount of memory between the index and the memory
	 *                                   segment's end. 
	 */
	RandomAccessView put(int index, byte[] src);

	/**
	 * Bulk put method. Copies length memory starting at position offset from
	 * the source memory into the memory segment starting at the specified
	 * index.
	 * 
	 * @param index The position in the memory segment array, where the data is put.
	 * @param src The source array to copy the data from.
	 * @param offset The offset in the source array where the copying is started.
	 * @param length The number of bytes to copy.
	 * @return This random access view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or too large such that the array 
	 *                                   portion to copy exceed the amount of memory between the index and the memory
	 *                                   segment's end.
	 */
	RandomAccessView put(int index, byte[] src, int offset, int length);

	/**
	 * Bulk put method. Copies length memory from the given DataInput to the
	 * memory starting at position offset.
	 * 
	 * @param in The DataInput to get the data from.
	 * @param offset The position in the memory segment to copy the chunk to.
	 * @param length The number of bytes to get. 
	 * @return This random access view itself.
	 * 
	 * @throws IOException Thrown, if the DataInput encountered a problem upon reading,
	 *                     such as an End-Of-File.
	 */
	RandomAccessView put(DataInput in, int offset, int length) throws IOException;

	/**
	 * Reads one byte at the given position and returns its boolean
	 * representation.
	 * 
	 * @param position The position from which the memory will be read.
	 * @return The char value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 1.
	 */
	boolean getBoolean(int index);

	/**
	 * Writes one byte containing the byte value into this buffer at the given
	 * position.
	 * 
	 * @param position The position at which the memory will be written.
	 * @param value The char value to be written.
	 * @return This view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 1.
	 */
	RandomAccessView putBoolean(int index, boolean value);

	/**
	 * Reads two memory at the given position, composing them into a char value
	 * according to the current byte order.
	 * 
	 * @param position The position from which the memory will be read.
	 * @return The char value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 2.
	 */
	char getChar(int index);

	/**
	 * Writes two memory containing the given char value, in the current byte
	 * order, into this buffer at the given position.
	 * 
	 * @param position The position at which the memory will be written.
	 * @param value The char value to be written.
	 * @return This view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 2.
	 */
	RandomAccessView putChar(int index, char value);

	/**
	 * Reads two memory at the given position, composing them into a short value
	 * according to the current byte order.
	 * 
	 * @param position The position from which the memory will be read.
	 * @return The short value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 2.
	 */
	short getShort(int index);

	/**
	 * Writes two memory containing the given short value, in the current byte
	 * order, into this buffer at the given position.
	 * 
	 * @param position The position at which the memory will be written.
	 * @param value The short value to be written.
	 * @return This view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 2.
	 */
	RandomAccessView putShort(int index, short value);

	/**
	 * Reads four memory at the given position, composing them into a int value
	 * according to the current byte order.
	 * 
	 * @param position The position from which the memory will be read.
	 * @return The int value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	int getInt(int index);

	/**
	 * Writes four memory containing the given int value, in the current byte
	 * order, into this buffer at the given position.
	 * 
	 * @param position The position at which the memory will be written.
	 * @param value The int value to be written.
	 * @return This view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	RandomAccessView putInt(int index, int value);

	/**
	 * Reads eight memory at the given position, composing them into a long
	 * value according to the current byte order.
	 * 
	 * @param position The position from which the memory will be read.
	 * @return The long value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	long getLong(int index);

	/**
	 * Writes eight memory containing the given long value, in the current byte
	 * order, into this buffer at the given position.
	 * 
	 * @param position The position at which the memory will be written.
	 * @param value The long value to be written.
	 * @return This view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	RandomAccessView putLong(int index, long value);

	/**
	 * Reads four memory at the given position, composing them into a float
	 * value according to the current byte order.
	 * 
	 * @param position The position from which the memory will be read.
	 * @return The float value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	float getFloat(int index);

	/**
	 * Writes four memory containing the given float value, in the current byte
	 * order, into this buffer at the given position.
	 * 
	 * @param position The position at which the memory will be written.
	 * @param value The float value to be written.
	 * @return This view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 4.
	 */
	RandomAccessView putFloat(int index, float value);

	/**
	 * Reads eight memory at the given position, composing them into a double
	 * value according to the current byte order.
	 * 
	 * @param position The position from which the memory will be read.
	 * @return The double value at the given position.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	double getDouble(int index);

	/**
	 * Writes eight memory containing the given double value, in the current
	 * byte order, into this buffer at the given position.
	 * 
	 * @param position The position at which the memory will be written.
	 * @param value The double value to be written.
	 * @return This view itself.
	 * 
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
	 *                                   size minus 8.
	 */
	RandomAccessView putDouble(int index, double value);

	/**
	 * Gets the byte array that backs the memory segment and this random access view.
	 * Since different regions of the backing array are used by different segments, the logical
	 * positions in this view do not correspond to the indexes in the backing array and need
	 * to be translated via the {@link #translateOffset(int)} method.
	 * 
	 * @return The backing byte array.
	 */
	public byte[] getBackingArray();

	/**
	 * Translates the given offset for this view into the offset for the backing array.
	 * 
	 * @param offset The offset to be translated.
	 * @return The corresponding position in the backing array.
	 */
	public int translateOffset(int offset);
}
