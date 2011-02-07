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

package eu.stratosphere.pact.runtime.sort;

import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.util.Iterator;

import eu.stratosphere.nephele.services.iomanager.Deserializer;
import eu.stratosphere.nephele.services.iomanager.MemoryIOWrapper;
import eu.stratosphere.nephele.services.iomanager.RawComparator;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.iomanager.Serializer;
import eu.stratosphere.nephele.services.iomanager.Writer;
import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.nephele.services.memorymanager.MemoryBacked;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.UnboundMemoryBackedException;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultDataOutputView;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemorySegmentView;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Pair;
import eu.stratosphere.pact.common.type.Value;

/**
 * Sortable buffer based on heap/stack concept where pairs are written in the heap and index into stack.
 * 
 * Caution (1): 
 *   - index is stored in stack (within memory segment)
 *   - thus Buffer.Input.read(...) == false will not work
 *   - take the BufferSortableGuaranteed.position as upper limit
 *   
 * Caution (2):
 *   - offset/order pointers are stored interleaved in the stack
 *   - order of index entries reflects the sequential write order
 *   - order of offset entries defines a total order (after sorting)
 * 
 * |        heap       |       empty      |                  stack                   |
 * | pair | pair | pair | ... | ... | ... | ... | offset (4 bytes) | index (8 bytes) |
 * 
 * @author Erik Nijkamp
 * @param <K>
 *        The type of the key.
 * @param <V>
 *        The type of the value.
 */
public final class BufferSortableGuaranteed<K extends Key, V extends Value> extends MemoryBacked implements IndexedSortable {
	
	/**
	 * {@link OutputView} wrapper which can (virtually) reduce the size of the buffer segment
	 * after being instantiated. Remember, we write the index as stack (reverse) thus
	 * the size of the buffer portion for pairs reduces with each additional index entry.
	 * 
	 * Guarantees that the index is not overwritten by pairs.
	 * 
	 * For reasons of efficiency (avoiding another layer of invocations) this code is
	 * mostly equivalent to {@link DefaultDataOutputView}
	 */
	private final class HeapStackDataOutputView extends DefaultMemorySegmentView implements DataOutputView {
		
		final DefaultMemoryManager.MemorySegmentDescriptorProxy descriptor;
		
		/**
		 * The current write size.
		 */
		private int position;
		
		/**
		 * The current (reversed) stack end.
		 */
		private int stackEndAbs;
		
		// -------------------------------------------------------------------------
		// Offsets
		// -------------------------------------------------------------------------
		public void growStack(int bytes)
		{
			stackEndAbs -= bytes;
		}
		
		public int getStackEndRel()
		{
			return stackEndAbs - descriptor.start;
		}
		
		public int getHeapEndRel()
		{
			return position - descriptor.start;
		}

		// -------------------------------------------------------------------------
		// Constructors
		// -------------------------------------------------------------------------

		public HeapStackDataOutputView(DefaultMemoryManager.MemorySegmentDescriptorProxy descriptor) {
			super(descriptor.proxee.get());
			this.descriptor = descriptor;
			resetStackHeap();
		}
		
		public void resetStackHeap()
		{
			position = this.descriptor.start;
			stackEndAbs = this.descriptor.end;
		}

		// -------------------------------------------------------------------------
		// DataOutputView
		// -------------------------------------------------------------------------

		@Override
		public int getPosition() {
			return position - descriptor.start;
		}

		@Override
		public DataOutputView setPosition(int position) {
			this.position = position + descriptor.start;
			return this;
		}

		@Override
		public DataOutputView skip(int size) {
			position += size;
			return this;
		}

		@Override
		public DataOutputView reset() {
			position = descriptor.start;
			return this;
		}

		// ------------------------------------------------------------------------
		// DataOutput
		// ------------------------------------------------------------------------

		@Override
		public void write(int b) throws IOException {
			if(descriptorReference.get() == null)
			{
				throw new NullPointerException();
			}

			if (position < stackEndAbs) {
				descriptor.memory[position++] = (byte) (b & 0xff);
			} else {
				throw new EOFException();
			}
		}

		@Override
		public void write(byte[] b) throws IOException {
			write(b, 0, b.length);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			if(descriptorReference.get() == null)
			{
				throw new NullPointerException();
			}

			if (position < stackEndAbs && position + len <= stackEndAbs && off + len <= b.length) {
				System.arraycopy(b, off, descriptor.memory, position, len);
				position += len;
			} else {
				throw new EOFException();
			}
		}

		@Override
		public void writeBoolean(boolean v) throws IOException {
			if(descriptorReference.get() == null)
			{
				throw new NullPointerException();
			}

			if (position < stackEndAbs) {
				descriptor.memory[position++] = (byte) (v ? 1 : 0);
			} else {
				throw new EOFException();
			}
		}

		@Override
		public void writeByte(int v) throws IOException {
			write(v);
		}

		@Override
		public void writeBytes(String s) throws IOException {
			if(descriptorReference.get() == null)
			{
				throw new NullPointerException();
			}

			if (position + s.length() < stackEndAbs) {
				int length = s.length();
				for (int i = 0; i < length; i++) {
					writeByte(s.charAt(i));
				}
				position += length;
			} else {
				throw new EOFException();
			}
		}

		@Override
		public void writeChar(int v) throws IOException {
			if(descriptorReference.get() == null)
			{
				throw new NullPointerException();
			}

			if (position + 1 < stackEndAbs) {
				descriptor.memory[position++] = (byte) ((v >> 8) & 0xff);
				descriptor.memory[position++] = (byte) ((v >> 0) & 0xff);
			} else {
				throw new EOFException();
			}
		}

		@Override
		public void writeChars(String s) throws IOException {
			if(descriptorReference.get() == null)
			{
				throw new NullPointerException();
			}

			if (position + 2 * s.length() < stackEndAbs) {
				int length = s.length();
				for (int i = 0; i < length; i++) {
					writeChar(s.charAt(i));
				}
			} else {
				throw new EOFException();
			}

		}

		@Override
		public void writeDouble(double v) throws IOException {
			writeLong(Double.doubleToLongBits(v));
		}

		@Override
		public void writeFloat(float v) throws IOException {
			writeInt(Float.floatToIntBits(v));
		}

		@Override
		public void writeInt(int v) throws IOException {
			if(descriptorReference.get() == null)
			{
				throw new NullPointerException();
			}

			if (position + 3 < stackEndAbs) {
				descriptor.memory[position++] = (byte) ((v >> 24) & 0xff);
				descriptor.memory[position++] = (byte) ((v >> 16) & 0xff);
				descriptor.memory[position++] = (byte) ((v >> 8) & 0xff);
				descriptor.memory[position++] = (byte) ((v >> 0) & 0xff);
			} else {
				throw new EOFException();
			}
		}

		@Override
		public void writeLong(long v) throws IOException {
			if(descriptorReference.get() == null)
			{
				throw new NullPointerException();
			}

			if (position + 7 < stackEndAbs) {
				descriptor.memory[position++] = (byte) ((v >> 56) & 0xff);
				descriptor.memory[position++] = (byte) ((v >> 48) & 0xff);
				descriptor.memory[position++] = (byte) ((v >> 40) & 0xff);
				descriptor.memory[position++] = (byte) ((v >> 32) & 0xff);
				descriptor.memory[position++] = (byte) ((v >> 24) & 0xff);
				descriptor.memory[position++] = (byte) ((v >> 16) & 0xff);
				descriptor.memory[position++] = (byte) ((v >> 8) & 0xff);
				descriptor.memory[position++] = (byte) ((v >> 0) & 0xff);
			} else {
				throw new EOFException();
			}
		}

		@Override
		public void writeShort(int v) throws IOException {
			if(descriptorReference.get() == null)
			{
				throw new NullPointerException();
			}

			if (position + 1 < stackEndAbs) {
				descriptor.memory[position++] = (byte) ((v >>> 8) & 0xff);
				descriptor.memory[position++] = (byte) ((v >>> 0) & 0xff);
			} else {
				throw new EOFException();
			}
		}

		@Override
		public void writeUTF(String str) throws IOException {
			int strlen = str.length();
			int utflen = 0;
			int c, count = 0;

			/* use charAt instead of copying String to char array */
			for (int i = 0; i < strlen; i++) {
				c = str.charAt(i);
				if ((c >= 0x0001) && (c <= 0x007F)) {
					utflen++;
				} else if (c > 0x07FF) {
					utflen += 3;
				} else {
					utflen += 2;
				}
			}

			if (utflen > 65535)
				throw new UTFDataFormatException("encoded string too long: " + utflen + " memory");

			byte[] bytearr = new byte[utflen + 2];

			bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
			bytearr[count++] = (byte) ((utflen >>> 0) & 0xFF);

			int i = 0;
			for (i = 0; i < strlen; i++) {
				c = str.charAt(i);
				if (!((c >= 0x0001) && (c <= 0x007F)))
					break;
				bytearr[count++] = (byte) c;
			}

			for (; i < strlen; i++) {
				c = str.charAt(i);
				if ((c >= 0x0001) && (c <= 0x007F)) {
					bytearr[count++] = (byte) c;

				} else if (c > 0x07FF) {
					bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
					bytearr[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
					bytearr[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
				} else {
					bytearr[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
					bytearr[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
				}
			}

			write(bytearr, 0, utflen + 2);
		}
	}

	/**
	 * Position in the underlying memory.
	 */
	protected int position;
	
	// ------------------------------------------------------------------------
	// Buffer management
	// ------------------------------------------------------------------------
	
	private HeapStackDataOutputView outputView;

	// ------------------------------------------------------------------------
	// Serialization / Deserialization
	// ------------------------------------------------------------------------

	private final MemoryIOWrapper memoryWrapper;

	private final RawComparator comparator;

	private final SerializationFactory<K> keySerialization;

	private final SerializationFactory<V> valSerialization;

	private final Serializer<K> keySerializer;

	private final Serializer<V> valSerializer;

	private final Deserializer<K> keyDeserializer;

	private final Deserializer<V> valDeserializer;

	// ------------------------------------------------------------------------
	// Key/Value accounting
	// ------------------------------------------------------------------------

	private static final int KEY_LEN = 4,  VAL_LEN = 4; // integers
	
	private static final int KEY_START = 0; // key offset in acct

	private static final int VAL_START = 4; // value offset in acct

	private static final int INDEX_ENTRY_SIZE = KEY_LEN + VAL_LEN; // acct bytes per record
	
	private static final int OFFSET_ENTRY_SIZE = 4; // bytes per offset
	
	private static final int STACK_ENTRY_SIZE = INDEX_ENTRY_SIZE + OFFSET_ENTRY_SIZE;
	
	private int pairsCount = 0;

	// -------------------------------------------------------------------------
	// Constructors / Destructors
	// -------------------------------------------------------------------------

	public BufferSortableGuaranteed(MemorySegment memory, RawComparator comparator, SerializationFactory<K> keySerialization,
			SerializationFactory<V> valSerialization) {
		super();

		// serialization
		this.comparator = comparator;
		this.keySerialization = keySerialization;
		this.valSerialization = valSerialization;
		this.keySerializer = keySerialization.getSerializer();
		this.valSerializer = valSerialization.getSerializer();
		this.keyDeserializer = keySerialization.getDeserializer();
		this.valDeserializer = valSerialization.getDeserializer();

		// bind memory segment
		bind(memory);
		this.memoryWrapper = new MemoryIOWrapper(memory);
	}

	// -------------------------------------------------------------------------
	// Memory Segment
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.nephele.services.memorymanager.MemoryBacked#bind(eu.stratosphere.nephele.services.memorymanager.
	 * MemorySegment)
	 */
	@Override
	public boolean bind(MemorySegment memory) {
		if (super.bind(memory)) {
			
			// inject proxy
			if(memory instanceof DefaultMemorySegment == false)
			{
				throw new UnsupportedOperationException();
			}
			DefaultMemorySegment segment = (DefaultMemorySegment) memory;
			DefaultMemoryManager.MemorySegmentDescriptorProxy descriptor = new DefaultMemoryManager.MemorySegmentDescriptorProxy(segment.descriptorReference.get());
			outputView = new HeapStackDataOutputView(descriptor);

			// reset counters
			reset();
			return true;
		} else {
			return false;
		}
	}

	public void reset() {
		try {
			// memory segment
			outputView.reset();

			// buffer
			position = 0;
			pairsCount = 0;

			// serialization
			keySerializer.open(outputView);
			valSerializer.open(outputView);

			// accounting
			outputView.resetStackHeap();
		} catch (IOException iex) {
			throw new RuntimeException(iex);
		}
	}

	// -------------------------------------------------------------------------
	// Buffering
	// -------------------------------------------------------------------------

	protected boolean isEmpty() {
		return outputView.getPosition() == 0;
	}

	public int getPosition() {
		return position;
	}

	// -------------------------------------------------------------------------
	// Retrieving and Writing
	// -------------------------------------------------------------------------

	/**
	 * Gets the key at the specified position.
	 * 
	 * @param position
	 *        The position of the value.
	 * @return The key.
	 * @throws IOException
	 *         Thrown, if the deserialization causes an exception.
	 * @throws ArrayIndexOutOfBoundsException
	 *         If the position is negative or if it is larger or equal to
	 *         the number of key/value pairs in the buffer.
	 */
	public K getKey(int position) throws IOException {
		keyDeserializer.open(memory.inputView);
		
		int offset = readOffset(position);
		int keyStart = readIndexKeyAbs(offset);

		K key = keySerialization.newInstance();

		memory.inputView.setPosition(keyStart);
		keyDeserializer.deserialize(key);

		return key;
	}

	/**
	 * Gets the value at the specified position.
	 * 
	 * @param position
	 *        The position of the value.
	 * @return The value.
	 * @throws IOException
	 *         Thrown, if the deserialization causes an exception.
	 * @throws ArrayIndexOutOfBoundsException
	 *         If the position is negative or if it is larger or equal to
	 *         the number of key/value pairs in the buffer.
	 */
	public V getValue(int position) throws IOException {
		valDeserializer.open(memory.inputView);

		int offset = readOffset(position);
		int valStart = readIndexValueAbs(offset);

		V val = valSerialization.newInstance();

		memory.inputView.setPosition(valStart);
		valDeserializer.deserialize(val);

		return val;
	}

	/**
	 * Writes the provided key/value pair into the underlying memory segment.
	 * 
	 * @param pair
	 *        The key/value pair to be written.
	 * @throws IOException
	 * @throws UnboundMemoryBackedException
	 */
	public boolean write(Pair<K, V> pair) {
		try {
			/* 1. WRITE PAIR */
			// reserve 4 bytes for length
			outputView.skip(4);

			// serialize key bytes into buffer
			final int keystart = outputView.getPosition();
			keySerializer.serialize(pair.getKey());

			// serialize value bytes into buffer
			final int valstart = outputView.getPosition();
			valSerializer.serialize(pair.getValue());
			
			// serialize object length
			memory.randomAccessView.putInt(position, outputView.getPosition() - position - 4);
			
			/* 2. INSERT INDEX AND OFFSET */
			// a. precheck heap size
			final int free = outputView.getStackEndRel() - outputView.getHeapEndRel();
			if(free < STACK_ENTRY_SIZE)
			{
				return false;
			}
			
			// b. write index (pointer into segment)
			outputView.growStack(INDEX_ENTRY_SIZE); // 8
			// TODO replace with direct access to outputView.descriptor.memory (en)
			memory.randomAccessView.putInt(outputView.getStackEndRel() + KEY_START, keystart);
			memory.randomAccessView.putInt(outputView.getStackEndRel() + VAL_START, valstart);

			// c. write offset (pointer to index)
			final int offset = outputView.getStackEndRel();
			outputView.growStack(OFFSET_ENTRY_SIZE); // 4
			memory.randomAccessView.putInt(outputView.getStackEndRel(), offset);

			/* 3. UPDATE WRITE POSITION */
			pairsCount++;
			position = outputView.getPosition();
			
			return true;
		} catch (IOException e) {
			return false;
		}
	}
	
	private final int readIndexKeyAbs(int absPosition)
	{
		return memory.randomAccessView.getInt(absPosition);
	}
	
	private final int readIndexValueAbs(int absPosition)
	{
		return memory.randomAccessView.getInt(absPosition + KEY_LEN);
	}
	
	private final void writeOffset(int position, int offset)
	{
		final int stackoffset = position * (INDEX_ENTRY_SIZE + OFFSET_ENTRY_SIZE) + INDEX_ENTRY_SIZE + OFFSET_ENTRY_SIZE;
		final int memoryoffset = outputView.descriptor.size - stackoffset;
		memory.randomAccessView.putInt(memoryoffset, offset);
	}
	
	private final int readOffset(int position)
	{
		final int stackoffset = position * (INDEX_ENTRY_SIZE + OFFSET_ENTRY_SIZE) + INDEX_ENTRY_SIZE + OFFSET_ENTRY_SIZE;
		final int memoryoffset = outputView.descriptor.size - stackoffset;
		return memory.randomAccessView.getInt(memoryoffset);
	}

	/**
	 * Writes this buffer completely to the given writer.
	 * 
	 * @param writer
	 *        The writer to write the segment to.
	 * @throws IOException
	 *         Thrown, if the writer caused an I/O exception.
	 */
	public void writeToChannel(Writer writer) throws IOException {
		if (!isBound()) {
			new UnboundMemoryBackedException();
		}

		final MemoryIOWrapper memoryWrapper = new MemoryIOWrapper(memory);

		// write according to index
		for (int i = 0; i < size(); i++) {
			// offset to index element
			int offset = readOffset(i);

			// start and end within memory segment
			int kvstart = readIndexKeyAbs(offset);
			int kvend = 0;
			
			// for the last pair there is no next pair
			if(offset - STACK_ENTRY_SIZE > outputView.getStackEndRel())
			{	
				// -> kvend = kvstart of next pair
				// -> 4 = kv-length -> see write(...)
				kvend = readIndexKeyAbs(offset-STACK_ENTRY_SIZE) - 4;
			}
			else
			{
				kvend = position;
			}

			// length of serialized pair
			final int kvlength = kvend - kvstart;

			// set offset within memory segment
			memoryWrapper.setIOBlock(kvstart, kvlength);

			// copy serialized pair to writer
			writer.write(memoryWrapper);
		}
	}

	/**
	 * Writes a series of key/value pairs in this buffer to the given writer.
	 * 
	 * @param writer
	 *        The writer to write the pairs to.
	 * @param start
	 *        The position (logical number) of the first pair that is written.
	 * @param num
	 *        The number of pairs to be written.
	 * @throws IOException
	 *         Thrown, if the writer caused an I/O exception.
	 */
	public void writeToChannel(Writer writer, int start, int num) throws IOException {
		// write according to index
		for (int i = start; i < start + num; i++) {
			// offset to index element
			int offset = readOffset(i);

			// start and end within memory segment
			int kvstart = readIndexKeyAbs(offset);
			int kvend = 0;
			
			// for the last pair there is no next pair
			if(offset - STACK_ENTRY_SIZE > outputView.getStackEndRel())
			{	
				// -> kvend = kvstart of next pair
				// -> 4 = kv-length -> see write(...)
				kvend = readIndexKeyAbs(offset-STACK_ENTRY_SIZE) - 4;
			}
			else
			{
				kvend = position;
			}

			// length of serialized pair
			final int kvlength = kvend - kvstart;

			// set offset within memory segment
			memoryWrapper.setIOBlock(kvstart, kvlength);

			// copy serialized pair to writer
			writer.write(memoryWrapper);
		}
	}

	// -------------------------------------------------------------------------
	// Indexed Sorting
	// -------------------------------------------------------------------------

	@Override
	public int compare(int i, int j) {
		// offsets into index
		final int offseti = readOffset(i);
		final int offsetj = readOffset(j);
		
		// key i
		final int indexi = readIndexKeyAbs(offseti);
		final int lengthi = readIndexValueAbs(offseti) - indexi;
		
		byte[] keyi = new byte[lengthi];
		memory.randomAccessView.get(indexi, keyi);
		
		// key j
		final int indexj = readIndexKeyAbs(offsetj);
		final int lengthj = readIndexValueAbs(offsetj) - indexj;

		byte[] keyj = new byte[lengthj];
		memory.randomAccessView.get(indexj, keyj);

		// sort by key
		return comparator.compare(keyi, keyj, 0, 0, keyi.length, keyj.length);
	}

	@Override
	public void swap(int i, int j) {
		int offseti = readOffset(i);
		int offsetj = readOffset(j);
		writeOffset(i, offsetj);
		writeOffset(j, offseti);
	}

	@Override
	public int size() {
		return pairsCount;
	}

	public Iterator<KeyValuePair<K, V>> getIterator() {

		return new Iterator<KeyValuePair<K, V>>() {

			int current = 0;

			@Override
			public boolean hasNext() {
				if (current < size()) {
					return true;
				} else {
					return false;
				}
			}

			@Override
			public KeyValuePair<K, V> next() {
				try {
					keyDeserializer.open(memory.inputView);
					valDeserializer.open(memory.inputView);

					int index = readOffset(current);
					int keyStart = readIndexKeyAbs(index);
					int valStart = readIndexValueAbs(index);

					K key = keySerialization.newInstance();
					V val = valSerialization.newInstance();

					memory.inputView.setPosition(keyStart);
					keyDeserializer.deserialize(key);

					memory.inputView.setPosition(valStart);
					valDeserializer.deserialize(val);

					current++;

					return new KeyValuePair<K, V>(key, val);

				} catch (IOException ioe) {
					throw new RuntimeException(ioe);
				}
			}

			@Override
			public void remove() {

			}
		};
	}
}
