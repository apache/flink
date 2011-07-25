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

package eu.stratosphere.pact.common.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;


/**
 * The Pact Record is the basic data record that flows between functions in a Pact program. The record is a tuple of
 * arbitrary values.
 * 
 * <p>
 * The Pact Record implements a sparse tuple model, meaning that the record can contain many fields which are actually
 * null and not represented in the record. It has internally a bitmap marking which fields are set and which are not.
 * 
 * This class is NOT thread-safe!
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class PactRecord implements Value
{
	private static final int NULL_INDICATOR_OFFSET = Integer.MIN_VALUE;			// value marking a field as null
	
	
	private byte[] binaryData;
	
	private int binaryLen;
	
	private int numFields;
	
	private int[] offsets;
	
	private int[] lengths;
	
	private Value[] fields;
	
	private int firstModifiedPos = -1;
	
	private int lastUnmodifiedPos = -1;
	
	private InternalSerializer serializer;
	
	
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Required nullary constructor for instantiation by serialization logic.
	 */
	public PactRecord()
	{}
	
	/**
	 * Creates a new record containing only a single field, which is the given value.
	 * 
	 * @param value The value for the single field of the record.
	 */
	public PactRecord(Value value)
	{
		setField(0, value);
	}
	
	/**
	 * Creates a new record containing exactly to fields, which are the given values.
	 * 
	 * @param val1 The value for the first field.
	 * @param val2 The value for the second field.
	 */
	public PactRecord(Value val1, Value val2)
	{
		setNumFields(2);
		setField(0, val1);
		setField(1, val2);
	}
	
	/**
	 * Creates a new pact record, containing the given number of fields. The fields are initially all nulls.
	 *  
	 * @param numFields The number of fields for the record.
	 */
	public PactRecord(int numFields)
	{
		setNumFields(numFields);
	}

	// --------------------------------------------------------------------------------------------
	//                             Basic Accessors
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the number of fields currently in the record. This also includes null fields.
	 *  
	 * @return The number of fields in the record.
	 */
	public int getNumFields()
	{
		return this.numFields;
	}
	
	/**
	 * Sets the number of fields in the record. If the new number of fields is longer than the current number of
	 * fields, then null fields are appended. If the new number of fields is smaller than the current number of
	 * fields, then the last fields are truncated.
	 * 
	 * @param numFields The new number of fields.
	 */
	public void setNumFields(final int numFields)
	{
		final int oldNumFields = this.numFields;
		
		// check whether we increase or decrease the fields 
		if (numFields > oldNumFields)
		{
			makeSpace(numFields);
			for (int i = oldNumFields; i < numFields; i++) {
				this.offsets[i] = NULL_INDICATOR_OFFSET;
			}
		}
		else {
			// decrease the number of fields
			// all we have to do is mark the last unmodified
			// we do not remove the values from the cache, as the objects (if they are there) will most likely
			// be reused when the record is re-filled
			lastUnmodified(numFields - 1);
		}
		
		this.numFields = numFields;
	}
	
	/**
	 * Reserves space for at least the given number of fields in the internal arrays.
	 * 
	 * @param numFields The number of fields to reserve space for.
	 */
	public void makeSpace(int numFields)
	{
		final int oldNumFields = this.numFields;
		
		// increase the number of fields.
		if (this.offsets == null) {
			this.offsets = new int[numFields];
		}
		else if (this.offsets.length < numFields) {
			int[] newOffs = new int[Math.max(numFields, oldNumFields << 1)];
			System.arraycopy(this.offsets, 0, newOffs, 0, oldNumFields);
			this.offsets = newOffs;
		}
		
		if (this.fields == null) {
			this.fields = new Value[numFields];
		}
		else if (this.fields.length < numFields) {
			Value[] newFields = new Value[Math.max(numFields, oldNumFields << 1)];
			System.arraycopy(this.fields, 0, newFields, 0, oldNumFields);
			this.fields = newFields;
		}
	}
	
	/**
	 * Gets the field at the given position from the record. This method checks internally, if this instance of
	 * the record has previously returned a value for this field. If so, it reuses the object, if not, it
	 * creates one from the supplied class.
	 *  
	 * @param <T> The type of the field.
	 * 
	 * @param fieldNum The logical position of the field.
	 * @param type The type of the field as a class. This class is used to instantiate a value object, if none had
	 *             previously been instantiated. 
	 * @return The field at the given position, or null, if the field was null.
	 * @throws IndexOutOfBoundsException Thrown, if the field number is negative or larger or equal to the number of
	 *                                   fields in this record.
	 */
	@SuppressWarnings("unchecked")
	public <T extends Value> T getField(final int fieldNum, final Class<T> type)
	{
		// range check
		if (fieldNum < 0 || fieldNum > this.numFields) {
			throw new IndexOutOfBoundsException();
		}
		
		// get offset and check for null
		final int offset = this.offsets[fieldNum];
		if (offset == NULL_INDICATOR_OFFSET) {
			return null;
		}		
		else if (offset <= 0) {
			// value that has been set is new or modified
			return (T) this.fields[fieldNum];
		}
		
		final int limit = fieldNum < this.numFields - 1 ? this.offsets[fieldNum + 1] : this.binaryLen;
		
		// get an instance, either from the instance cache or create a new one 
		final T field;
		if (this.fields[fieldNum] != null) {
			field = (T) this.fields[fieldNum];
		}
		else {
			field = createInstance(type);
		}
		
		// deserialize
		deserialize(field, offset, limit);
		return field;
	}
	
	/**
	 * Gets the field at the given position. If the field at that position is null, then this method leaves
	 * the target field unchanged and returns false.
	 * 
	 * @param fieldNum The position of the field.
	 * @param target The value to deserialize the field into.
	 * @return True, if the field was deserialized properly, false, if the field was null.
	 */
	public boolean getField(int fieldNum, Value target)
	{
		// range check
		if (fieldNum < 0 || fieldNum > this.numFields) {
			throw new IndexOutOfBoundsException();
		}
		
		// get offset and check for null
		final int offset = this.offsets[fieldNum];
		if (offset == NULL_INDICATOR_OFFSET) {
			return false;
		}
		else if (offset <= 0) {
			if (this.fields[fieldNum] == null) {
				return false;
			}
			else {
				// value that has been set is new or modified
				// bring the binary in sync so that the deserialization gives the correct result
				syncBinaryState();
			}
		}
		
		final int limit = fieldNum < this.numFields - 1 ? this.offsets[fieldNum + 1] : this.binaryLen;
		
		deserialize(target, offset, limit);
		return true;
	}
	
	public boolean getFields(int[] positions, Value[] targets)
	{
		for (int i = 0; i < positions.length; i++) {
			if (!getField(positions[i], targets[i])) {
				return false;
			}
		}
		return true;
	}
	
	
	private final <T extends Value> T deserialize(T target, int offset, int limit)
	{
		return null;
	}
	
	
	
	/**
	 * Sets the field at the given position to the given value. If the field position is larger or equal than
	 * the current number of fields in the record, than the record is expanded to host as many columns.
	 * <p>
	 * The value is kept as a reference in the record until the binary representation is synchronized. Until that
	 * point, all modifications to the value's object will change the value inside the record. 
	 * <p>
	 * The binary representation is synchronized the latest when the record is emitted. It may be triggered 
	 * manually at an earlier point, but it is generally not necessary and advisable. Because the synchronization
	 * triggers the serialization on all modified values, it may be an expensive operation. 
	 * 
	 * @param fieldNum The position of the field, starting at zero.
	 * @param value The new value.
	 */
	public void setField(int fieldNum, Value value)
	{
		// range check
		if (fieldNum < 0) {
			throw new IndexOutOfBoundsException();
		}
		// if the field number is beyond the size, the tuple is expanded
		if (fieldNum >= this.numFields) {
			setNumFields(fieldNum + 1);
		}
		internallySetField(fieldNum, value);
	}
	
	public void addField(Value value)
	{
		int pos = this.numFields;
		setNumFields(pos + 1);
		internallySetField(pos, value);
	}
	
	public void insertField(int position, Value value)
	{
		throw new UnsupportedOperationException();
	}
	
	private final void internallySetField(int fieldNum, Value value)
	{
		// check if we modify an existing field
		final int oldOffset = this.offsets[fieldNum];
		if (oldOffset > 0) {
			this.offsets[fieldNum] = -oldOffset;
		}
		else if (oldOffset == NULL_INDICATOR_OFFSET) {
			this.offsets[fieldNum] = 0;
		}
		
		this.fields[fieldNum] = value;
		
	}
	
	private final void lastUnmodified(int field)
	{
		if (this.lastUnmodifiedPos > field) {
			this.lastUnmodifiedPos = field;
		}
	}
	
	private final void firstModified(int field)
	{
		if (this.firstModifiedPos > field) {
			this.firstModifiedPos = field;
		}
	}
	
	
	
	
	
	
	public void removeField(int field) {
		throw new UnsupportedOperationException();
	}
	
	
	
	
	public void project(long mask) {
		throw new UnsupportedOperationException();
	}
	
	public void project(long[] mask) {
		throw new UnsupportedOperationException();
	}
	
	public void setNull(int field) {
		throw new UnsupportedOperationException();
	}
	
	public void setNull(long fields) {
		throw new UnsupportedOperationException();
	}
	
	public void setNull(long[] fields) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Clears the record. After this operation, the pact record will have zero fields.
	 */
	public void clear()
	{
		// reset field counter. other members are kept to reuse them
		this.numFields = 0;
		
		// make binary state consistent
		this.binaryLen = 1;
		this.binaryData[0] = 0;
	}
	
	public void unionFields(PactRecord other)
	{
	}
	
	public void copyTo(PactRecord target)
	{
		
	}
	
	public PactRecord createCopy()
	{
		final PactRecord rec = new PactRecord();
		copyTo(rec);
		return rec;
	}

	// --------------------------------------------------------------------------------------------
	
	public final boolean equalsFields(int[] positions, Value[] searchValues, Value[] deserializationHolders)
	{
		for (int i = 0; i < positions.length; i++) {
			getField(positions[i], deserializationHolders[i]);
			if (deserializationHolders[i].equals(searchValues[i])) {
				return false;
			}
		}
		return true;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public void syncBinaryState()
	{
		final int firstModified = this.firstModifiedPos;
		
		if (firstModified < 0) {
			return;
		}
		
		final int numFields = this.numFields;
		final int[] offsets = this.offsets;
	
		if (this.serializer == null) {
			this.serializer = new InternalSerializer();
		}
		final InternalSerializer serializer = this.serializer;
		
		if (this.lastUnmodifiedPos < firstModified) {
			// all changes go after what we keep
			int offset = this.offsets[this.lastUnmodifiedPos] + this.lengths[this.lastUnmodifiedPos];
			
			serializer.memory = this.binaryData;
			serializer.position = this.offsets[this.lastUnmodifiedPos] + this.lengths[this.lastUnmodifiedPos];
			
			try {
				for (int i = firstModified; i < numFields; i++) {
					offsets[i] = offset;
					
					this.fields[i].write(serializer);
					
					int newOffset = serializer.position;
					this.lengths[i] = newOffset - offset;
					offset = newOffset;
				}
			}
			catch (Exception e) {
				throw new RuntimeException("Error in data type serialization: " + e.getMessage()); 
			}	
		}
		else {
			// changed and unchanged fields are interleaved
			throw new UnsupportedOperationException();
		}
		
		this.firstModifiedPos = -1;
		this.lastUnmodifiedPos = numFields - 1;
	}
	
	// --------------------------------------------------------------------------------------------
	//                             Serialization
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException
	{
		// make sure everything is in a valid binary representation
		syncBinaryState();
		
		// write the length first, variably encoded, then the contents of the binary array
		writeVarLengthInt(out, this.binaryLen);
		out.write(this.binaryData, 0, this.binaryLen);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException
	{
		final int len = readValLengthInt(in);
		this.binaryLen = len;
			
		// ensure out byte array is large enough
		byte[] data = this.binaryData;
		if (data == null || data.length < len) {
			data = new byte[len];
			this.binaryData = data;
		}
		
		// read the binary data
		in.readFully(data, 0, len);
		int offset = 1;
		
		// read number of fields, variable length encoded
		int numFields = data[0];
		if (numFields >= MAX_BIT) {
			int shift = 7;
			int curr;
			numFields = numFields & 0x7f;
			while ((curr = data[offset++]) >= MAX_BIT) {
				numFields |= (curr & 0x7f) << shift;
				shift += 7;
			}
			numFields |= curr << shift;
		}
		this.numFields = numFields;
		
		// ensure that all arrays are there and of sufficient size
		if (this.offsets == null || this.offsets.length < numFields) {
			this.offsets = new int[numFields];
		}
		if (this.fields == null || this.fields.length < numFields) {
			this.fields = new Value[numFields];
		}
		
		final int beginMasks = offset; // beginning of bitmap for null fields
		final int fieldsBy8 = numFields >> 3;
		
		offset = beginMasks + fieldsBy8; 
		
		for (int field = 0, chunk = 0; chunk < fieldsBy8; chunk++) {
			int mask = data[beginMasks + chunk];
			for (int i = 0; i < 8 && field < numFields; i++, field++) {
				if ((mask & 0x1) == 0x1) {
					// not null, so read the offset value
					// offset value is variable length encoded
					int start = data[offset];
					if (start >= MAX_BIT) {
						int shift = 7;
						int curr;
						start = start & 0x7f;
						while ((curr = data[offset++]) >= MAX_BIT) {
							start |= (curr & 0x7f) << shift;
							shift += 7;
						}
						start |= curr << shift;
					}
					this.offsets[field] = start;
				}
				else {
					// field is null
					this.offsets[field] = NULL_INDICATOR_OFFSET;
				}
				mask >>= 1;
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                                     Utilities
	// --------------------------------------------------------------------------------------------
	
	private static final void writeVarLengthInt(DataOutput out, int value) throws IOException
	{
		while (value >= MAX_BIT) {
			out.write(value | MAX_BIT);
			value >>= 7;
		}
		out.write(value);
	}
	
	private static final int readValLengthInt(DataInput in) throws IOException
	{
		// read first byte
		int val = in.readUnsignedByte();
		if (val >= MAX_BIT) {
			int shift = 7;
			int curr;
			val = val & 0x7f;
			while ((curr = in.readUnsignedByte()) >= MAX_BIT) {
				val |= (curr & 0x7f) << shift;
				shift += 7;
			}
			val |= curr << shift;
		}
		return val;
	}
	 
	private static final <T> T createInstance(Class<T> clazz)
	{
		try {
			return clazz.newInstance();
		}
		catch (InstantiationException e) {
			throw new RuntimeException("The given class could not be instantiated. " +
						"The class is either abstract or it misses a nullary constructor."); 
		}
		catch (IllegalAccessException e) {
			throw new RuntimeException("Class or its nullary constructor is not public.");
		}
	}
	
	private static final int MAX_BIT = 0x1 << 7;
	
	
	// ------------------------------------------------------------------------
	//                Utility classes for internal serialization
	// ------------------------------------------------------------------------
	
	private final class InternalSerializer implements DataOutput
	{
		private byte[] memory;
		private int position;

		@Override
		public void write(int b) throws IOException {
//			if (position < this.end) {
//				this.memory[position++] = (byte) (b & 0xff);
//			} else {
//				throw new EOFException();
//			}
		}

		@Override
		public void write(byte[] b) throws IOException {
			write(b, 0, b.length);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
//			if (this.position < this.end && this.position <= this.end - len && off <= b.length - len) {
//				System.arraycopy(b, off, this.memory, position, len);
//				this.position += len;
//			} else {
//				throw new EOFException();
//			}
		}

		@Override
		public void writeBoolean(boolean v) throws IOException {
//			if (this.position < this.end) {
//				this.memory[this.position++] = (byte) (v ? 1 : 0);
//			} else {
//				throw new EOFException();
//			}
		}

		@Override
		public void writeByte(int v) throws IOException {
			write(v);
		}

		@Override
		public void writeBytes(String s) throws IOException {
//			final int sLen = s.length();
//			
//			if (this.position < this.end - sLen) {
//				for (int i = 0; i < sLen; i++) {
//					writeByte(s.charAt(i));
//				}
//				this.position += sLen;
//			} else {
//				throw new EOFException();
//			}
		}

		@Override
		public void writeChar(int v) throws IOException {
//			if (position < this.end - 1) {
//				this.memory[position++] = (byte) ((v >> 8) & 0xff);
//				this.memory[position++] = (byte) ((v >> 0) & 0xff);
//			} else {
//				throw new EOFException();
//			}
		}

		@Override
		public void writeChars(String s) throws IOException {
//			if (position < this.end - (2 * s.length())) {
//				int length = s.length();
//				for (int i = 0; i < length; i++) {
//					writeChar(s.charAt(i));
//				}
//			} else {
//				throw new EOFException();
//			}

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
//			if (position < this.end - 3) {
//				this.memory[position++] = (byte) ((v >> 24) & 0xff);
//				this.memory[position++] = (byte) ((v >> 16) & 0xff);
//				this.memory[position++] = (byte) ((v >> 8) & 0xff);
//				this.memory[position++] = (byte) ((v >> 0) & 0xff);
//			} else {
//				throw new EOFException();
//			}
		}

		@Override
		public void writeLong(long v) throws IOException {
//			if (position < this.end - 7) {
//				this.memory[position++] = (byte) ((v >> 56) & 0xff);
//				this.memory[position++] = (byte) ((v >> 48) & 0xff);
//				this.memory[position++] = (byte) ((v >> 40) & 0xff);
//				this.memory[position++] = (byte) ((v >> 32) & 0xff);
//				this.memory[position++] = (byte) ((v >> 24) & 0xff);
//				this.memory[position++] = (byte) ((v >> 16) & 0xff);
//				this.memory[position++] = (byte) ((v >> 8) & 0xff);
//				this.memory[position++] = (byte) ((v >> 0) & 0xff);
//			} else {
//				throw new EOFException();
//			}
		}

		@Override
		public void writeShort(int v) throws IOException {
//			if (position < this.end - 1) {
//				this.memory[position++] = (byte) ((v >>> 8) & 0xff);
//				this.memory[position++] = (byte) ((v >>> 0) & 0xff);
//			} else {
//				throw new EOFException();
//			}
		}

		@Override
		public void writeUTF(String str) throws IOException {
//			int strlen = str.length();
//			int utflen = 0;
//			int c, count = 0;
//
//			/* use charAt instead of copying String to char array */
//			for (int i = 0; i < strlen; i++) {
//				c = str.charAt(i);
//				if ((c >= 0x0001) && (c <= 0x007F)) {
//					utflen++;
//				} else if (c > 0x07FF) {
//					utflen += 3;
//				} else {
//					utflen += 2;
//				}
//			}
//
//			if (utflen > 65535)
//				throw new UTFDataFormatException("encoded string too long: " + utflen + " memory");
//
//			byte[] bytearr = new byte[utflen + 2];
//
//			bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
//			bytearr[count++] = (byte) ((utflen >>> 0) & 0xFF);
//
//			int i = 0;
//			for (i = 0; i < strlen; i++) {
//				c = str.charAt(i);
//				if (!((c >= 0x0001) && (c <= 0x007F)))
//					break;
//				bytearr[count++] = (byte) c;
//			}
//
//			for (; i < strlen; i++) {
//				c = str.charAt(i);
//				if ((c >= 0x0001) && (c <= 0x007F)) {
//					bytearr[count++] = (byte) c;
//
//				} else if (c > 0x07FF) {
//					bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
//					bytearr[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
//					bytearr[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
//				} else {
//					bytearr[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
//					bytearr[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
//				}
//			}
//
//			write(bytearr, 0, utflen + 2);
		}
	};
	
}
