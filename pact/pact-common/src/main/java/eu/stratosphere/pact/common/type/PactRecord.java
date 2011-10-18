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
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.util.InstantiationUtil;



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
	
	private static final int MODIFIED_INDICATOR_OFFSET = Integer.MIN_VALUE + 1;	// value marking field as modified
	
	private static final int DEFAULT_FIELD_LEN = 8;								// length estimate for bin array
	
	// --------------------------------------------------------------------------------------------
	
	private byte[] binaryData;			// the buffer containing the binary representation
	
	private int binaryLen;				// the length of the contents in the binary buffer that is valid
	
	private int numFields;				// the number of fields in the record
	
	private int[] offsets;				// the offsets to the binary representations of the fields
	
	private int[] lengths;				// the lengths of the fields
	
	private Value[] fields;				// the cache for objects into which the binary representations are read 
	
	private int firstModifiedPos = Integer.MAX_VALUE;	// position of the first modification (since (de)serialization)
	
	private int lastUnmodifiedPos = -1;					// position of the latest unmodified field
	
	private InternalDeSerializer serializer;			// provides DataInpout and DataOutput abstraction to fields
	
	private byte[] serializationSwitchBuffer;			// byte array that is switched with binData during ser/deser
	
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
		makeSpace(2);
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
		if (numFields > oldNumFields) {
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
			if (this.lastUnmodifiedPos >= numFields)
				this.lastUnmodifiedPos = numFields - 1;
			markModified(numFields);
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
		// increase the number of fields in the arrays
		if (this.offsets == null) {
			this.offsets = new int[numFields];
		}
		else if (this.offsets.length < numFields) {
			int[] newOffs = new int[Math.max(numFields + 1, oldNumFields << 1)];
			System.arraycopy(this.offsets, 0, newOffs, 0, oldNumFields);
			this.offsets = newOffs;
		}
		
		if (this.lengths == null) {
			this.lengths = new int[numFields];
		}
		else if (this.lengths.length < numFields) {
			int[] newLens = new int[Math.max(numFields + 1, oldNumFields << 1)];
			System.arraycopy(this.lengths, 0, newLens, 0, oldNumFields);
			this.lengths = newLens;
		}
		
		if (this.fields == null) {
			this.fields = new Value[numFields];
		}
		else if (this.fields.length < numFields) {
			Value[] newFields = new Value[Math.max(numFields + 1, oldNumFields << 1)];
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
		if (fieldNum < 0 || fieldNum >= this.numFields) {
			throw new IndexOutOfBoundsException();
		}
		
		// get offset and check for null
		final int offset = this.offsets[fieldNum];
		if (offset == NULL_INDICATOR_OFFSET) {
			return null;
		}		
		else if (offset == MODIFIED_INDICATOR_OFFSET) {
			// value that has been set is new or modified
			return (T) this.fields[fieldNum];
		}
		
		final int limit = offset + this.lengths[fieldNum];
		
		// get an instance, either from the instance cache or create a new one
		final Value oldField = this.fields[fieldNum]; 
		final T field;
		if (oldField != null && oldField.getClass() == type) {
			field = (T) this.fields[fieldNum];
		}
		else {
			field = InstantiationUtil.instantiate(type, Value.class);
			this.fields[fieldNum] = field;
		}
		
		// deserialize
		deserialize(field, offset, limit);
		return field;
	}
	
	/**
	 * Gets the field at the given position. The method tries to deserialize the fields into the given target value.
	 * If the fields has been changed since the last (de)serialization, or is null, them the target value is left
	 * unchanged and the changed value (or null) is returned.   
	 * <p>
	 * In all cases, the returned value contains the correct data (or is correctly null).
	 * 
	 * @param fieldNum The position of the field.
	 * @param target The value to deserialize the field into.
	 * 
	 * @return The value with the contents of the requested field, or null, if the field is null.
	 */
	@SuppressWarnings("unchecked")
	public <T extends Value> T getField(int fieldNum, T target)
	{
		// range check
		if (fieldNum < 0 || fieldNum >= this.numFields) {
			throw new IndexOutOfBoundsException();
		}
		
		// get offset and check for null
		int offset = this.offsets[fieldNum];
		if (offset == NULL_INDICATOR_OFFSET) {
			return null;
		}
		else if (offset == MODIFIED_INDICATOR_OFFSET) {
			// value that has been set is new or modified
			// bring the binary in sync so that the deserialization gives the correct result
			return (T) this.fields[fieldNum];
		}
		
		final int limit = offset + this.lengths[fieldNum];
		deserialize(target, offset, limit);
		return target;
	}
	
	/**
	 * Gets the field at the given position. If the field at that position is null, then this method leaves
	 * the target field unchanged and returns false.
	 * 
	 * @param fieldNum The position of the field.
	 * @param target The value to deserialize the field into.
	 * @return True, if the field was deserialized properly, false, if the field was null.
	 */
	public boolean getFieldInto(int fieldNum, Value target)
	{
		// range check
		if (fieldNum < 0 || fieldNum >= this.numFields) {
			throw new IndexOutOfBoundsException();
		}
		
		// get offset and check for null
		int offset = this.offsets[fieldNum];
		if (offset == NULL_INDICATOR_OFFSET) {
			return false;
		}
		else if (offset == MODIFIED_INDICATOR_OFFSET) {
			// value that has been set is new or modified
			// bring the binary in sync so that the deserialization gives the correct result
			updateBinaryRepresenation();
			offset = offsets[fieldNum];
		}
		
		final int limit = offset + this.lengths[fieldNum];
		deserialize(target, offset, limit);
		return true;
	}
	
	/**
	 * @param positions
	 * @param targets
	 * @return
	 */
	public boolean getFieldsInto(int[] positions, Value[] targets)
	{
		for (int i = 0; i < positions.length; i++) {
			if (!getFieldInto(positions[i], targets[i]))
				return false;
		}
		return true;
	}
	
	/**
	 * Deserializes the given object from the binary string, starting at the given position.
	 * If the deserialization asks for more that <code>limit - offset</code> bytes, than 
	 * an exception is thrown.
	 * 
	 * @param <T> The generic type of the value to be deserialized.
	 * @param target The object to deserialize the data into.
	 * @param offset The offset in the binary string.
	 * @param limit The limit in the binary string.
	 */
	private final <T extends Value> void deserialize(T target, int offset, int limit)
	{
		if (this.serializer == null) {
			this.serializer = new InternalDeSerializer();
		}
		final InternalDeSerializer serializer = this.serializer;
		serializer.memory = this.binaryData;
		serializer.position = offset;
		serializer.end = limit;
		try {
			target.read(serializer);
		}
		catch (Exception e) {
			throw new DeserializationException(e);
		}
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
	
	/**
	 * @param value
	 */
	public void addField(Value value)
	{
		int pos = this.numFields;
		setNumFields(pos + 1);
		internallySetField(pos, value);
	}
	
	/**
	 * @param position
	 * @param value
	 */
	public void insertField(int position, Value value)
	{
		throw new UnsupportedOperationException();
	}
	
	private final void internallySetField(int fieldNum, Value value)
	{
		// check if we modify an existing field
		this.offsets[fieldNum] = value != null ? MODIFIED_INDICATOR_OFFSET : NULL_INDICATOR_OFFSET;
		this.fields[fieldNum] = value;
		markModified(fieldNum);
	}
	
	private final void markModified(int field)
	{
		if (this.firstModifiedPos > field) {
			this.firstModifiedPos = field;
		}
		if (field == this.lastUnmodifiedPos) {
			this.lastUnmodifiedPos--;
		}
	}
	
//	public void removeField(int field) {
//		throw new UnsupportedOperationException();
//	}
//	
//	public void project(long mask) {
//		throw new UnsupportedOperationException();
//	}
//	
//	public void project(long[] mask) {
//		throw new UnsupportedOperationException();
//	}
//	
//	public void setNull(int field) {
//		throw new UnsupportedOperationException();
//	}
//	
//	public void setNull(long fields) {
//		throw new UnsupportedOperationException();
//	}
//	
//	public void setNull(long[] fields) {
//		throw new UnsupportedOperationException();
//	}
	
	/**
	 * Clears the record. After this operation, the record will have zero fields.
	 */
	public void clear()
	{
		this.numFields = 0;
		this.lastUnmodifiedPos = -1;
		this.firstModifiedPos = Integer.MAX_VALUE;
	}
	
	/**
	 * @param other
	 */
	public void unionFields(PactRecord other)
	{
		throw new UnsupportedOperationException();
	}
	
	/**
	 * @param target
	 */
	public void copyToIfModified(PactRecord target)
	{
		copyTo(target);
	}
	
	/**
	 * @param target
	 */
	public void copyTo(PactRecord target)
	{
		updateBinaryRepresenation();
		
		if (target.binaryData == null || target.binaryData.length < this.binaryLen) {
			target.binaryData = new byte[this.binaryLen];
		}
		if (target.offsets == null || target.offsets.length < this.numFields) {
			target.offsets = new int[this.numFields];
		}
		if (target.lengths == null || target.lengths.length < this.numFields) {
			target.lengths = new int[this.numFields];
		}
		if (target.fields == null || target.fields.length < this.numFields) {
			target.fields = new Value[this.numFields];
		}
		
		System.arraycopy(this.binaryData, 0, target.binaryData, 0, this.binaryLen);
		System.arraycopy(this.offsets, 0, target.offsets, 0, this.numFields);
		System.arraycopy(this.lengths, 0, target.lengths, 0, this.numFields);
		
		target.binaryLen = this.binaryLen;
		target.numFields = this.numFields;
		target.firstModifiedPos = Integer.MAX_VALUE;
		target.lastUnmodifiedPos = this.numFields - 1;
	}
	
	/**
	 * Creates an exact copy of this record.
	 * 
	 * @return An exact copy of this record. 
	 */
	public PactRecord createCopy()
	{
		final PactRecord rec = new PactRecord();
		copyTo(rec);
		return rec;
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * @param positions
	 * @param searchValues
	 * @param deserializationHolders
	 * @return
	 */
	public final boolean equalsFields(int[] positions, Value[] searchValues, Value[] deserializationHolders)
	{
		for (int i = 0; i < positions.length; i++) {
			Value v = getField(positions[i], deserializationHolders[i]);
			if (v == null || (!v.equals(searchValues[i]))) {
				return false;
			}
		}
		return true;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Updates the binary representation of the data, such that it reflects the state of the currently
	 * stored fields. If the binary representation is already up to date, nothing happens. Otherwise,
	 * this function triggers the modified fields to serialize themselves into the records buffer and
	 * afterwards updates the offset table.
	 */
	public void updateBinaryRepresenation()
	{
		// check whether the binary state is in sync
		final int firstModified = this.firstModifiedPos;
		final int numFields = this.numFields;
		if (firstModified >= numFields) {
			return;
		}
		
		final int[] offsets = this.offsets;
		if (this.serializer == null) {
			this.serializer = new InternalDeSerializer();
		}
		final InternalDeSerializer serializer = this.serializer;
		
		int offset = firstModified <= 0 ? 0 : this.offsets[firstModified - 1] + this.lengths[firstModified - 1];
		serializer.position = offset;
		
		// for efficiency, we treat the typical pattern that modifications are at the end as a special case
		if (this.lastUnmodifiedPos < firstModified) {
			// all changed fields are after all unchanged fields 
			// serialize the fields first at the beginning
			serializer.memory = this.binaryData == null ? new byte[numFields * DEFAULT_FIELD_LEN] : this.binaryData;	
			try {
				for (int i = firstModified; i < numFields; i++) {
					if (offsets[i] == NULL_INDICATOR_OFFSET)
						continue;
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
			// we serialize into another array
			serializer.memory = this.serializationSwitchBuffer == null ? new byte[numFields * DEFAULT_FIELD_LEN] : this.serializationSwitchBuffer;
			if (offset > 0 & this.binaryData != null) {
				System.arraycopy(this.binaryData, 0, serializer.memory, 0, offset);
			}
			try {
				for (int i = firstModified; i < numFields; i++) {
					final int co = offsets[i];
					if (co == NULL_INDICATOR_OFFSET)
						continue;
					
					offsets[i] = offset;
					if (co == MODIFIED_INDICATOR_OFFSET)
						this.fields[i].write(serializer);
					else
						serializer.write(this.binaryData, co, this.lengths[i]);
					this.lengths[i] = serializer.position - offset;
					offset = serializer.position;
				}
			}
			catch (Exception e) {
				throw new RuntimeException("Error in data type serialization: " + e.getMessage()); 
			}
			
			this.serializationSwitchBuffer = this.binaryData;
			this.binaryData = serializer.memory;
		}
		
		try {
			// now, serialize the lengths, the sparsity mask and the number of fields
			if (numFields <= 8) {
				// efficient handling of common case with less than eight fields
				int mask = 0;
				for (int i = numFields - 1; i > 0; i--) {
					mask <<= 1;
					if (offsets[i] != NULL_INDICATOR_OFFSET) {
						serializer.writeValLenIntBackwards(offsets[i]);
						mask |= 0x1;
					}
				}
				mask <<= 1;
				mask |= (offsets[0] != NULL_INDICATOR_OFFSET) ? 0x1 : 0x0;
				serializer.writeByte(mask);
			}
			else {
				// general case. offsets first (in backward order)
				for (int i = numFields - 1; i > 0; i--) {
					if (offsets[i] != NULL_INDICATOR_OFFSET) {
						serializer.writeValLenIntBackwards(offsets[i]);
					}
				}
				// now the mask. we write it in chucks of 8 bit.
				// the remainder %8 comes first 
				int col = numFields - 1;
				int mask = 0;
				for (int i = numFields & 0x7; i >= 0; i--, col--) {
					mask <<= 1;
					mask |= (offsets[col] != NULL_INDICATOR_OFFSET) ? 0x1 : 0x0;
				}
				serializer.writeByte(mask);
				
				// now the eight-bit chunks
				for (int i = numFields >>> 3; i >= 0; i--) {
					mask = 0;
					for (int k = 0; k < 8; k++, col--) {
						mask <<= 1;
						mask |= (offsets[col] != NULL_INDICATOR_OFFSET) ? 0x1 : 0x0;
					}
					serializer.writeByte(mask);
				}
			}
			serializer.writeValLenIntBackwards(numFields);
		}
		catch (Exception e) {
			throw new RuntimeException("Serialization into binary state failed: " + e.getMessage(), e);
		}
		
		// set the fields
		this.binaryData = serializer.memory;
		this.binaryLen = serializer.position;
		this.firstModifiedPos = Integer.MAX_VALUE;
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
		updateBinaryRepresenation();
		
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
		final int len = readVarLengthInt(in);
		this.binaryLen = len;
			
		// ensure out byte array is large enough
		byte[] data = this.binaryData;
		if (data == null || data.length < len) {
			data = new byte[len];
			this.binaryData = data;
		}
		
		// read the binary data
		in.readFully(data, 0, len);
		initFields(data, 0, len);
	}
	
	private final void initFields(byte[] data, int begin, int len) {
		// read number of fields, variable length encoded reverse at the back
		int pos = begin + len - 2;
		int numFields = data[begin + len - 1] & 0xFF;
		if (numFields >= MAX_BIT) {
			int shift = 7;
			int curr;
			numFields = numFields & 0x7f;
			while ((curr = data[pos--]) >= MAX_BIT) {
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
		if (this.lengths == null || this.lengths.length < numFields) {
			this.lengths = new int[numFields];
		}
		
		final int beginMasks = pos; // beginning of bitmap for null fields
		final int fieldsBy8 = (numFields >>> 3) + ((numFields & 0x7) == 0 ? 0 : 1);
		
		pos = beginMasks - fieldsBy8; 
		int lastNonNullField = -1;
		
		for (int field = 0, chunk = 0; chunk < fieldsBy8; chunk++) {
			int mask = data[beginMasks - chunk];
			for (int i = 0; i < 8 && field < numFields; i++, field++) {
				if ((mask & 0x1) == 0x1) {
					// not null, so read the offset value, if we are not the first non-null fields
					if (lastNonNullField >= 0) {
						// offset value is variable length encoded
						int start = data[pos--];
						if (start >= MAX_BIT) {
							int shift = 7;
							int curr;
							start = start & 0x7f;
							while ((curr = data[pos--]) >= MAX_BIT) {
								start |= (curr & 0x7f) << shift;
								shift += 7;
							}
							start |= curr << shift;
						}
						this.offsets[field] = start + begin;
						this.lengths[lastNonNullField] = start + begin - this.offsets[lastNonNullField];
					}
					else {
						this.offsets[field] = begin;
					}
					lastNonNullField = field;
				}
				else {
					// field is null
					this.offsets[field] = NULL_INDICATOR_OFFSET;
				}
				mask >>= 1;
			}
		}
		if (lastNonNullField >= 0) {
			this.lengths[lastNonNullField] = pos - this.offsets[lastNonNullField] + 1;
		}
		this.firstModifiedPos = Integer.MAX_VALUE;
		this.lastUnmodifiedPos = numFields - 1;
	}
	
	/**
	 * @param fields
	 * @param holders
	 * @param binData
	 * @param offset
	 * @return
	 */
	public boolean readBinary(int[] fields, Value[] holders, byte[] binData, int offset)
	{
		// read the length
		int val = binData[offset++] & 0xff;
		if (val >= MAX_BIT) {
			int shift = 7;
			int curr;
			val = val & 0x7f;
			while ((curr = binData[offset++] & 0xff) >= MAX_BIT) {
				val |= (curr & 0x7f) << shift;
				shift += 7;
			}
			val |= curr << shift;
		}
		
		// initialize the fields
		this.binaryData = binData;
		initFields(binData, offset, val);
		
		// get the values
		return getFieldsInto(fields, holders);
	}
	
	/**
	 * @param fields
	 * @param holders
	 * @param memory
	 * @param firstSegment
	 * @param offset
	 * @return
	 */
	public boolean readBinary(int[] fields, Value[] holders, List<MemorySegment> memory, int firstSegment, int offset)
	{
		deserialize(memory, firstSegment, offset);
		return getFieldsInto(fields, holders);
	}
	
	/**
	 * @param record
	 * @param target
	 * @param furtherBuffers
	 * @param targetForUsedFurther
	 * @return
	 * @throws IOException
	 */
	public long serialize(PactRecord record, DataOutputView target, Iterator<MemorySegment> furtherBuffers,
			List<MemorySegment> targetForUsedFurther)
	throws IOException
	{
		updateBinaryRepresenation();
		
		long bytesForLen = 1;
		if (target.getRemainingBytes() >= this.binaryLen + 5) {
			int len = this.binaryLen;
			while (len >= MAX_BIT) {
				target.write(len | MAX_BIT);
				len >>= 7;
				bytesForLen++;
			}
			target.write(len);
			target.write(this.binaryData, 0, this.binaryLen);
		}
		else {
			// need to span multiple buffers
			if (target.getRemainingBytes() < 6) {
				// span var-length-int
				int len = this.binaryLen;
				while (len >= MAX_BIT) {
					if (target.getRemainingBytes() == 0) {
						target = getNextBuffer(furtherBuffers, targetForUsedFurther);
						if (target == null) {
							return -1;
						}
					}
					target.write(len | MAX_BIT);
					len >>= 7;
					bytesForLen++;
				}
				if (target.getRemainingBytes() == 0) {
					target = getNextBuffer(furtherBuffers, targetForUsedFurther);
					if (target == null) {
						return -1;
					}
				}
				target.write(len);
				if (target.getRemainingBytes() == 0) {
					target = getNextBuffer(furtherBuffers, targetForUsedFurther);
					if (target == null) {
						return -1;
					}
				}
			}
			else {
				int len = this.binaryLen;
				while (len >= MAX_BIT) {
					target.write(len | MAX_BIT);
					len >>= 7;
					bytesForLen++;
				}
				target.write(len);
			}
			
			// now write the binary data
			int currOff = 0;
			while (true) {
				int toWrite = Math.min(this.binaryLen - currOff, target.getRemainingBytes());
				target.write(this.binaryData, currOff, toWrite);
				currOff += toWrite;
				
				if (currOff < this.binaryLen) {
					target = getNextBuffer(furtherBuffers, targetForUsedFurther);
					if (target == null) {
						return -1;
					}
				}
				else {
					break;
				}
			}
		}
		
		return bytesForLen + this.binaryLen;
	}
	
	private final DataOutputView getNextBuffer(Iterator<MemorySegment> furtherBuffers, List<MemorySegment> targetForUsedFurther)
	{
		if (furtherBuffers.hasNext()) {
			MemorySegment seg = furtherBuffers.next();
			targetForUsedFurther.add(seg);
			DataOutputView target = seg.outputView;
			return target;
		}
		else return null;
	}
	
	/**
	 * @param sources
	 * @param segmentNum
	 * @param segmentOffset
	 * @throws IOException
	 */
	public void deserialize(List<MemorySegment> sources, int segmentNum, int segmentOffset)
	{
		MemorySegment seg = sources.get(segmentNum);
		
		if (seg.size() - segmentOffset > 5) {
			int val = seg.get(segmentOffset++) & 0xff;
			if (val >= MAX_BIT) {
				int shift = 7;
				int curr;
				val = val & 0x7f;
				while ((curr = seg.get(segmentOffset++) & 0xff) >= MAX_BIT) {
					val |= (curr & 0x7f) << shift;
					shift += 7;
				}
				val |= curr << shift;
			}
			this.binaryLen = val;
		}
		else {
			int end = seg.size();
			int val = seg.get(segmentOffset++) & 0xff;
			if (segmentOffset == end) {
				segmentOffset = 0;
				seg = sources.get(++segmentNum);
			}
			
			if (val >= MAX_BIT) {
				int shift = 7;
				int curr;
				val = val & 0x7f;
				while ((curr = seg.get(segmentOffset++) & 0xff) >= MAX_BIT) {
					val |= (curr & 0x7f) << shift;
					shift += 7;
					if (segmentOffset == end) {
						segmentOffset = 0;
						seg = sources.get(++segmentNum);
					}
				}
				val |= curr << shift;
			}
			this.binaryLen = val;
			if (segmentOffset == end) {
				segmentOffset = 0;
				seg = sources.get(++segmentNum);
			}
		}

		// read the binary representation
		if (this.binaryData == null || this.binaryData.length < this.binaryLen) {
			this.binaryData = new byte[this.binaryLen];
		}
		
		int remaining = seg.size() - segmentOffset;
		if (remaining >= this.binaryLen) {
			seg.get(segmentOffset, this.binaryData,	0, this.binaryLen);
		}
		else {
			// read across segments
			int offset = 0;
			while (true) {
				int toRead = Math.min(seg.size() - segmentOffset, this.binaryLen - offset);
				seg.get(segmentOffset, this.binaryData, offset, toRead);
				offset += toRead;
				segmentOffset += toRead;
				
				if (offset < this.binaryLen) {
					segmentOffset = 0;
					seg = sources.get(++segmentNum); 
				}
				else break;
			}
		}
		
		initFields(this.binaryData, 0, this.binaryLen);
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
	
	private static final int readVarLengthInt(DataInput in) throws IOException
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
	
	private static final int MAX_BIT = 0x1 << 7;
	
	
	// ------------------------------------------------------------------------
	//                Utility class for internal (de)serialization
	// ------------------------------------------------------------------------
	
	/**
	 * Internal interface class to provide serialization for the data types.
	 *
	 * @author Stephan Ewen
	 */
	private final class InternalDeSerializer implements DataInput, DataOutput
	{
		private byte[] memory;
		private int position;
		private int end;
		
		// ----------------------------------------------------------------------------------------
		//                               Data Input
		// ----------------------------------------------------------------------------------------
		
		@Override
		public boolean readBoolean() throws IOException {
			if (this.position < this.end) {
				return this.memory[this.position++] != 0;
			} else {
				throw new EOFException();
			}
		}

		@Override
		public byte readByte() throws IOException {
			if (this.position < this.end) {
				return this.memory[this.position++];
			} else {
				throw new EOFException();
			}
		}

		@Override
		public char readChar() throws IOException {
			if (this.position < this.end - 1) {
				return (char) (((this.memory[this.position++] & 0xff) << 8) | ((this.memory[this.position++] & 0xff) << 0));
			} else {
				throw new EOFException();
			}
		}

		@Override
		public double readDouble() throws IOException {
			return Double.longBitsToDouble(readLong());
		}

		@Override
		public float readFloat() throws IOException {
			return Float.intBitsToFloat(readInt());
		}

		@Override
		public void readFully(byte[] b) throws IOException {
			readFully(b, 0, b.length);
		}

		@Override
		public void readFully(byte[] b, int off, int len) throws IOException {
			if (this.position < this.end && this.position <= this.end - len && off <= b.length - len) {
				System.arraycopy(this.memory, position, b, off, len);
				position += len;
			} else {
				throw new EOFException();
			}
		}

		@Override
		public int readInt() throws IOException {
			if (this.position >= 0 && this.position < this.end - 3) {
				return ((this.memory[position++] & 0xff) << 24) | ((this.memory[position++] & 0xff) << 16)
					| ((this.memory[position++] & 0xff) << 8) | ((this.memory[position++] & 0xff) << 0);
			} else {
				throw new EOFException();
			}
		}

		@Override
		public String readLine() throws IOException
		{
			if (this.position < this.end) {
				// read until a newline is found
				StringBuilder bld = new StringBuilder();
				char curr = readChar();
				while (position < this.end && curr != '\n') {
					bld.append(curr);
					curr = readChar();
				}
				// trim a trailing carriage return
				int len = bld.length();
				if (len > 0 && bld.charAt(len - 1) == '\r') {
					bld.setLength(len - 1);
				}
				String s = bld.toString();
				bld.setLength(0);
				return s;
			} else {
				return null;
			}
		}

		@Override
		public long readLong() throws IOException {
			if (position >= 0 && position < this.end - 7) {
				return (((long) this.memory[position++] & 0xff) << 56)
					| (((long) this.memory[position++] & 0xff) << 48)
					| (((long) this.memory[position++] & 0xff) << 40)
					| (((long) this.memory[position++] & 0xff) << 32)
					| (((long) this.memory[position++] & 0xff) << 24)
					| (((long) this.memory[position++] & 0xff) << 16)
					| (((long) this.memory[position++] & 0xff) << 8)
					| (((long) this.memory[position++] & 0xff) << 0);
			} else {
				throw new EOFException();
			}
		}

		@Override
		public short readShort() throws IOException {
			if (position >= 0 && position < this.end - 1) {
				return (short) ((((this.memory[position++]) & 0xff) << 8) | (((this.memory[position++]) & 0xff) << 0));
			} else {
				throw new EOFException();
			}
		}

		@Override
		public String readUTF() throws IOException {
			int utflen = readUnsignedShort();
			byte[] bytearr = new byte[utflen];
			char[] chararr = new char[utflen];

			int c, char2, char3;
			int count = 0;
			int chararr_count = 0;

			readFully(bytearr, 0, utflen);

			while (count < utflen) {
				c = (int) bytearr[count] & 0xff;
				if (c > 127)
					break;
				count++;
				chararr[chararr_count++] = (char) c;
			}

			while (count < utflen) {
				c = (int) bytearr[count] & 0xff;
				switch (c >> 4) {
				case 0:
				case 1:
				case 2:
				case 3:
				case 4:
				case 5:
				case 6:
				case 7:
					/* 0xxxxxxx */
					count++;
					chararr[chararr_count++] = (char) c;
					break;
				case 12:
				case 13:
					/* 110x xxxx 10xx xxxx */
					count += 2;
					if (count > utflen)
						throw new UTFDataFormatException("malformed input: partial character at end");
					char2 = (int) bytearr[count - 1];
					if ((char2 & 0xC0) != 0x80)
						throw new UTFDataFormatException("malformed input around byte " + count);
					chararr[chararr_count++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
					break;
				case 14:
					/* 1110 xxxx 10xx xxxx 10xx xxxx */
					count += 3;
					if (count > utflen)
						throw new UTFDataFormatException("malformed input: partial character at end");
					char2 = (int) bytearr[count - 2];
					char3 = (int) bytearr[count - 1];
					if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
						throw new UTFDataFormatException("malformed input around byte " + (count - 1));
					chararr[chararr_count++] = (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0));
					break;
				default:
					/* 10xx xxxx, 1111 xxxx */
					throw new UTFDataFormatException("malformed input around byte " + count);
				}
			}
			// The number of chars produced may be less than utflen
			return new String(chararr, 0, chararr_count);
		}

		@Override
		public int readUnsignedByte() throws IOException {
			if (this.position < this.end) {
				return (this.memory[this.position++] & 0xff);
			} else {
				throw new EOFException();
			}
		}

		@Override
		public int readUnsignedShort() throws IOException {
			if (this.position < this.end - 1) {
				return ((this.memory[this.position++] & 0xff) << 8) | ((this.memory[this.position++] & 0xff) << 0);
			} else {
				throw new EOFException();
			}
		}

		@Override
		public int skipBytes(int n) throws IOException {
			if (this.position <= this.end - n) {
				this.position += n;
				return n;
			} else {
				n = this.end - this.position;
				this.position = this.end;
				return n;
			}
		}
		
		// ----------------------------------------------------------------------------------------
		//                               Data Output
		// ----------------------------------------------------------------------------------------
		
		@Override
		public void write(int b) throws IOException {
			if (this.position >= this.memory.length) {
				resize(1);
			}
			this.memory[this.position++] = (byte) (b & 0xff);
		}

		@Override
		public void write(byte[] b) throws IOException {
			write(b, 0, b.length);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			if (len < 0 || off > b.length - len) {
				throw new ArrayIndexOutOfBoundsException();
			}
			if (this.position > this.memory.length - len) {
				resize(len);
			}
			System.arraycopy(b, off, this.memory, this.position, len);
			this.position += len;
		}

		@Override
		public void writeBoolean(boolean v) throws IOException {
			write(v ? 1 : 0);
		}

		@Override
		public void writeByte(int v) throws IOException {
			write(v);
		}

		@Override
		public void writeBytes(String s) throws IOException {
			final int sLen = s.length();
			if (this.position >= this.memory.length - sLen) {
				resize(sLen);
			}
			
			for (int i = 0; i < sLen; i++) {
				writeByte(s.charAt(i));
			}
			this.position += sLen;
		}

		@Override
		public void writeChar(int v) throws IOException {
			if (this.position >= this.memory.length - 1) {
				resize(2);
			}
			this.memory[this.position++] = (byte) (v >> 8);
			this.memory[this.position++] = (byte) v;
		}

		@Override
		public void writeChars(String s) throws IOException {
			final int sLen = s.length();
			if (this.position >= this.memory.length - 2*sLen) {
				resize(2*sLen);
			} 
			for (int i = 0; i < sLen; i++) {
				writeChar(s.charAt(i));
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
			if (this.position >= this.memory.length - 3) {
				resize(4);
			}
			this.memory[this.position++] = (byte) (v >> 24);
			this.memory[this.position++] = (byte) (v >> 16);
			this.memory[this.position++] = (byte) (v >> 8);
			this.memory[this.position++] = (byte) v;
		}

		@Override
		public void writeLong(long v) throws IOException {
			if (this.position >= this.memory.length - 7) {
				resize(8);
			}
			this.memory[this.position++] = (byte) (v >> 56);
			this.memory[this.position++] = (byte) (v >> 48);
			this.memory[this.position++] = (byte) (v >> 40);
			this.memory[this.position++] = (byte) (v >> 32);
			this.memory[this.position++] = (byte) (v >> 24);
			this.memory[this.position++] = (byte) (v >> 16);
			this.memory[this.position++] = (byte) (v >> 8);
			this.memory[this.position++] = (byte) v;
		}

		@Override
		public void writeShort(int v) throws IOException {
			if (this.position >= this.memory.length - 1) {
				resize(2);
			}
			this.memory[this.position++] = (byte) ((v >>> 8) & 0xff);
			this.memory[this.position++] = (byte) ((v >>> 0) & 0xff);
		}

		@Override
		public void writeUTF(String str) throws IOException {
			int strlen = str.length();
			int utflen = 0;
			int c;

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
				throw new UTFDataFormatException("Encoded string is too long: " + utflen);
			
			else if (this.position > this.memory.length - utflen - 2) {
				resize(utflen);
			}
			
			byte[] bytearr = this.memory;
			int count = this.position;

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

			this.position = count;
		}
		
		private final void writeValLenIntBackwards(int value) throws IOException
		{
			if (this.position > this.memory.length - 4) {
				resize(4);
			}
			
			if (value <= 0x7f) {
				this.memory[this.position++] = (byte) value;
			}
			else if (value <= 0x3fff) {
				this.memory[this.position++] = (byte) (value >>> 7);
				this.memory[this.position++] = (byte) (value | MAX_BIT);
			}
			else if (value <= 0x1fffff) {
				this.memory[this.position++] = (byte) (value >>> 14);
				this.memory[this.position++] = (byte) ((value >>> 7) | MAX_BIT);
				this.memory[this.position++] = (byte) (value | MAX_BIT);
			}
			else if (value <= 0xfffffff) {
				this.memory[this.position++] = (byte) (  value >>> 21);
				this.memory[this.position++] = (byte) ((value >>> 14) | MAX_BIT);
				this.memory[this.position++] = (byte) ((value >>>  7) | MAX_BIT);
				this.memory[this.position++] = (byte) (value | MAX_BIT);				
			}
			else {
				this.memory[this.position++] = (byte) ( value >>> 28);
				this.memory[this.position++] = (byte) ((value >>> 21) | MAX_BIT);
				this.memory[this.position++] = (byte) ((value >>> 14) | MAX_BIT);
				this.memory[this.position++] = (byte) ((value >>>  7) | MAX_BIT);
				this.memory[this.position++] = (byte) (value | MAX_BIT);
			}
		}
		
		private final void resize(int minCapacityAdd) throws IOException
		{
			try {
				final int newLen = Math.max(this.memory.length * 2, this.memory.length + minCapacityAdd);
				byte[] nb = new byte[newLen];
				System.arraycopy(this.memory, 0, nb, 0, this.position);
				this.memory = nb;
			}
			catch (NegativeArraySizeException nasex) {
				throw new IOException("Serialization failed because the record length would exceed 2GB.");
			}
		}
	};
}
