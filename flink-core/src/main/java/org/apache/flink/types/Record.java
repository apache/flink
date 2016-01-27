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


package org.apache.flink.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.io.UTFDataFormatException;
import java.nio.ByteOrder;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemoryUtils;
import org.apache.flink.util.InstantiationUtil;


/**
 * The Record represents a multi-valued data record.
 * The record is a tuple of arbitrary values. It implements a sparse tuple model, meaning that the record can contain
 * many fields which are actually null and not represented in the record. It has internally a bitmap marking which fields
 * are set and which are not.
 * <p>
 * For efficient data exchange, a record that is read from any source holds its data in serialized binary form.
 * Fields are deserialized lazily upon first access. Modified fields are cached and the modifications are
 * incorporated into the binary representation upon the next serialization or any explicit call to the
 * {@link #updateBinaryRepresenation()} method.
 * <p>
 * IMPORTANT NOTE: Records must be used as mutable objects and be reused across user function calls in order
 * to achieve performance. The record is a heavy-weight object, designed to minimize calls to the individual fields'
 * serialization and deserialization methods. It holds quite a bit of state consumes a comparably large amount of
 * memory (&gt; 200 bytes in a 64 bit JVM) due to several pointers and arrays.
 * <p>
 * This class is NOT thread-safe!
 */
@Public
public final class Record implements Value, CopyableValue<Record> {
	private static final long serialVersionUID = 1L;
	
	private static final int NULL_INDICATOR_OFFSET = Integer.MIN_VALUE;			// value marking a field as null
	
	private static final int MODIFIED_INDICATOR_OFFSET = Integer.MIN_VALUE + 1;	// value marking field as modified
	
	private static final int DEFAULT_FIELD_LEN_ESTIMATE = 8;					// length estimate for bin array
	
	// --------------------------------------------------------------------------------------------
	
	private final InternalDeSerializer serializer = new InternalDeSerializer();	// DataInput and DataOutput abstraction
	
	private byte[] binaryData;			// the buffer containing the binary representation
	
	private byte[] switchBuffer;		// the buffer containing the binary representation
	
	private int[] offsets;				// the offsets to the binary representations of the fields
	
	private int[] lengths;				// the lengths of the fields
	
	private Value[] readFields;			// the cache for objects into which the binary representations are read
	
	private Value[] writeFields;		// the cache for objects into which the binary representations are read
	
	private int binaryLen;				// the length of the contents in the binary buffer that is valid
	
	private int numFields;				// the number of fields in the record
	
	private int firstModifiedPos;		// position of the first modification (since (de)serialization)
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Required nullary constructor for instantiation by serialization logic.
	 */
	public Record() {}
	
	/**
	 * Creates a new record containing only a single field, which is the given value.
	 * 
	 * @param value The value for the single field of the record.
	 */
	public Record(Value value) {
		setField(0, value);
	}
	
	/**
	 * Creates a new record containing exactly two fields, which are the given values.
	 * 
	 * @param val1 The value for the first field.
	 * @param val2 The value for the second field.
	 */
	public Record(Value val1, Value val2) {
		makeSpace(2);
		setField(0, val1);
		setField(1, val2);
	}
	
	/**
	 * Creates a new record, containing the given number of fields. The fields are initially all nulls.
	 *  
	 * @param numFields The number of fields for the record.
	 */
	public Record(int numFields) {
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
	public int getNumFields() {
		return this.numFields;
	}
	
	/**
	 * Sets the number of fields in the record. If the new number of fields is longer than the current number of
	 * fields, then null fields are appended. If the new number of fields is smaller than the current number of
	 * fields, then the last fields are truncated.
	 * 
	 * @param numFields The new number of fields.
	 */
	public void setNumFields(final int numFields) {
		final int oldNumFields = this.numFields;
		// check whether we increase or decrease the fields 
		if (numFields > oldNumFields) {
			makeSpace(numFields);
			for (int i = oldNumFields; i < numFields; i++) {
				this.offsets[i] = NULL_INDICATOR_OFFSET;
			}
			markModified(oldNumFields);
		}
		else {
			// decrease the number of fields
			// we do not remove the values from the cache, as the objects (if they are there) will most likely
			// be reused when the record is re-filled
			markModified(numFields);
		}
		this.numFields = numFields;
	}
	
	/**
	 * Reserves space for at least the given number of fields in the internal arrays.
	 * 
	 * @param numFields The number of fields to reserve space for.
	 */
	public void makeSpace(int numFields) {
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
		
		if (this.readFields == null) {
			this.readFields = new Value[numFields];
		}
		else if (this.readFields.length < numFields) {
			Value[] newFields = new Value[Math.max(numFields + 1, oldNumFields << 1)];
			System.arraycopy(this.readFields, 0, newFields, 0, oldNumFields);
			this.readFields = newFields;
		}
		
		if (this.writeFields == null) {
			this.writeFields = new Value[numFields];
		}
		else if (this.writeFields.length < numFields) {
			Value[] newFields = new Value[Math.max(numFields + 1, oldNumFields << 1)];
			System.arraycopy(this.writeFields, 0, newFields, 0, oldNumFields);
			this.writeFields = newFields;
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
	public <T extends Value> T getField(final int fieldNum, final Class<T> type) {
		// range check
		if (fieldNum < 0 || fieldNum >= this.numFields) {
			throw new IndexOutOfBoundsException(fieldNum + " for range [0.." + (this.numFields - 1) + "]");
		}
		
		// get offset and check for null
		final int offset = this.offsets[fieldNum];
		if (offset == NULL_INDICATOR_OFFSET) {
			return null;
		}		
		else if (offset == MODIFIED_INDICATOR_OFFSET) {
			// value that has been set is new or modified
			return (T) this.writeFields[fieldNum];
		}
		
		final int limit = offset + this.lengths[fieldNum];
		
		// get an instance, either from the instance cache or create a new one
		final Value oldField = this.readFields[fieldNum]; 
		final T field;
		if (oldField != null && oldField.getClass() == type) {
			field = (T) oldField;
		}
		else {
			field = InstantiationUtil.instantiate(type, Value.class);
			this.readFields[fieldNum] = field;
		}
		
		// deserialize
		deserialize(field, offset, limit, fieldNum);
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
	public <T extends Value> T getField(int fieldNum, T target) {
		// range check
		if (fieldNum < 0 || fieldNum >= this.numFields) {
			throw new IndexOutOfBoundsException();
		}
		if (target == null) {
			throw new NullPointerException("The target object may not be null");
		}
		
		// get offset and check for null
		final int offset = this.offsets[fieldNum];
		if (offset == NULL_INDICATOR_OFFSET) {
			return null;
		}
		else if (offset == MODIFIED_INDICATOR_OFFSET) {
			// value that has been set is new or modified
			// bring the binary in sync so that the deserialization gives the correct result
			return (T) this.writeFields[fieldNum];
		}
		
		final int limit = offset + this.lengths[fieldNum];
		deserialize(target, offset, limit, fieldNum);
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
	public boolean getFieldInto(int fieldNum, Value target) {
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
			offset = this.offsets[fieldNum];
		}
		
		final int limit = offset + this.lengths[fieldNum];
		deserialize(target, offset, limit, fieldNum);
		return true;
	}
	
	/**
	 * Gets the fields at the given positions into an array. 
	 * If at any position a field is null, then this method returns false.
	 * All fields that have been successfully read until the failing read are correctly contained in the record.
	 * All other fields are not set.
	 * 
	 * @param positions The positions of the fields to get.
	 * @param targets The values into which the content of the fields is put.
	 * 
	 * @return True if all fields were successfully read, false if some read failed.
	 */
	public boolean getFieldsInto(int[] positions, Value[] targets) {
		for (int i = 0; i < positions.length; i++) {
			if (!getFieldInto(positions[i], targets[i])) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Gets the fields at the given positions into an array. 
	 * If at any position a field is null, then this method throws a @link NullKeyFieldException.
	 * All fields that have been successfully read until the failing read are correctly contained in the record.
	 * All other fields are not set.
	 * 
	 * @param positions The positions of the fields to get.
	 * @param targets The values into which the content of the fields is put.
	 * 
	 * @throws NullKeyFieldException in case of a failing field read.
	 */
	public void getFieldsIntoCheckingNull(int[] positions, Value[] targets) {
		for (int i = 0; i < positions.length; i++) {
			if (!getFieldInto(positions[i], targets[i])) {
				throw new NullKeyFieldException(i);
			}
		}
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
	private <T extends Value> void deserialize(T target, int offset, int limit, int fieldNumber) {
		final InternalDeSerializer serializer = this.serializer;
		serializer.memory = this.binaryData;
		serializer.position = offset;
		serializer.end = limit;
		try {
			target.read(serializer);
		}
		catch (Exception e) {
			throw new DeserializationException("Error reading field " + fieldNumber + " as " + target.getClass().getName(), e);
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
	public void setField(int fieldNum, Value value) {
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
	public void addField(Value value) {
		final int num = this.numFields;
		setNumFields(num + 1);
		internallySetField(num, value);
	}
	
	private void internallySetField(int fieldNum, Value value) {
		// check if we modify an existing field
		this.offsets[fieldNum] = value != null ? MODIFIED_INDICATOR_OFFSET : NULL_INDICATOR_OFFSET;
		this.writeFields[fieldNum] = value;
		markModified(fieldNum);
	}
	
	private void markModified(int field) {
		if (this.firstModifiedPos > field) {
			this.firstModifiedPos = field;
		}
	}
	
	private boolean isModified() {
		return this.firstModifiedPos != Integer.MAX_VALUE;
	}
	
	/**
	 * Removes the field at the given position. 
	 * <p>
	 * This method should be used carefully. Be aware that as the field is actually removed from the record, the
	 * total number of fields is modified, and all fields to the right of the field removed shift one position to
	 * the left.
	 * 
	 * @param fieldNum The position of the field to be removed, starting at zero.
	 * @throws IndexOutOfBoundsException Thrown, when the position is not between 0 (inclusive) and the
	 *                                   number of fields (exclusive).
	 */
	public void removeField(int fieldNum)
	{
		// range check
		if (fieldNum < 0 || fieldNum >= this.numFields) {
			throw new IndexOutOfBoundsException();
		}
		int lastIndex = this.numFields - 1;		

		if (fieldNum < lastIndex) {
			int len = lastIndex - fieldNum;
			System.arraycopy(this.offsets, fieldNum + 1, this.offsets, fieldNum, len);
			System.arraycopy(this.lengths, fieldNum + 1, this.lengths, fieldNum, len);
			System.arraycopy(this.readFields, fieldNum + 1, this.readFields, fieldNum, len);
			System.arraycopy(this.writeFields, fieldNum + 1, this.writeFields, fieldNum, len);
			markModified(fieldNum);
		}
		this.offsets[lastIndex] = NULL_INDICATOR_OFFSET;
		this.lengths[lastIndex] = 0;
		this.writeFields[lastIndex] = null;

		setNumFields(lastIndex);
	}
	
	public final boolean isNull(int fieldNum) {
		// range check
		if (fieldNum < 0 || fieldNum >= this.numFields) {
			throw new IndexOutOfBoundsException();
		}
		
		// get offset and check for null
		final int offset = this.offsets[fieldNum];
		return offset == NULL_INDICATOR_OFFSET;
	}
	
	/**
	 * Sets the field at the given position to <code>null</code>.
	 * 
	 * @param field The field index.
	 * @throws IndexOutOfBoundsException Thrown, when the position is not between 0 (inclusive) and the
	 *                                   number of fields (exclusive).
	 */
	public void setNull(int field) {
		// range check
		if (field < 0 || field >= this.numFields) {
			throw new IndexOutOfBoundsException();
		}

		internallySetField(field, null);
	}
	
	/**
	 * Sets the fields to <code>null</code> using the given bit mask.
	 * The bits correspond to the individual columns: <code>(1 == nullify, 0 == keep)</code>.
	 * 
	 * @param mask Bit mask, where the i-th least significant bit represents the i-th field in the record.
	 */
	public void setNull(long mask) {
		for (int i = 0; i < this.numFields; i++, mask >>>= 1) {
			if ((mask & 0x1) != 0) {
				internallySetField(i, null);
			}
		}
	}

	/**
	 * Sets the fields to <code>null</code> using the given bit mask.
	 * The bits correspond to the individual columns: <code>(1 == nullify, 0 == keep)</code>.
	 * 
	 * @param mask Bit mask, where the i-th least significant bit in the n-th bit mask represents the
	 *             <code>(n*64) + i</code>-th field in the record.
	 */
	public void setNull(long[] mask) {
		for (int maskPos = 0, i = 0; i < this.numFields;) {
			long currMask = mask[maskPos];
			for (int k = 64; i < this.numFields && k > 0; --k, i++, currMask >>>= 1) {
				if ((currMask & 0x1) != 0) {
					internallySetField(i, null);
				}
			}
		}
	}
	
	/**
	 * Clears the record. After this operation, the record will have zero fields.
	 */
	public void clear() {
		if (this.numFields > 0) {
			this.numFields = 0;
			this.firstModifiedPos = 0;
		}
	}
	
	public void concatenate(Record record) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Unions the other record's fields with this records fields. After the method invocation with record
	 * <code>B</code> as the parameter, this record <code>A</code> will contain at field <code>i</code>:
	 * <ul>
	 *   <li>Field <code>i</code> from record <code>A</code>, if that field is within record <code>A</code>'s number
	 *       of fields and is not <i>null</i>.</li>
	 *   <li>Field <code>i</code> from record <code>B</code>, if that field is within record <code>B</code>'s number
	 *       of fields.</li>
	 * </ul>
	 * It is not necessary that both records have the same number of fields. This record will have the number of
	 * fields of the larger of the two records. Naturally, if both <code>A</code> and <code>B</code> have field
	 * <code>i</code> set to <i>null</i>, this record will have <i>null</i> at that position.
	 * 
	 * @param other The records whose fields to union with this record's fields.
	 */
	public void unionFields(Record other) {
		final int minFields = Math.min(this.numFields, other.numFields);
		final int maxFields = Math.max(this.numFields, other.numFields);
		
		final int[] offsets = this.offsets.length >= maxFields ? this.offsets : new int[maxFields];
		final int[] lengths = this.lengths.length >= maxFields ? this.lengths : new int[maxFields];
		
		if (!(this.isModified() || other.isModified())) {
			// handle the special (but common) case where both records have a valid binary representation differently
			// allocate space for the switchBuffer first
			final int estimatedLength = this.binaryLen + other.binaryLen;
			this.serializer.memory = (this.switchBuffer != null && this.switchBuffer.length >= estimatedLength) ? 
										this.switchBuffer : new byte[estimatedLength];
			this.serializer.position = 0;
			
			try {
				// common loop for both records
				for (int i = 0; i < minFields; i++) {
					final int thisOff = this.offsets[i];
					if (thisOff == NULL_INDICATOR_OFFSET) {
						final int otherOff = other.offsets[i];
						if (otherOff == NULL_INDICATOR_OFFSET) {
							offsets[i] = NULL_INDICATOR_OFFSET;
						} else {
							// take field from other record
							offsets[i] = this.serializer.position;
							this.serializer.write(other.binaryData, otherOff, other.lengths[i]);
							lengths[i] = other.lengths[i];
						}
					} else {
						// copy field from this one
						offsets[i] = this.serializer.position;
						this.serializer.write(this.binaryData, thisOff, this.lengths[i]);
						lengths[i] = this.lengths[i];
					}
				}
				
				// add the trailing fields from one record
				if (minFields != maxFields) {
					final Record sourceForRemainder = this.numFields > minFields ? this : other;
					int begin = -1;
					int end = -1;
					int offsetDelta = 0;
					
					// go through the offsets, find the non-null fields to account for the remaining data
					for (int k = minFields; k < maxFields; k++) {
						final int off = sourceForRemainder.offsets[k];
						if (off == NULL_INDICATOR_OFFSET) {
							offsets[k] = NULL_INDICATOR_OFFSET;
						} else {
							end = sourceForRemainder.offsets[k]+sourceForRemainder.lengths[k];
							if (begin == -1) {
								// first non null column in the remainder
								begin = sourceForRemainder.offsets[k];
								offsetDelta = this.serializer.position - begin;
							}
							offsets[k] = sourceForRemainder.offsets[k] + offsetDelta;
						}
					}
					
					// copy the remaining fields directly as binary
					if (begin != -1) {
						this.serializer.write(sourceForRemainder.binaryData, begin, 
								end - begin);
					}
					
					// the lengths can be copied directly
					if (lengths != sourceForRemainder.lengths) {
						System.arraycopy(sourceForRemainder.lengths, minFields, lengths, minFields, maxFields - minFields);
					}
				}
			} catch (Exception ioex) {
				throw new RuntimeException("Error creating field union of record data" + 
							ioex.getMessage() == null ? "." : ": " + ioex.getMessage(), ioex);
			}
		}
		else {
			// the general case, where at least one of the two records has a binary representation that is not in sync.
			final int estimatedLength = (this.binaryLen > 0 ? this.binaryLen : this.numFields * DEFAULT_FIELD_LEN_ESTIMATE) + 
										(other.binaryLen > 0 ? other.binaryLen : other.numFields * DEFAULT_FIELD_LEN_ESTIMATE);
			this.serializer.memory = (this.switchBuffer != null && this.switchBuffer.length >= estimatedLength) ? 
										this.switchBuffer : new byte[estimatedLength];
			this.serializer.position = 0;
			
			try {
				// common loop for both records
				for (int i = 0; i < minFields; i++) {
					final int thisOff = this.offsets[i];
					if (thisOff == NULL_INDICATOR_OFFSET) {
						final int otherOff = other.offsets[i];
						if (otherOff == NULL_INDICATOR_OFFSET) {
							offsets[i] = NULL_INDICATOR_OFFSET;
						} else if (otherOff == MODIFIED_INDICATOR_OFFSET) {
							// serialize modified field from other record
							offsets[i] = this.serializer.position;
							other.writeFields[i].write(this.serializer);
							lengths[i] = this.serializer.position - offsets[i];
						} else {
							// take field from other record binary
							offsets[i] = this.serializer.position;
							this.serializer.write(other.binaryData, otherOff, other.lengths[i]);
							lengths[i] = other.lengths[i];
						}
					} else if (thisOff == MODIFIED_INDICATOR_OFFSET) {
						// serialize modified field from this record
						offsets[i] = this.serializer.position;
						this.writeFields[i].write(this.serializer);
						lengths[i] = this.serializer.position - offsets[i];
					} else {
						// copy field from this one
						offsets[i] = this.serializer.position;
						this.serializer.write(this.binaryData, thisOff, this.lengths[i]);
						lengths[i] = this.lengths[i];
					}
				}
				
				// add the trailing fields from one record
				if (minFields != maxFields) {
					final Record sourceForRemainder = this.numFields > minFields ? this : other;
					
					// go through the offsets, find the non-null fields
					for (int k = minFields; k < maxFields; k++) {
						final int off = sourceForRemainder.offsets[k];
						if (off == NULL_INDICATOR_OFFSET) {
							offsets[k] = NULL_INDICATOR_OFFSET;
						} else if (off == MODIFIED_INDICATOR_OFFSET) {
							// serialize modified field from the source record
							offsets[k] = this.serializer.position;
							sourceForRemainder.writeFields[k].write(this.serializer);
							lengths[k] = this.serializer.position - offsets[k];
						} else {
							// copy field from the source record binary
							offsets[k] = this.serializer.position;
							final int len = sourceForRemainder.lengths[k];
							this.serializer.write(sourceForRemainder.binaryData, off, len);
							lengths[k] = len;
						}
					}
				}
			} catch (Exception ioex) {
				throw new RuntimeException("Error creating field union of record data" + 
							ioex.getMessage() == null ? "." : ": " + ioex.getMessage(), ioex);
			}
		}
		
		serializeHeader(this.serializer, offsets, maxFields);
		
		// set the fields
		this.switchBuffer = this.binaryData;
		this.binaryData = serializer.memory;
		this.binaryLen = serializer.position;
		
		this.numFields = maxFields;
		this.offsets = offsets;
		this.lengths = lengths;
		
		this.firstModifiedPos = Integer.MAX_VALUE;
		
		// make sure that the object arrays reflect the size as well
		if (this.readFields == null || this.readFields.length < maxFields) {
			final Value[] na = new Value[maxFields];
			System.arraycopy(this.readFields, 0, na, 0, this.readFields.length);
			this.readFields = na;
		}
		this.writeFields = (this.writeFields == null || this.writeFields.length < maxFields) ? 
																new Value[maxFields] : this.writeFields;
	}
	
	/**
	 * @param target
	 */
	public void copyTo(Record target) {
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
		if (target.readFields == null || target.readFields.length < this.numFields) {
			target.readFields = new Value[this.numFields];
		}
		if (target.writeFields == null || target.writeFields.length < this.numFields) {
			target.writeFields = new Value[this.numFields];
		}
		
		System.arraycopy(this.binaryData, 0, target.binaryData, 0, this.binaryLen);
		System.arraycopy(this.offsets, 0, target.offsets, 0, this.numFields);
		System.arraycopy(this.lengths, 0, target.lengths, 0, this.numFields);
		
		target.binaryLen = this.binaryLen;
		target.numFields = this.numFields;
		target.firstModifiedPos = Integer.MAX_VALUE;
	}
	
	@Override
	public int getBinaryLength() {
		return -1;
	}

	@Override
	public Record copy() {
		return createCopy();
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int val = source.readUnsignedByte();
		target.writeByte(val);
		
		if (val >= MAX_BIT) {
			int shift = 7;
			int curr;
			val = val & 0x7f;
			while ((curr = source.readUnsignedByte()) >= MAX_BIT) {
				target.writeByte(curr);
				val |= (curr & 0x7f) << shift;
				shift += 7;
			}
			target.writeByte(curr);
			val |= curr << shift;
		}
		
		target.write(source, val);
	};
	
	/**
	 * Creates an exact copy of this record.
	 * 
	 * @return An exact copy of this record. 
	 */
	public Record createCopy() {
		final Record rec = new Record();
		copyTo(rec);
		return rec;
	}
	
	/**
	 * Bin-copies fields from a source record to this record. The following caveats apply:
	 * 
	 * If the source field is in a modified state, no binary representation will exist yet.
	 * In that case, this method is equivalent to {@code setField(..., source.getField(..., <class>))}.
	 * In particular, if setValue is called on the source field Value instance, that change 
	 * will propagate to this record.
	 * 
	 * If the source field has already been serialized, then the binary representation 
	 * will be copied. Further modifications to the source field will not be observable 
	 * via this record, but attempting to read the field from this record will cause it 
	 * to be deserialized.
	 * 
	 * Finally, bin-copying a source field requires calling updateBinaryRepresentation
	 * on this instance in order to reserve space in the binaryData array. If none
	 * of the source fields are actually bin-copied, then updateBinaryRepresentation
	 * won't be called. 
	 *
	 * @param source
	 * @param sourcePositions
	 * @param targetPositions
	 */
	public void copyFrom(final Record source, final int[] sourcePositions, final int[] targetPositions) {
		
		final int[] sourceOffsets = source.offsets;
		final int[] sourceLengths = source.lengths;
		final byte[] sourceBuffer = source.binaryData;
		final Value[] sourceFields = source.writeFields;

		boolean anyFieldIsBinary = false;
		int maxFieldNum = 0;
		
		for (int i = 0; i < sourcePositions.length; i++) {
		
			final int sourceFieldNum = sourcePositions[i];
			final int sourceOffset = sourceOffsets[sourceFieldNum];
			final int targetFieldNum = targetPositions[i];

			maxFieldNum = Math.max(targetFieldNum, maxFieldNum);

			if (sourceOffset == NULL_INDICATOR_OFFSET) {
				// set null on existing field (new fields are null by default)
				if (targetFieldNum < numFields) {
					internallySetField(targetFieldNum, null);
				}
			} else if (sourceOffset != MODIFIED_INDICATOR_OFFSET) {
				anyFieldIsBinary = true;
			}
		}
		
		if (numFields < maxFieldNum + 1) {
			setNumFields(maxFieldNum + 1);
		}
		
		final int[] targetLengths = this.lengths;
		final int[] targetOffsets = this.offsets;
		
		// reserve space in binaryData for the binary source fields
		if (anyFieldIsBinary) {
			
			for (int i = 0; i < sourcePositions.length; i++) {
				final int sourceFieldNum = sourcePositions[i];
				final int sourceOffset = sourceOffsets[sourceFieldNum];
				
				if (sourceOffset != MODIFIED_INDICATOR_OFFSET && sourceOffset != NULL_INDICATOR_OFFSET) {
					final int targetFieldNum = targetPositions[i];
					targetLengths[targetFieldNum] = sourceLengths[sourceFieldNum];
					internallySetField(targetFieldNum, RESERVE_SPACE);
				}
			}
			
			updateBinaryRepresenation();
		}
			
		final byte[] targetBuffer = this.binaryData;

		for (int i = 0; i < sourcePositions.length; i++) {
			final int sourceFieldNum = sourcePositions[i];
			final int sourceOffset = sourceOffsets[sourceFieldNum];
			final int targetFieldNum = targetPositions[i];

			if (sourceOffset == MODIFIED_INDICATOR_OFFSET) {
				internallySetField(targetFieldNum, sourceFields[sourceFieldNum]);
			} else if (sourceOffset != NULL_INDICATOR_OFFSET) {
				// bin-copy
				final int targetOffset = targetOffsets[targetFieldNum];
				final int length = targetLengths[targetFieldNum];
				System.arraycopy(sourceBuffer, sourceOffset, targetBuffer, targetOffset, length);
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Checks the values of this record and a given list of values at specified positions for equality.
	 * The values of this record are deserialized and compared against the corresponding search value.
	 * The position specify which values are compared.
	 * The method returns true if the values on all positions are equal and false otherwise.  
	 * 
	 * @param positions The positions of the values to check for equality.
	 * @param searchValues The values against which the values of this record are compared.
	 * @param deserializationHolders An array to hold the deserialized values of this record.
	 * 
	 * @return True if all the values on all positions are equal, false otherwise.
	 */
	public final boolean equalsFields(int[] positions, Value[] searchValues, Value[] deserializationHolders) {
		for (int i = 0; i < positions.length; i++) {
			final Value v = getField(positions[i], deserializationHolders[i]);
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
	public void updateBinaryRepresenation() {
		// check whether the binary state is in sync
		final int firstModified = this.firstModifiedPos;
		if (firstModified == Integer.MAX_VALUE) {
			return;
		}
		
		final InternalDeSerializer serializer = this.serializer;
		final int[] offsets = this.offsets;
		final int numFields = this.numFields;
		
		serializer.memory = this.switchBuffer != null ? this.switchBuffer : 
				(this.binaryLen > 0 ? new byte[this.binaryLen] : new byte[numFields * DEFAULT_FIELD_LEN_ESTIMATE + 1]);
		serializer.position = 0;
		
		if (numFields > 0) {
			int offset = 0;
			
			// search backwards to find the latest preceding non-null field
			if (firstModified > 0) {
				for (int i = firstModified - 1; i >= 0; i--) {
					if (this.offsets[i] != NULL_INDICATOR_OFFSET) {
						offset = this.offsets[i] + this.lengths[i];
						break;
					}
				}
			}
			
			// we assume that changed and unchanged fields are interleaved and serialize into another array
			try {
				if (offset > 0) {
					// copy the first unchanged portion as one
					serializer.write(this.binaryData, 0, offset);
				}
				// copy field by field
				for (int i = firstModified; i < numFields; i++) {
					final int co = offsets[i];
					/// skip null fields
					if (co == NULL_INDICATOR_OFFSET) {
						continue;
					}
					
					offsets[i] = offset;
					if (co == MODIFIED_INDICATOR_OFFSET) {
						
						final Value writeField = this.writeFields[i];
						
						if (writeField == RESERVE_SPACE) {
							// RESERVE_SPACE is a placeholder indicating lengths[i] bytes should be reserved
							final int length = this.lengths[i];
							
							if (serializer.position >= serializer.memory.length - length - 1) {
								serializer.resize(length);
							}
							serializer.position += length;
							
						} else {
							// serialize modified fields
							this.writeFields[i].write(serializer);
						}
					} else {
						// bin-copy unmodified fields
						serializer.write(this.binaryData, co, this.lengths[i]);
					}
					
					this.lengths[i] = serializer.position - offset;
					offset = serializer.position;
				}
			}
			catch (Exception e) {
				throw new RuntimeException("Error in data type serialization: " + e.getMessage(), e); 
			}
		}
		
		serializeHeader(serializer, offsets, numFields);
		
		// set the fields
		this.switchBuffer = this.binaryData;
		this.binaryData = serializer.memory;
		this.binaryLen = serializer.position;
		this.firstModifiedPos = Integer.MAX_VALUE;
	}
	
	private void serializeHeader(final InternalDeSerializer serializer, final int[] offsets, final int numFields) {
		try {
			if (numFields > 0) {
				int slp = serializer.position;	// track the last position of the serializer
				
				// now, serialize the lengths, the sparsity mask and the number of fields
				if (numFields <= 8) {
					// efficient handling of common case with up to eight fields
					int mask = 0;
					for (int i = numFields - 1; i > 0; i--) {
						if (offsets[i] != NULL_INDICATOR_OFFSET) {
							slp = serializer.position;
							serializer.writeValLenIntBackwards(offsets[i]);
							mask |= 0x1;
						}
						mask <<= 1;
					}
	
					if (offsets[0] != NULL_INDICATOR_OFFSET) {
						mask |= 0x1;	// add the non-null bit to the mask
					} else {
						// the first field is null, so some previous field was the first non-null field
						serializer.position = slp;
					}
					serializer.writeByte(mask);
				}
				else {
					// general case. offsets first (in backward order)
					for (int i = numFields - 1; i > 0; i--) {
						if (offsets[i] != NULL_INDICATOR_OFFSET) {
							slp = serializer.position;
							serializer.writeValLenIntBackwards(offsets[i]);
						}
					}
					if (offsets[0] == NULL_INDICATOR_OFFSET) {
						serializer.position = slp;
					}
					
					// now the mask. we write it in chucks of 8 bit.
					// the remainder %8 comes first 
					int col = numFields - 1;
					int mask = 0;
					int i = numFields & 0x7;
					
					if (i > 0) {
						for (; i > 0; i--, col--) {
							mask <<= 1;
							mask |= (offsets[col] != NULL_INDICATOR_OFFSET) ? 0x1 : 0x0;
						}
						serializer.writeByte(mask);
					}
					
					// now the eight-bit chunks
					for (i = numFields >>> 3; i > 0; i--) {
						mask = 0;
						for (int k = 0; k < 8; k++, col--) {
							mask <<= 1;
							mask |= (offsets[col] != NULL_INDICATOR_OFFSET) ? 0x1 : 0x0;
						}
						serializer.writeByte(mask);
					}
				}
			}
			serializer.writeValLenIntBackwards(numFields);
		}
		catch (Exception e) {
			throw new RuntimeException("Error serializing Record header: " + e.getMessage(), e);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                             Serialization
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void write(DataOutputView out) throws IOException {
		// make sure everything is in a valid binary representation
		updateBinaryRepresenation();
		
		// write the length first, variably encoded, then the contents of the binary array
		writeVarLengthInt(out, this.binaryLen);
		out.write(this.binaryData, 0, this.binaryLen);
	}

	@Override
	public void read(DataInputView in) throws IOException {
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
	
	private void initFields(final byte[] data, final int begin, final int len) {
		try {
			// read number of fields, variable length encoded reverse at the back
			int pos = begin + len - 2;
			int numFields = data[begin + len - 1] & 0xFF;
			if (numFields >= MAX_BIT) {
				int shift = 7;
				int curr;
				numFields = numFields & 0x7f;
				while ((curr = data[pos--] & 0xff) >= MAX_BIT) {
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
			if (this.lengths == null || this.lengths.length < numFields) {
				this.lengths = new int[numFields];
			}
			if (this.readFields == null || this.readFields.length < numFields) {
				this.readFields = new Value[numFields];
			}
			if (this.writeFields == null || this.writeFields.length < numFields) {
				this.writeFields = new Value[numFields];
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
							int start = data[pos--] & 0xff;
							if (start >= MAX_BIT) {
								int shift = 7;
								int curr;
								start = start & 0x7f;
								while ((curr = data[pos--] & 0xff) >= MAX_BIT) {
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
		}
		catch (ArrayIndexOutOfBoundsException aioobex) {
			StringBuilder bld = new StringBuilder(len * 4 + 64);
			bld.append("Record deserialization error: Record byte signature: ");
			
			for (int i = 0; i < len; i++) {
				int num = data[i + begin] & 0xff;
				bld.append(num);
				if (i < len - 1) {
					bld.append(',');
				}
			}
			throw new RuntimeException(bld.toString(), aioobex);
		}
	}
	
	/**
	 * Writes this record to the given output view. This method is similar to {@link org.apache.flink.core.io.IOReadableWritable#write(org.apache.flink.core.memory.DataOutputView)}, but
	 * it returns the number of bytes written.
	 * 
	 * @param target The view to write the record to.
	 * @return The number of bytes written.
	 * 
	 * @throws IOException Thrown, if an error occurred in the view during writing.
	 */
	public long serialize(DataOutputView target) throws IOException {
		updateBinaryRepresenation();
		
		long bytesForLen = 1;
		int len = this.binaryLen;
		while (len >= MAX_BIT) {
			target.write(len | MAX_BIT);
			len >>= 7;
			bytesForLen++;
		}
		target.write(len);
		target.write(this.binaryData, 0, this.binaryLen);

		return bytesForLen + this.binaryLen;
	}
	
	/**
	 * @param source
	 * @throws IOException
	 */
	public void deserialize(DataInputView source) throws IOException {
		read(source);
	}
	
	// --------------------------------------------------------------------------------------------
	//                                     Utilities
	// --------------------------------------------------------------------------------------------
	
	private static final void writeVarLengthInt(DataOutput out, int value) throws IOException {
		while (value >= MAX_BIT) {
			out.write(value | MAX_BIT);
			value >>= 7;
		}
		out.write(value);
	}
	
	private static final int readVarLengthInt(DataInput in) throws IOException {
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
	 * A special Value instance that doesn't actually (de)serialize anything,
	 * but instead indicates that space should be reserved in the buffer.
	 */
	private static final Value RESERVE_SPACE = new Value() {
		private static final long serialVersionUID = 1L;

		@Override
		public void write(DataOutputView out) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void read(DataInputView in) throws IOException {
			throw new UnsupportedOperationException();
		}		
	};
	
	/**
	 * Internal interface class to provide serialization for the data types.
	 */
	private static final class InternalDeSerializer implements DataInputView, DataOutputView, Serializable {
		
		private static final long serialVersionUID = 1L;
		
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
			if (len >= 0) {
				if (off <= b.length - len) {
					if (this.position <= this.end - len) {
						System.arraycopy(this.memory, position, b, off, len);
						position += len;
					} else {
						throw new EOFException();
					}
				} else {
					throw new ArrayIndexOutOfBoundsException();
				}
			} else if (len < 0) {
				throw new IllegalArgumentException("Length may not be negative.");
			}
		}

		@Override
		public int readInt() throws IOException {
			if (this.position >= 0 && this.position < this.end - 3) {
				@SuppressWarnings("restriction")
				int value = UNSAFE.getInt(this.memory, BASE_OFFSET + this.position);
				if (LITTLE_ENDIAN) {
					value = Integer.reverseBytes(value);
				}
				
				this.position += 4;
				return value;
			} else {
				throw new EOFException();
			}
		}

		@Override
		public String readLine() throws IOException {
			if (this.position < this.end) {
				// read until a newline is found
				StringBuilder bld = new StringBuilder();
				char curr = (char) readUnsignedByte();
				while (position < this.end && curr != '\n') {
					bld.append(curr);
					curr = (char) readUnsignedByte();
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
				@SuppressWarnings("restriction")
				long value = UNSAFE.getLong(this.memory, BASE_OFFSET + this.position);
				if (LITTLE_ENDIAN) {
					value = Long.reverseBytes(value);
				}
				this.position += 8;
				return value;
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
				if (c > 127) {
					break;
				}
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
					if (count > utflen) {
						throw new UTFDataFormatException("malformed input: partial character at end");
					}
					char2 = (int) bytearr[count - 1];
					if ((char2 & 0xC0) != 0x80) {
						throw new UTFDataFormatException("malformed input around byte " + count);
					}
					chararr[chararr_count++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
					break;
				case 14:
					/* 1110 xxxx 10xx xxxx 10xx xxxx */
					count += 3;
					if (count > utflen) {
						throw new UTFDataFormatException("malformed input: partial character at end");
					}
					char2 = (int) bytearr[count - 2];
					char3 = (int) bytearr[count - 1];
					if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
						throw new UTFDataFormatException("malformed input around byte " + (count - 1));
					}
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

		@Override
		public void skipBytesToRead(int numBytes) throws IOException {
			if (this.end - this.position < numBytes) {
				throw new EOFException("Could not skip " + numBytes + ".");
			}
			skipBytes(numBytes);
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			if (b == null) {
				throw new NullPointerException("Byte array b cannot be null.");
			}

			if (off < 0) {
				throw new IndexOutOfBoundsException("Offset cannot be negative.");
			}

			if (len < 0) {
				throw new IndexOutOfBoundsException("Length cannot be negative.");
			}

			if (b.length - off < len) {
				throw new IndexOutOfBoundsException("Byte array does not provide enough space to store requested data" +
						".");
			}

			if (this.position >= this.end) {
				return -1;
			} else {
				int toRead = Math.min(this.end-this.position, len);
				System.arraycopy(this.memory,this.position,b,off,toRead);
				this.position += toRead;

				return toRead;
			}
		}

		@Override
		public int read(byte[] b) throws IOException {
			return read(b, 0, b.length);
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

		@SuppressWarnings("restriction")
		@Override
		public void writeInt(int v) throws IOException {
			if (this.position >= this.memory.length - 3) {
				resize(4);
			}
			if (LITTLE_ENDIAN) {
				v = Integer.reverseBytes(v);
			}			
			UNSAFE.putInt(this.memory, BASE_OFFSET + this.position, v);
			this.position += 4;
		}

		@SuppressWarnings("restriction")
		@Override
		public void writeLong(long v) throws IOException {
			if (this.position >= this.memory.length - 7) {
				resize(8);
			}
			if (LITTLE_ENDIAN) {
				v = Long.reverseBytes(v);
			}
			UNSAFE.putLong(this.memory, BASE_OFFSET + this.position, v);
			this.position += 8;
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

			if (utflen > 65535) {
				throw new UTFDataFormatException("Encoded string is too long: " + utflen);
			} else if (this.position > this.memory.length - utflen - 2) {
				resize(utflen + 2);
			}
			
			byte[] bytearr = this.memory;
			int count = this.position;

			bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
			bytearr[count++] = (byte) ((utflen >>> 0) & 0xFF);

			int i = 0;
			for (i = 0; i < strlen; i++) {
				c = str.charAt(i);
				if (!((c >= 0x0001) && (c <= 0x007F))) {
					break;
				}
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
		
		private void writeValLenIntBackwards(int value) throws IOException {
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
		
		private void resize(int minCapacityAdd) throws IOException {
			try {
				final int newLen = Math.max(this.memory.length * 2, this.memory.length + minCapacityAdd);
				final byte[] nb = new byte[newLen];
				System.arraycopy(this.memory, 0, nb, 0, this.position);
				this.memory = nb;
			}
			catch (NegativeArraySizeException nasex) {
				throw new IOException("Serialization failed because the record length would exceed 2GB.");
			}
		}
		
		@SuppressWarnings("restriction")
		private static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;
		
		@SuppressWarnings("restriction")
		private static final long BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
		
		private static final boolean LITTLE_ENDIAN = (MemoryUtils.NATIVE_BYTE_ORDER == ByteOrder.LITTLE_ENDIAN);

		@Override
		public void skipBytesToWrite(int numBytes) throws IOException {
			int skippedBytes = skipBytes(numBytes);

			if (skippedBytes != numBytes) {
				throw new EOFException("Could not skip " + numBytes + " bytes.");
			}
		}

		@Override
		public void write(DataInputView source, int numBytes) throws IOException {
			if (numBytes > this.end - this.position) {
				throw new IOException("Could not write " + numBytes + " bytes since the buffer is full.");
			}

			source.read(this.memory,this.position, numBytes);
			this.position += numBytes;
		}
	}
}
