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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;
import java.io.Serializable;

/**
 * This interface describes the methods that are required for a data type to be handled by the pact
 * runtime. Specifically, this interface contains the methods used for hashing, comparing, and creating
 * auxiliary structures.
 * <p>
 * The methods in this interface depend not only on the record, but also on what fields of a record are
 * used for the comparison or hashing. That set of fields is typically a subset of a record's fields.
 * In general, this class assumes a contract on hash codes and equality the same way as defined for
 * {@link java.lang.Object#equals(Object)} {@link java.lang.Object#equals(Object)}
 * <p>
 * Implementing classes are stateful, because several methods require to set one record as the reference for
 * comparisons and later comparing a candidate against it. Therefore, the classes implementing this interface are
 * not thread safe. The runtime will ensure that no instance is used twice in different threads, but will create
 * a copy for that purpose. It is hence imperative that the copies created by the {@link #duplicate()} method
 * share no state with the instance from which they were copied: they have to be deep copies.
 *
 * @see java.lang.Object#hashCode()
 * @see java.lang.Object#equals(Object)
 * @see java.util.Comparator#compare(Object, Object)
 * 
 * @param <T> The data type that the comparator works on.
 */
@PublicEvolving
public abstract class TypeComparator<T> implements Serializable {
	
	private static final long serialVersionUID = 1L;

	/**
	 * Computes a hash value for the given record. The hash value should include all fields in the record
	 * relevant to the comparison.
	 * <p>
	 * The hash code is typically not used as it is in hash tables and for partitioning, but it is further
	 * scrambled to make sure that a projection of the hash values to a lower cardinality space is as
	 * results in a rather uniform value distribution.
	 * However, any collisions produced by this method cannot be undone. While it is NOT
	 * important to create hash codes that cover the full spectrum of bits in the integer, it IS important 
	 * to avoid collisions when combining two value as much as possible.
	 * 
	 * @param record The record to be hashed.
	 * @return A hash value for the record.
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	public abstract int hash(T record);
	
	/**
	 * Sets the given element as the comparison reference for future calls to
	 * {@link #equalToReference(Object)} and {@link #compareToReference(TypeComparator)}. This method
	 * must set the given element into this comparator instance's state. If the comparison happens on a subset
	 * of the fields from the record, this method may extract those fields.
	 * <p>
	 * A typical example for checking the equality of two elements is the following:
	 * <pre>{@code
	 * E e1 = ...;
	 * E e2 = ...;
	 * 
	 * TypeComparator<E> acc = ...;
	 * 
	 * acc.setReference(e1);
	 * boolean equal = acc.equalToReference(e2);
	 * }</pre>
	 * 
	 * The rational behind this method is that elements are typically compared using certain features that
	 * are extracted from them, (such de-serializing as a subset of fields). When setting the
	 * reference, this extraction happens. The extraction needs happen only once per element,
	 * even though an element is often compared to multiple other elements, such as when finding equal elements
	 * in the process of grouping the elements.
	 * 
	 * @param toCompare The element to set as the comparison reference.
	 */
	public abstract void setReference(T toCompare);
	
	/**
	 * Checks, whether the given element is equal to the element that has been set as the comparison
	 * reference in this comparator instance.
	 * 
	 * @param candidate The candidate to check.
	 * @return True, if the element is equal to the comparison reference, false otherwise.
	 * 
	 * @see #setReference(Object)
	 */
	public abstract boolean equalToReference(T candidate);
	
	/**
	 * This method compares the element that has been set as reference in this type accessor, to the
	 * element set as reference in the given type accessor. Similar to comparing two
	 * elements {@code e1} and {@code e2} via a comparator, this method can be used the
	 * following way.
	 * 
	 * <pre>{@code
	 * E e1 = ...;
	 * E e2 = ...;
	 * 
	 * TypeComparator<E> acc1 = ...;
	 * TypeComparator<E> acc2 = ...;
	 * 
	 * acc1.setReference(e1);
	 * acc2.setReference(e2);
	 * 
	 * int comp = acc1.compareToReference(acc2);
	 * }</pre>
	 * 
	 * The rational behind this method is that elements are typically compared using certain features that
	 * are extracted from them, (such de-serializing as a subset of fields). When setting the
	 * reference, this extraction happens. The extraction needs happen only once per element,
	 * even though an element is typically compared to many other elements when establishing a
	 * sorted order. The actual comparison performed by this method may be very cheap, as it
	 * happens on the extracted features.
	 * 
	 * @param referencedComparator The type accessors where the element for comparison has been set
	 *                            as reference.
	 * 
	 * @return A value smaller than zero, if the reference value of {@code referencedAccessors} is smaller
	 *         than the reference value of this type accessor; a value greater than zero, if it is larger;
	 *         zero, if both are equal.
	 * 
	 * @see #setReference(Object)
	 */
	public abstract int compareToReference(TypeComparator<T> referencedComparator);

	// A special case method that the runtime uses for special "PactRecord" support
	public boolean supportsCompareAgainstReference() {
		return false;
	}
	
	/**
	 * Compares two records in object form. The return value indicates the order of the two in the same way
	 * as defined by {@link java.util.Comparator#compare(Object, Object)}.
	 *
	 * @param first The first record.
	 * @param second The second record.
	 * @return An integer defining the oder among the objects in the same way as {@link java.util.Comparator#compare(Object, Object)}.
	 * 
	 *  @see java.util.Comparator#compare(Object, Object)
	 */
	public abstract int compare(T first, T second);
	
	/**
	 * Compares two records in serialized form. The return value indicates the order of the two in the same way
	 * as defined by {@link java.util.Comparator#compare(Object, Object)}.
	 * <p>
	 * This method may de-serialize the records or compare them directly based on their binary representation. 
	 * 
	 * @param firstSource The input view containing the first record.
	 * @param secondSource The input view containing the second record.
	 * @return An integer defining the oder among the objects in the same way as {@link java.util.Comparator#compare(Object, Object)}.
	 * @throws IOException Thrown, if any of the input views raised an exception when reading the records.
	 * 
	 *  @see java.util.Comparator#compare(Object, Object)
	 */
	public abstract int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Checks whether the data type supports the creation of a normalized key for comparison.
	 * 
	 * @return True, if the data type supports the creation of a normalized key for comparison, false otherwise.
	 */
	public abstract boolean supportsNormalizedKey();
	
	/**
	 * Check whether this comparator supports to serialize the record in a format that replaces its keys by a normalized
	 * key.
	 * 
	 * @return True, if the comparator supports that specific form of serialization, false if not.
	 */
	public abstract boolean supportsSerializationWithKeyNormalization();

	/**
	 * Gets the number of bytes that the normalized key would maximally take. A value of
	 * {@link java.lang.Integer}.MAX_VALUE is interpreted as infinite.
	 * 
	 * @return The number of bytes that the normalized key would maximally take.
	 */
	public abstract int getNormalizeKeyLen();
	
	/**
	 * Checks, whether the given number of bytes for a normalized is only a prefix to determine the order of elements
	 * of the data type for which this comparator provides the comparison methods. For example, if the
	 * data type is ordered with respect to an integer value it contains, then this method would return
	 * true, if the number of key bytes is smaller than four.
	 * 
	 * @return True, if the given number of bytes is only a prefix,
	 *         false otherwise.
	 */
	public abstract boolean isNormalizedKeyPrefixOnly(int keyBytes);
	
	/**
	 * Writes a normalized key for the given record into the target byte array, starting at the specified position
	 * and writing exactly the given number of bytes. Note that the comparison of the bytes is treating the bytes
	 * as unsigned bytes: {@code int byteI = bytes[i] & 0xFF;}
	 * <p>
	 * If the meaningful part of the normalized key takes less than the given number of bytes, than it must be padded.
	 * Padding is typically required for variable length data types, such as strings. The padding uses a special
	 * character, either {@code 0} or {@code 0xff}, depending on whether shorter values are sorted to the beginning or
	 * the end. 
	 * <p>
	 * This method is similar to {@link org.apache.flink.types.NormalizableKey#copyNormalizedKey(MemorySegment, int, int)}. In the case that
	 * multiple fields of a record contribute to the normalized key, it is crucial that the fields align on the
	 * byte field, i.e. that every field always takes up the exact same number of bytes.
	 * 
	 * @param record The record for which to create the normalized key.
	 * @param target The byte array into which to write the normalized key bytes.
	 * @param offset The offset in the byte array, where to start writing the normalized key bytes.
	 * @param numBytes The number of bytes to be written exactly. 
	 * 
	 * @see org.apache.flink.types.NormalizableKey#copyNormalizedKey(MemorySegment, int, int)
	 */
	public abstract void putNormalizedKey(T record, MemorySegment target, int offset, int numBytes);

	/**
	 * Writes the record in such a fashion that all keys are normalizing and at the beginning of the serialized data.
	 * This must only be used when for all the key fields the full normalized key is used. The method
	 * {@code #supportsSerializationWithKeyNormalization()} allows to check that.
	 *
	 * @param record The record object into which to read the record data.
	 * @param target The stream to which to write the data,
	 *
	 * @see #supportsSerializationWithKeyNormalization()
	 * @see #readWithKeyDenormalization(Object, DataInputView)
	 * @see org.apache.flink.types.NormalizableKey#copyNormalizedKey(MemorySegment, int, int)
	 */
	public abstract void writeWithKeyNormalization(T record, DataOutputView target) throws IOException;
	
	/**
	 * Reads the record back while de-normalizing the key fields. This must only be used when
	 * for all the key fields the full normalized key is used, which is hinted by the
	 * {@code #supportsSerializationWithKeyNormalization()} method.
	 *
	 * @param reuse The reuse object into which to read the record data.
	 * @param source The stream from which to read the data,
	 *
	 * @see #supportsSerializationWithKeyNormalization()
	 * @see #writeWithKeyNormalization(Object, DataOutputView)
	 * @see org.apache.flink.types.NormalizableKey#copyNormalizedKey(MemorySegment, int, int)
	 */
	public abstract T readWithKeyDenormalization(T reuse, DataInputView source) throws IOException;

	/**
	 * Flag whether normalized key comparisons should be inverted key should be interpreted
	 * inverted, i.e. descending.
	 * 
	 * @return True, if all normalized key comparisons should invert the sign of the comparison result,
	 *         false if the normalized key should be used as is.
	 */
	public abstract boolean invertNormalizedKey();
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a copy of this class. The copy must be deep such that no state set in the copy affects this
	 * instance of the comparator class.
	 * 
	 * @return A deep copy of this comparator instance.
	 */
	public abstract TypeComparator<T> duplicate();
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Extracts the key fields from a record. This is for use by the PairComparator to provide
	 * interoperability between different record types. Note, that at least one key should be extracted.
	 * @param record The record that contains the key(s)
	 * @param target The array to write the key(s) into.
	 * @param index The offset of the target array to start writing into.
	 * @return the number of keys added to target.
	 */
	public abstract int extractKeys(Object record, Object[] target, int index);

	/**
	 * Get the field comparators. This is used together with {@link #extractKeys(Object, Object[], int)}
	 * to provide interoperability between different record types. Note, that this should return at
	 * least one Comparator and that the number of Comparators must match the number of extracted
	 * keys.
	 * @return An Array of Comparators for the extracted keys.
	 */
	@SuppressWarnings("rawtypes")
	public abstract TypeComparator[] getFlatComparators();

	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("rawtypes")
	public int compareAgainstReference(Comparable[] keys) {
		throw new UnsupportedOperationException("Workaround hack.");
	}
}
