/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.plugable;

import java.io.IOException;

import eu.stratosphere.nephele.services.memorymanager.DataInputViewV2;
import eu.stratosphere.nephele.services.memorymanager.DataOutputViewV2;


/**
 * This interface describes the methods that are required for a data type to be handled by the pact
 * runtime. For every data type, a utility class implementing this interface is required.
 *
 * @author Stephan Ewen
 */
public interface TypeAccessors<T>
{
	/**
	 * Creates a new instance of the data type.
	 * 
	 * @return A new instance of the data type.
	 */
	public T createInstance();
	
	/**
	 * Creates a copy from the given element.
	 * 
	 * @param from The element to copy.
	 * @return A copy of the given element.
	 */
	public T createCopy(T from);
	
	/**
	 * Creates a copy from the given element, storing the copied result in the given target element.
	 * 
	 * @param from The element to be copied.
	 * @param to The target element.
	 */
	public void copyTo(T from, T to);
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the length of the data type, if it is a fix length data type.
	 * 
	 * @return The length of the data type, or <code>-1</code> for variable length data types.
	 */
	public int getLength();
	
	// --------------------------------------------------------------------------------------------

	public long serialize(T record, DataOutputViewV2 target) throws IOException;
	
	public void deserialize(T target, DataInputViewV2 source) throws IOException;
	
	public void copy(DataInputViewV2 source, DataOutputViewV2 target) throws IOException;
	
	// --------------------------------------------------------------------------------------------
	
	public int hash(T object);
	
	public void setReference(T toCompare);
	
	public boolean equalToReference(T candidate);
	
	/**
	 * This method the element set as reference in this type accessor two the
	 * element set as reference in the given type accessor. Similar to comparing two
	 * elements {@code e1} and {@code e2} via a comparator, this method can be used the
	 * following way.
	 * 
	 * <pre>
	 * E e1 = ...;
	 * E e2 = ...;
	 * 
	 * TypeAccessors<E> acc1 = ...;
	 * TypeAccessors<E> acc2 = ...;
	 * 
	 * acc1.setReference(e1);
	 * acc2.setReference(e2);
	 * 
	 * int comp = acc1.compareToReference(acc2);
	 * </pre>
	 * 
	 * The rational behind this method is that elements are typically compared using certain features that
	 * are extracted from them, (such deserializing as a subset of fields). When setting the
	 * reference, this extraction happens. The extraction needs happen only once per element,
	 * even though an element is typically compared to many other elements when establishing a
	 * sorted order. The actual comparison performed by this method may be very cheap, as it
	 * happens on the extracted features.
	 * 
	 * @param referencedAccessors The type accessors where the element for comparison has been set
	 *                            as reference.
	 * 
	 * @return A value smaller than zero, if the reference value of {@code referencedAccessors} is smaller
	 *         than the reference value of this type accessor; a value greater than zero, if it is larger;
	 *         zero, if both are equal.
	 */
	public int compareToReference(TypeAccessors<T> referencedAccessors);
	
	public int compare(DataInputViewV2 firstSource, DataInputViewV2 secondSource) throws IOException;
	
	// --------------------------------------------------------------------------------------------
	
	public boolean supportsNormalizedKey();
	
	public int getNormalizeKeyLen();
	
	public boolean isNormalizedKeyPrefixOnly(int keyBytes);
	
	public void putNormalizedKey(T record, byte[] target, int offset, int numBytes);
	
	// --------------------------------------------------------------------------------------------
	
	public TypeAccessors<T> duplicate();
}
