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


/**
 * The base interface for normalizable keys. Normalizable keys can create a binary representation
 * of themselves that is byte-wise comparable. The byte-wise comparison of two normalized keys 
 * proceeds until all bytes are compared or two bytes at the corresponding positions are not equal.
 * If two corresponding byte values are not equal, the lower byte value indicates the lower key.
 * If both normalized keys are byte-wise identical, the actual key may have to be looked at to
 * determine which one is actually lower.
 * <p>
 * The latter depends on whether the normalized key covers the entire key or is just a prefix of the
 * key. A normalized key is considered a prefix, if its length is less than the maximal normalized
 * key length.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface NormalizableKey
{
	/**
	 * Gets the maximal length of normalized keys produced by this data type.
	 * 
	 * @return The maximal length of normalized keys.
	 */
	int getMaxNormalizedKeyLen();
	
	/**
	 * Puts the bytes of the normalized key into the given byte array.
	 * 
	 * @param target The byte array to put the normalized key bytes into.
	 * @param offset The offset in the byte array where the normalized key's bytes should start.
	 * @param len The number of bytes to put.
	 */
	void copyNormalizedKey(byte[] target, int offset, int len);
}
