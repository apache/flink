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
 * The base interface for (de)normalizable keys. De-Normalizable keys have all
 * the properties of normalizable keys. In addition one can also retrieve the
 * original value from the normalized value.
 */
public interface DeNormalizableKey extends NormalizableKey {
	/**
	 * Reads a normalized key from the source byte array into this record while
	 * de-normalizing it.
	 * 
	 * @param source The byte array to read the normalized key bytes from.
	 * @param offset The offset in the byte array where the normalized key's bytes start.
	 * @param len The number of bytes to read.
	 */
	void readFromNormalizedKey(byte[] source, int offset, int len);
}
