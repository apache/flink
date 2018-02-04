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

package org.apache.flink.runtime.state;

import org.apache.hadoop.util.StringUtils;

import java.io.ObjectStreamException;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A reference to a storage location. This is a wrapper around an array of bytes that
 * are subject to interpretation by the state backend's storage locations (similar as
 * a serializer needs to interpret byte streams). There is special handling for a
 * 'default location', which can be used as an optimization by state backends, when no
 * extra information is needed to determine where the checkpoints should be stored
 * (all information can be derived from the configuration and the checkpoint id).
 *
 * <h3>Why is this simply a byte array?</h3>
 *
 * <p>The reference is represented via raw bytes, which are subject to interpretation
 * by the state backends. We did not add any more typing and serialization abstraction
 * in between, because these types need to serialize/deserialize fast in between
 * network streams (byte buffers) and barriers. We may ultimately add some more typing
 * if we simply keep the byte buffers for the checkpoint barriers and forward them,
 * thus saving decoding and re-encoding these references repeatedly.
 */
public class CheckpointStorageLocationReference implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	/** The encoded location reference. null indicates the default location. */
	private final byte[] encodedReference;

	/**
	 * Creates a new location reference.
	 *
	 * @param encodedReference The location reference, represented as bytes (non null)
	 */
	public CheckpointStorageLocationReference(byte[] encodedReference) {
		checkNotNull(encodedReference);
		checkArgument(encodedReference.length > 0);

		this.encodedReference = encodedReference;
	}

	/**
	 * Private constructor for singleton only.
	 */
	private CheckpointStorageLocationReference() {
		this.encodedReference = null;
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the reference bytes.
	 *
	 * <p><b>Important:</b> For efficiency, this method does not make a defensive copy,
	 * so the caller must not modify the bytes in the array.
	 */
	public byte[] getReferenceBytes() {
		// return a non null object always
		return encodedReference != null ? encodedReference : new byte[0];
	}

	/**
	 * Returns true, if this object is the default reference.
	 */
	public boolean isDefaultReference() {
		return encodedReference == null;
	}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return encodedReference == null ? 2059243550 : Arrays.hashCode(encodedReference);
	}

	@Override
	public boolean equals(Object obj) {
		return obj == this ||
				obj != null && obj.getClass() == CheckpointStorageLocationReference.class &&
						Arrays.equals(encodedReference, ((CheckpointStorageLocationReference) obj).encodedReference);
	}

	@Override
	public String toString() {
		return encodedReference == null ? "(default)"
				: StringUtils.byteToHexString(encodedReference, 0, encodedReference.length);
	}

	/**
	 * readResolve() preserves the singleton property of the default value.
 	 */
	protected final Object readResolve() throws ObjectStreamException {
		return encodedReference == null ? DEFAULT : this;
	}

	// ------------------------------------------------------------------------
	//  Default Location Reference
	// ------------------------------------------------------------------------

	/** The singleton object for the default reference. */
	private static final CheckpointStorageLocationReference DEFAULT = new CheckpointStorageLocationReference();

	public static CheckpointStorageLocationReference getDefault() {
		return DEFAULT;
	}
}
