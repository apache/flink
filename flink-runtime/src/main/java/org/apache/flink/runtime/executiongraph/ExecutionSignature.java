/**
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

package org.apache.flink.runtime.executiongraph;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

/**
 * An execution signature allows to uniquely identify a job vertex. The signature involves the name of the
 * class to be invoked at runtime as well as a cryptographic hash of all the JAR files which are required to
 * instantiate the class and run it. The execution is the basis for feedback learning as it enables the profiler
 * to recognize particular parts of a job. Execution signature objects are immutable and, consequently, thread-safe.
 * <p>
 * This class is thread-safe.
 */
public final class ExecutionSignature {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(ExecutionSignature.class);

	/**
	 * The name of the hashing algorithm to be used.
	 */
	private static final String HASHINGALGORITHM = "SHA-1";

	/**
	 * The message digest object used to calculate the signature.
	 */
	private static MessageDigest MESSAGE_DIGEST = null;

	/**
	 * The buffer storing the signature.
	 */
	private final byte[] signature;

	/**
	 * Constructs a new execution signature object and passes the signature buffer.
	 * 
	 * @param signature
	 *        the byte buffer containing the signature.
	 */
	private ExecutionSignature(final byte[] signature) {
		this.signature = signature;
	}

	/**
	 * Calculates the execution signature from the given class name and job ID.
	 * 
	 * @param invokableClass
	 *        the name of the class to contain the task program
	 * @param requiredJarFiles
	 *        + * list of BLOB keys referring to the JAR files required to run this job in lexicographic order
	 * @return the cryptographic signature of this vertex
	 */
	public static synchronized ExecutionSignature createSignature(
			final Class<? extends AbstractInvokable> invokableClass, final List<BlobKey> requiredJarFiles) {

		// First, try to load message digest algorithm, if necessary
		if (MESSAGE_DIGEST == null) {
			try {
				MESSAGE_DIGEST = MessageDigest.getInstance(HASHINGALGORITHM);
			} catch (NoSuchAlgorithmException e) {
				LOG.error("Unable to load message digest algorithm " + HASHINGALGORITHM);
				return null;
			}
		}

		// Reset digest buffer and add the name of the invokable class to the message digest buffer
		MESSAGE_DIGEST.reset();
		MESSAGE_DIGEST.update(invokableClass.getName().getBytes());

		for (final Iterator<BlobKey> it = requiredJarFiles.iterator(); it.hasNext();) {
			it.next().addToMessageDigest(MESSAGE_DIGEST);
		}

		return new ExecutionSignature(MESSAGE_DIGEST.digest());
	}

	@Override
	public boolean equals(final Object obj) {

		if (obj instanceof ExecutionSignature) {

			final ExecutionSignature executionSignature = (ExecutionSignature) obj;
			return Arrays.equals(this.signature, executionSignature.signature);
		}

		return false;
	}

	@Override
	public int hashCode() {

		int hashCode = 0;

		for (int i = 0; i < this.signature.length; i++) {
			hashCode += this.signature[i];
		}

		return hashCode;
	}

	@Override
	public String toString() {

		final StringBuffer stringBuffer = new StringBuffer();
		for (int i = 0; i < this.signature.length; i++) {
			stringBuffer.append(Integer.toHexString(0xFF & this.signature[i]));
		}

		return stringBuffer.toString();
	}
}
