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

package eu.stratosphere.nephele.execution;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.template.AbstractInvokable;

/**
 * An execution signature allows to uniquely identify a job vertex. The signature involves the name of the
 * class to be invoked at runtime as well as a cryptographic hash of all the JAR files which are required to
 * instantiate the class and run it. The execution is the basis for feedback learning as it enables the profiler
 * to recognize particular parts of a job. Execution signature objects are immutable and, consequently, thread-safe.
 * 
 * @author warneke
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
	private static MessageDigest messageDigest = null;

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
	 * @param jobID
	 *        the ID of the job
	 * @return the cryptographic signature of this vertex
	 */
	public static ExecutionSignature createSignature(final Class<? extends AbstractInvokable> invokableClass,
			final JobID jobID) {

		// First, try to load message digest algorithm, if necessary
		if (messageDigest == null) {
			try {
				messageDigest = MessageDigest.getInstance(HASHINGALGORITHM);
			} catch (NoSuchAlgorithmException e) {
				LOG.error("Unable to load message digest algorithm " + HASHINGALGORITHM);
				return null;
			}
		}

		// Reset digest buffer and add the name of the invokable class to the message digest buffer
		messageDigest.reset();
		messageDigest.update(invokableClass.getName().getBytes());

		String[] requiredJarFiles;
		// Next, retrieve the JAR-files associated with this job
		try {
			requiredJarFiles = LibraryCacheManager.getRequiredJarFiles(jobID);
		} catch (IOException ioe) {
			// Output an error message and return
			LOG.error("Cannot access library cache manager for job ID " + jobID);
			return null;
		}

		// Now, sort the list of JAR-files in order to always calculate the signature in the same manner
		Arrays.sort(requiredJarFiles);

		// Finally, add the names of the JAR-files to the hash calculation
		for (int i = 0; i < requiredJarFiles.length; i++) {
			messageDigest.update(requiredJarFiles[i].getBytes());
		}

		return new ExecutionSignature(messageDigest.digest());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {

		if (obj instanceof ExecutionSignature) {

			final ExecutionSignature executionSignature = (ExecutionSignature) obj;
			return Arrays.equals(this.signature, executionSignature.signature);
		}

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		int hashCode = 0;

		for (int i = 0; i < this.signature.length; i++) {
			hashCode += this.signature[i];
		}

		return hashCode;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		final StringBuffer stringBuffer = new StringBuffer();
		for (int i = 0; i < this.signature.length; i++) {
			stringBuffer.append(Integer.toHexString(0xFF & this.signature[i]));
		}

		return stringBuffer.toString();
	}
}
