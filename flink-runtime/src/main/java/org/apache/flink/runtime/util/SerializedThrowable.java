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
package org.apache.flink.runtime.util;

import com.google.common.base.Preconditions;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Utility class for dealing with serialized Throwables.
 * Needed to send around user-specific exception classes with Akka.
 */
public class SerializedThrowable extends Exception implements Serializable {
	private static final long serialVersionUID = 7284183123441947635L;
	private final byte[] serializedError;

	// The exception must not be (de)serialized with the class, as its
	// class may not be part of the system class loader.
	private transient Throwable cachedError;


	/**
	 * Create a new SerializedThrowable.
	 * @param error The exception to serialize.
	 */
	public SerializedThrowable(Throwable error) {
		Preconditions.checkNotNull(error, "The exception to serialize has to be set");
		this.cachedError = error;
		byte[] serializedError;
		try {
			serializedError = InstantiationUtil.serializeObject(error);
		}
		catch (Throwable t) {
			// could not serialize exception. send the stringified version instead
			try {
				this.cachedError = new Exception(ExceptionUtils.stringifyException(error));
				serializedError = InstantiationUtil.serializeObject(this.cachedError);
			}
			catch (Throwable tt) {
				// seems like we cannot do much to report the actual exception
				// report a placeholder instead
				try {
					this.cachedError = new Exception("Cause is a '" + error.getClass().getName()
							+ "' (failed to serialize or stringify)");
					serializedError = InstantiationUtil.serializeObject(this.cachedError);
				}
				catch (Throwable ttt) {
					// this should never happen unless the JVM is fubar.
					// we just report the state without the error
					this.cachedError = null;
					serializedError = null;
				}
			}
		}
		this.serializedError = serializedError;
	}

	public Throwable deserializeError(ClassLoader userCodeClassloader) {
		if (this.cachedError == null) {
			try {
				cachedError = (Throwable) InstantiationUtil.deserializeObject(this.serializedError, userCodeClassloader);
			}
			catch (Exception e) {
				throw new RuntimeException("Error while deserializing the attached exception", e);
			}
		}
		return this.cachedError;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof SerializedThrowable) {
			return Arrays.equals(this.serializedError, ((SerializedThrowable)obj).serializedError);
		}
		return false;
	}

	@Override
	public String toString() {
		if(cachedError != null) {
			return cachedError.getClass().getName() + ": " + cachedError.getMessage();
		}
		if(serializedError == null) {
			return "(null)"; // can not happen as per Ctor check.
		} else {
			return "(serialized)";
		}
	}

	public static Throwable get(Throwable serThrowable, ClassLoader loader) {
		if(serThrowable instanceof SerializedThrowable) {
			return ((SerializedThrowable)serThrowable).deserializeError(loader);
		} else {
			return serThrowable;
		}
	}
}
