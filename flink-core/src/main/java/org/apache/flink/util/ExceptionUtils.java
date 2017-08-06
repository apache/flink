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

//
// The function "stringifyException" is based on source code from the Hadoop Project (http://hadoop.apache.org/),
// licensed by the Apache Software Foundation (ASF) under the Apache License, Version 2.0.
// See the NOTICE file distributed with this work for additional information regarding copyright ownership. 
//

package org.apache.flink.util;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A collection of utility functions for dealing with exceptions and exception workflows.
 */
@Internal
public final class ExceptionUtils {

	/** The stringified representation of a null exception reference */ 
	public static final String STRINGIFIED_NULL_EXCEPTION = "(null)";

	/**
	 * Makes a string representation of the exception's stack trace, or "(null)", if the
	 * exception is null.
	 * 
	 * This method makes a best effort and never fails.
	 * 
	 * @param e The exception to stringify.
	 * @return A string with exception name and call stack.
	 */
	public static String stringifyException(final Throwable e) {
		if (e == null) {
			return STRINGIFIED_NULL_EXCEPTION;
		}
		
		try {
			StringWriter stm = new StringWriter();
			PrintWriter wrt = new PrintWriter(stm);
			e.printStackTrace(wrt);
			wrt.close();
			return stm.toString();
		}
		catch (Throwable t) {
			return e.getClass().getName() + " (error while printing stack trace)";
		}
	}

	/**
	 * Checks whether the given exception indicates a situation that may leave the
	 * JVM in a corrupted state, meaning a state where continued normal operation can only be
	 * guaranteed via clean process restart.
	 *
	 * <p>Currently considered fatal exceptions are Virtual Machine errors indicating
	 * that the JVM is corrupted, like {@link InternalError}, {@link UnknownError},
	 * and {@link java.util.zip.ZipError} (a special case of InternalError).
	 *
	 * @param t The exception to check.
	 * @return True, if the exception is considered fatal to the JVM, false otherwise.
	 */
	public static boolean isJvmFatalError(Throwable t) {
		return (t instanceof InternalError) || (t instanceof UnknownError);
	}

	/**
	 * Checks whether the given exception indicates a situation that may leave the
	 * JVM in a corrupted state, or an out-of-memory error.
	 * 
	 * <p>See {@link ExceptionUtils#isJvmFatalError(Throwable)} for a list of fatal JVM errors.
	 * This method additionally classifies the {@link OutOfMemoryError} as fatal, because it
	 * may occur in any thread (not the one that allocated the majority of the memory) and thus
	 * is often not recoverable by destroying the particular thread that threw the exception.
	 * 
	 * @param t The exception to check.
	 * @return True, if the exception is fatal to the JVM or and OutOfMemoryError, false otherwise.
	 */
	public static boolean isJvmFatalOrOutOfMemoryError(Throwable t) {
		return isJvmFatalError(t) || t instanceof OutOfMemoryError;
	}

	/**
	 * Rethrows the given {@code Throwable}, if it represents an error that is fatal to the JVM.
	 * See {@link ExceptionUtils#isJvmFatalError(Throwable)} for a definition of fatal errors.
	 * 
	 * @param t The Throwable to check and rethrow.
	 */
	public static void rethrowIfFatalError(Throwable t) {
		if (isJvmFatalError(t)) {
			throw (Error) t;
		}
	}

	/**
	 * Rethrows the given {@code Throwable}, if it represents an error that is fatal to the JVM
	 * or an out-of-memory error. See {@link ExceptionUtils#isJvmFatalError(Throwable)} for a
	 * definition of fatal errors.
	 *
	 * @param t The Throwable to check and rethrow.
	 */
	public static void rethrowIfFatalErrorOrOOM(Throwable t) {
		if (isJvmFatalError(t) || t instanceof OutOfMemoryError) {
			throw (Error) t;
		}
	}

	/**
	 * Adds a new exception as a {@link Throwable#addSuppressed(Throwable) suppressed exception}
	 * to a prior exception, or returns the new exception, if no prior exception exists.
	 *
	 * <pre>{@code
	 *
	 * public void closeAllThings() throws Exception {
	 *     Exception ex = null;
	 *     try {
	 *         component.shutdown();
	 *     } catch (Exception e) {
	 *         ex = firstOrSuppressed(e, ex);
	 *     }
	 *     try {
	 *         anotherComponent.stop();
	 *     } catch (Exception e) {
	 *         ex = firstOrSuppressed(e, ex);
	 *     }
	 *     try {
	 *         lastComponent.shutdown();
	 *     } catch (Exception e) {
	 *         ex = firstOrSuppressed(e, ex);
	 *     }
	 *
	 *     if (ex != null) {
	 *         throw ex;
	 *     }
	 * }
	 * }</pre>
	 *
	 * @param newException The newly occurred exception
	 * @param previous     The previously occurred exception, possibly null.
	 *
	 * @return The new exception, if no previous exception exists, or the previous exception with the
	 *         new exception in the list of suppressed exceptions.
	 */
	public static <T extends Throwable> T firstOrSuppressed(T newException, @Nullable T previous) {
		checkNotNull(newException, "newException");

		if (previous == null) {
			return newException;
		} else {
			previous.addSuppressed(newException);
			return previous;
		}
	}

	/**
	 * Throws the given {@code Throwable} in scenarios where the signatures do not allow you to
	 * throw an arbitrary Throwable. Errors and RuntimeExceptions are thrown directly, other exceptions
	 * are packed into runtime exceptions
	 * 
	 * @param t The throwable to be thrown.
	 */
	public static void rethrow(Throwable t) {
		if (t instanceof Error) {
			throw (Error) t;
		}
		else if (t instanceof RuntimeException) {
			throw (RuntimeException) t;
		}
		else {
			throw new RuntimeException(t);
		}
	}
	
	/**
	 * Throws the given {@code Throwable} in scenarios where the signatures do not allow you to
	 * throw an arbitrary Throwable. Errors and RuntimeExceptions are thrown directly, other exceptions
	 * are packed into a parent RuntimeException.
	 * 
	 * @param t The throwable to be thrown.
	 * @param parentMessage The message for the parent RuntimeException, if one is needed.
	 */
	public static void rethrow(Throwable t, String parentMessage) {
		if (t instanceof Error) {
			throw (Error) t;
		}
		else if (t instanceof RuntimeException) {
			throw (RuntimeException) t;
		}
		else {
			throw new RuntimeException(parentMessage, t);
		}
	}

	/**
	 * Throws the given {@code Throwable} in scenarios where the signatures do allow to
	 * throw a Exception. Errors and Exceptions are thrown directly, other "exotic"
	 * subclasses of Throwable are wrapped in an Exception.
	 *
	 * @param t The throwable to be thrown.
	 * @param parentMessage The message for the parent Exception, if one is needed.
	 */
	public static void rethrowException(Throwable t, String parentMessage) throws Exception {
		if (t instanceof Error) {
			throw (Error) t;
		}
		else if (t instanceof Exception) {
			throw (Exception) t;
		}
		else {
			throw new Exception(parentMessage, t);
		}
	}

	/**
	 * Tries to throw the given {@code Throwable} in scenarios where the signatures allows only IOExceptions
	 * (and RuntimeException and Error). Throws this exception directly, if it is an IOException,
	 * a RuntimeException, or an Error. Otherwise does nothing.
	 *
	 * @param t The Throwable to be thrown.
	 */
	public static void tryRethrowIOException(Throwable t) throws IOException {
		if (t instanceof IOException) {
			throw (IOException) t;
		}
		else if (t instanceof RuntimeException) {
			throw (RuntimeException) t;
		}
		else if (t instanceof Error) {
			throw (Error) t;
		}
	}

	/**
	 * Re-throws the given {@code Throwable} in scenarios where the signatures allows only IOExceptions
	 * (and RuntimeException and Error).
	 * 
	 * Throws this exception directly, if it is an IOException, a RuntimeException, or an Error. Otherwise it 
	 * wraps it in an IOException and throws it.
	 * 
	 * @param t The Throwable to be thrown.
	 */
	public static void rethrowIOException(Throwable t) throws IOException {
		if (t instanceof IOException) {
			throw (IOException) t;
		}
		else if (t instanceof RuntimeException) {
			throw (RuntimeException) t;
		}
		else if (t instanceof Error) {
			throw (Error) t;
		}
		else {
			throw new IOException(t.getMessage(), t);
		}
	}

	/**
	 * Checks whether a throwable chain contains a specific type of exception.
	 *
	 * @param throwable the throwable chain to check.
	 * @param searchType the type of exception to search for in the chain.
	 * @return True, if the searched type is nested in the throwable, false otherwise.
	 */
	public static boolean containsThrowable(Throwable throwable, Class<?> searchType) {
		if (throwable == null || searchType == null) {
			return false;
		}

		Throwable t = throwable;
		while (t != null) {
			if (searchType.isAssignableFrom(t.getClass())) {
				return true;
			} else {
				t = t.getCause();
			}
		}

		return false;
	}

	/**
	 * Unpacks an {@link ExecutionException} and returns its cause. Otherwise the given
	 * Throwable is returned.
	 *
	 * @param throwable to unpack if it is an ExecutionException
	 * @return Cause of ExecutionException or given Throwable
	 */
	public static Throwable stripExecutionException(Throwable throwable) {
		while (throwable instanceof ExecutionException && throwable.getCause() != null) {
			throwable = throwable.getCause();
		}

		return throwable;
	}

	/**
	 * Tries to find a {@link SerializedThrowable} as the cause of the given throwable and throws its
	 * deserialized value. If there is no such throwable, then the original throwable is thrown.
	 *
	 * @param throwable to check for a SerializedThrowable
	 * @param classLoader to be used for the deserialization of the SerializedThrowable
	 * @throws Throwable either the deserialized throwable or the given throwable
	 */
	public static void tryDeserializeAndThrow(Throwable throwable, ClassLoader classLoader) throws Throwable {
		Throwable current = throwable;

		while (!(current instanceof SerializedThrowable) && current.getCause() != null) {
			current = current.getCause();
		}

		if (current instanceof SerializedThrowable) {
			throw ((SerializedThrowable) current).deserializeError(classLoader);
		} else {
			throw throwable;
		}
	}

	// ------------------------------------------------------------------------

	/** Private constructor to prevent instantiation. */
	private ExceptionUtils() {}
}
