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

import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public final class ExceptionUtils {
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
			throw new IOException(t);
		}
	}

	// ------------------------------------------------------------------------

	/** Private constructor to prevent instantiation. */
	private ExceptionUtils() {}
}
