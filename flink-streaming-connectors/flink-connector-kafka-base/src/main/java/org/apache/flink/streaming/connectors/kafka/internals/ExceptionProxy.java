/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internals;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 
 */
public class ExceptionProxy {
	
	/** The thread that should be interrupted when an exception occurs */
	private final Thread toInterrupt;
	
	/** The exception to throw */ 
	private final AtomicReference<Throwable> exception;

	/**
	 * 
	 * @param toInterrupt The thread to interrupt upon an exception. May be null.
	 */
	public ExceptionProxy(@Nullable Thread toInterrupt) {
		this.toInterrupt = toInterrupt;
		this.exception = new AtomicReference<>();
	}
	
	// ------------------------------------------------------------------------
	
	/**
	 * Sets the exception occurred and interrupts the target thread,
	 * if no other exception has occurred so far.
	 * 
	 * @param t The exception that occurred
	 */
	public void reportError(Throwable t) {
		// set the exception, if it is the first
		if (exception.compareAndSet(null, t) && toInterrupt != null) {
			toInterrupt.interrupt();
		}
	}
	
	public void checkAndThrowException() throws Exception {
		Throwable t = exception.get();
		if (t != null) {
			if (t instanceof Exception) {
				throw (Exception) t;
			}
			else if (t instanceof Error) {
				throw (Error) t;
			}
			else {
				throw new Exception(t);
			}
		}
	}
}
