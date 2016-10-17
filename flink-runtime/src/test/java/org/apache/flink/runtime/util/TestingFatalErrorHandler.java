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

import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Testing fatal error handler which records the occurred exceptions during the execution of the
 * tests. Captured exceptions are thrown as a {@link TestingException}.
 */
public class TestingFatalErrorHandler implements FatalErrorHandler {
	private static final Logger LOG = LoggerFactory.getLogger(TestingFatalErrorHandler.class);
	private final AtomicReference<Throwable> atomicThrowable;

	public TestingFatalErrorHandler() {
		atomicThrowable = new AtomicReference<>(null);
	}

	public void rethrowError() throws TestingException {
		Throwable throwable = atomicThrowable.get();

		if (throwable != null) {
			throw new TestingException(throwable);
		}
	}

	public boolean hasExceptionOccurred() {
		return atomicThrowable.get() != null;
	}

	public Throwable getException() {
		return atomicThrowable.get();
	}

	@Override
	public void onFatalError(Throwable exception) {
		LOG.error("OnFatalError:", exception);

		if (!atomicThrowable.compareAndSet(null, exception)) {
			atomicThrowable.get().addSuppressed(exception);
		}
	}

	//------------------------------------------------------------------
	// static utility classes
	//------------------------------------------------------------------

	private static final class TestingException extends Exception {
		public TestingException(String message) {
			super(message);
		}

		public TestingException(String message, Throwable cause) {
			super(message, cause);
		}

		public TestingException(Throwable cause) {
			super(cause);
		}

		private static final long serialVersionUID = -4648195335470914498L;
	}
}
