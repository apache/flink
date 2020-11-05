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

package org.apache.flink.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

/**
 * This signal handler / signal logger is based on Apache Hadoop's org.apache.hadoop.util.SignalLogger.
 *
 * <p>This is a reduced version of {@link org.apache.flink.runtime.util.SignalHandler}.
 */
public class TestSignalHandler {

	private static final Logger LOG = LoggerFactory.getLogger(TestSignalHandler.class);

	private static boolean registered = false;

	/**
	 * Our signal handler.
	 */
	private static class Handler implements sun.misc.SignalHandler {

		private final sun.misc.SignalHandler prevHandler;

		Handler(String name) {
			prevHandler = Signal.handle(new Signal(name), this);
		}

		/**
		 * Handle an incoming signal.
		 *
		 * @param signal    The incoming signal
		 */
		@Override
		public void handle(Signal signal) {
			LOG.warn("RECEIVED SIGNAL {}: SIG{}. Shutting down as requested.",
				signal.getNumber(),
				signal.getName());
			prevHandler.handle(signal);
		}
	}

	/**
	 * Register some signal handlers.
	 */
	public static void register() {
		synchronized (TestSignalHandler.class) {
			if (registered) {
				return;
			}
			registered = true;

			final String[] signals = System.getProperty("os.name").startsWith("Windows")
				? new String[]{"TERM", "INT"}
				: new String[]{"TERM", "HUP", "INT"};

			for (String signalName : signals) {
				try {
					new Handler(signalName);
				} catch (Exception e) {
					LOG.info("Error while registering signal handler", e);
				}
			}
		}
	}
}
