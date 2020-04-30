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

import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A utility that guards against blocking shutdown hooks that block JVM shutdown.
 * 
 * <p>When the JVM shuts down cleanly (<i>SIGTERM</i> or {@link System#exit(int)}) it runs
 * all installed shutdown hooks. It is possible that any of the shutdown hooks blocks,
 * which causes the JVM to get stuck and not exit at all.
 * 
 * <p>This utility installs a shutdown hook that forcibly terminates the JVM if it is still alive
 * a certain time after clean shutdown was initiated. Even if some shutdown hooks block, the JVM will
 * terminate within a certain time.
 */
public class JvmShutdownSafeguard extends Thread {

	/** Default delay to wait after clean shutdown was stared, before forcibly terminating the JVM */  
	private static final long DEFAULT_DELAY = 5000L;

	/** The exit code returned by the JVM process if it is killed by the safeguard */
	private static final int EXIT_CODE = -17;

	/** The thread that actually does the termination */
	private final Thread terminator;
	
	private JvmShutdownSafeguard(long delayMillis) {
		setName("JVM Terminator Launcher");

		this.terminator = new Thread(new DelayedTerminator(delayMillis), "Jvm Terminator");
		this.terminator.setDaemon(true);
	}

	@Override
	public void run() {
		// Because this thread is registered as a shutdown hook, we cannot
		// wait here and then call for termination. That would always delay the JVM shutdown.
		// Instead, we spawn a non shutdown hook thread from here. 
		// That thread is a daemon, so it does not keep the JVM alive.
		terminator.start();
	}

	// ------------------------------------------------------------------------
	//  The actual Shutdown thread
	// ------------------------------------------------------------------------

	private static class DelayedTerminator implements Runnable {

		private final long delayMillis;

		private DelayedTerminator(long delayMillis) {
			this.delayMillis = delayMillis;
		}

		@Override
		public void run() {
			try {
				Thread.sleep(delayMillis);
			}
			catch (Throwable t) {
				// catch all, including thread death, etc
			}

			Runtime.getRuntime().halt(EXIT_CODE);
		}
	} 

	// ------------------------------------------------------------------------
	//  Installing as a shutdown hook
	// ------------------------------------------------------------------------

	/**
	 * Installs the safeguard shutdown hook. The maximum time that the JVM is allowed to spend
	 * on shutdown before being killed is five seconds.
	 * 
	 * @param logger The logger to log errors to.
	 */
	public static void installAsShutdownHook(Logger logger) {
		installAsShutdownHook(logger, DEFAULT_DELAY);
	}

	/**
	 * Installs the safeguard shutdown hook. The maximum time that the JVM is allowed to spend
	 * on shutdown before being killed is the given number of milliseconds.
	 * 
	 * @param logger      The logger to log errors to.
	 * @param delayMillis The delay (in milliseconds) to wait after clean shutdown was stared,
	 *                    before forcibly terminating the JVM.
	 */
	public static void installAsShutdownHook(Logger logger, long delayMillis) {
		checkArgument(delayMillis >= 0, "delay must be >= 0");

		// install the blocking shutdown hook
		Thread shutdownHook = new JvmShutdownSafeguard(delayMillis);
		ShutdownHookUtil.addShutdownHookThread(shutdownHook, JvmShutdownSafeguard.class.getSimpleName(), logger);
	}
}
