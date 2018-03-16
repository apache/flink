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

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.IOUtils;

import java.net.URI;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The FileSystemSafetyNet can be used to guard a thread against {@link FileSystem} stream resource leaks.
 * When activated for a thread, it tracks all streams that are opened by FileSystems that the thread
 * obtains. The safety net has a global cleanup hook that will close all streams that were
 * not properly closed.
 *
 * <p>The main thread of each Flink task, as well as the checkpointing thread are automatically guarded
 * by this safety net.
 *
 * <p><b>Important:</b> This safety net works only for streams created by Flink's FileSystem abstraction,
 * i.e., for {@code FileSystem} instances obtained via {@link FileSystem#get(URI)} or through
 * {@link Path#getFileSystem()}.
 *
 * <p><b>Important:</b> When a guarded thread obtains a {@code FileSystem} or a stream and passes them
 * to another thread, the safety net will close those resources once the former thread finishes.
 *
 * <p>The safety net can be used as follows:
 * <pre>{@code
 *
 * class GuardedThread extends Thread {
 *
 *     public void run() {
 *         FileSystemSafetyNet.initializeSafetyNetForThread();
 *         try {
 *             // do some heavy stuff where you are unsure whether it closes all streams
 *             // like some untrusted user code or library code
 *         }
 *         finally {
 *             FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
 *         }
 *     }
 * }
 * }</pre>
 */
@Internal
public class FileSystemSafetyNet {

	/** The map from thread to the safety net registry for that thread. */
	private static final ThreadLocal<SafetyNetCloseableRegistry> REGISTRIES = new ThreadLocal<>();

	// ------------------------------------------------------------------------
	//  Activating / Deactivating
	// ------------------------------------------------------------------------

	/**
	 * Activates the safety net for a thread. {@link FileSystem} instances obtained by the thread
	 * that called this method will be guarded, meaning that their created streams are tracked and can
	 * be closed via the safety net closing hook.
	 *
	 * <p>This method should be called at the beginning of a thread that should be guarded.
	 *
	 * @throws IllegalStateException Thrown, if a safety net was already registered for the thread.
	 */
	@Internal
	public static void initializeSafetyNetForThread() {
		SafetyNetCloseableRegistry oldRegistry = REGISTRIES.get();

		checkState(null == oldRegistry, "Found an existing FileSystem safety net for this thread: %s " +
				"This may indicate an accidental repeated initialization, or a leak of the" +
				"(Inheritable)ThreadLocal through a ThreadPool.", oldRegistry);

		SafetyNetCloseableRegistry newRegistry = new SafetyNetCloseableRegistry();
		REGISTRIES.set(newRegistry);
	}

	/**
	 * Closes the safety net for a thread. This closes all remaining unclosed streams that were opened
	 * by safety-net-guarded file systems. After this method was called, no streams can be opened any more
	 * from any FileSystem instance that was obtained while the thread was guarded by the safety net.
	 *
	 * <p>This method should be called at the very end of a guarded thread.
	 */
	@Internal
	public static void closeSafetyNetAndGuardedResourcesForThread() {
		SafetyNetCloseableRegistry registry = REGISTRIES.get();
		if (null != registry) {
			REGISTRIES.remove();
			IOUtils.closeQuietly(registry);
		}
	}

	/**
	 * Returns the active safety-net registry for the current thread.
	 * @deprecated This method should be removed after FLINK-6684 is implemented.
	 */
	@Deprecated
	@Internal
	public static SafetyNetCloseableRegistry getSafetyNetCloseableRegistryForThread() {
		return REGISTRIES.get();
	}

	/**
	 * Sets the active safety-net registry for the current thread.
	 */
	@Internal
	public static void setSafetyNetCloseableRegistryForThread(SafetyNetCloseableRegistry registry) {
		REGISTRIES.set(registry);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	static FileSystem wrapWithSafetyNetWhenActivated(FileSystem fs) {
		SafetyNetCloseableRegistry reg = REGISTRIES.get();
		return reg != null ? new SafetyNetWrapperFileSystem(fs, reg) : fs;
	}
}
