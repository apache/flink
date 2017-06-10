/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.AbstractCloseableRegistry;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.WrappingProxyUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * This implementation of an {@link AbstractCloseableRegistry} registers {@link WrappingProxyCloseable}. When
 * the proxy becomes subject to GC, this registry takes care of closing unclosed {@link Closeable}s.
 * <p>
 * Phantom references are used to track when {@link org.apache.flink.util.WrappingProxy}s of {@link Closeable} got
 * GC'ed. We ensure that the wrapped {@link Closeable} is properly closed to avoid resource leaks.
 * <p>
 * Other than that, it works like a normal {@link CloseableRegistry}.
 * <p>
 * All methods in this class are thread-safe.
 */
@Internal
public class SafetyNetCloseableRegistry extends
		AbstractCloseableRegistry<WrappingProxyCloseable<? extends Closeable>,
				SafetyNetCloseableRegistry.PhantomDelegatingCloseableRef> {

	private static final Logger LOG = LoggerFactory.getLogger(SafetyNetCloseableRegistry.class);

	/** Lock for accessing reaper thread and registry count */
	private static final Object REAPER_THREAD_LOCK = new Object();

	/** Singleton reaper thread takes care of all registries in VM */
	@GuardedBy("REAPER_THREAD_LOCK")
	private static CloseableReaperThread REAPER_THREAD = null;

	/** Global count of all instances of SafetyNetCloseableRegistry */
	@GuardedBy("REAPER_THREAD_LOCK")
	private static int GLOBAL_SAFETY_NET_REGISTRY_COUNT = 0;

	public SafetyNetCloseableRegistry() {
		super(new IdentityHashMap<Closeable, PhantomDelegatingCloseableRef>());

		synchronized (REAPER_THREAD_LOCK) {
			if (0 == GLOBAL_SAFETY_NET_REGISTRY_COUNT) {
				Preconditions.checkState(null == REAPER_THREAD);
				REAPER_THREAD = new CloseableReaperThread();
				REAPER_THREAD.start();
			}
			++GLOBAL_SAFETY_NET_REGISTRY_COUNT;
		}
	}

	@Override
	protected void doRegister(
			WrappingProxyCloseable<? extends Closeable> wrappingProxyCloseable,
			Map<Closeable, PhantomDelegatingCloseableRef> closeableMap) throws IOException {

		assert Thread.holdsLock(getSynchronizationLock());

		Closeable innerCloseable = WrappingProxyUtil.stripProxy(wrappingProxyCloseable.getWrappedDelegate());

		if (null == innerCloseable) {
			return;
		}

		PhantomDelegatingCloseableRef phantomRef = new PhantomDelegatingCloseableRef(
				wrappingProxyCloseable,
				this,
				REAPER_THREAD.referenceQueue);

		closeableMap.put(innerCloseable, phantomRef);
	}

	@Override
	protected void doUnRegister(
			WrappingProxyCloseable<? extends Closeable> closeable,
			Map<Closeable, PhantomDelegatingCloseableRef> closeableMap) {

		assert Thread.holdsLock(getSynchronizationLock());

		Closeable innerCloseable = WrappingProxyUtil.stripProxy(closeable.getWrappedDelegate());

		if (null == innerCloseable) {
			return;
		}

		closeableMap.remove(innerCloseable);
	}

	@Override
	public void close() throws IOException {
		try {
			super.close();
		}
		finally {
			synchronized (REAPER_THREAD_LOCK) {
				--GLOBAL_SAFETY_NET_REGISTRY_COUNT;
				if (0 == GLOBAL_SAFETY_NET_REGISTRY_COUNT) {
					REAPER_THREAD.interrupt();
					REAPER_THREAD = null;
				}
			}
		}
	}

	@VisibleForTesting
	public static boolean isReaperThreadRunning() {
		synchronized (REAPER_THREAD_LOCK) {
			return null != REAPER_THREAD && REAPER_THREAD.isAlive();
		}
	}

	/**
	 * Phantom reference to {@link WrappingProxyCloseable}.
	 */
	static final class PhantomDelegatingCloseableRef
			extends PhantomReference<WrappingProxyCloseable<? extends Closeable>>
			implements Closeable {

		private final Closeable innerCloseable;
		private final SafetyNetCloseableRegistry closeableRegistry;
		private final String debugString;

		public PhantomDelegatingCloseableRef(
				WrappingProxyCloseable<? extends Closeable> referent,
				SafetyNetCloseableRegistry closeableRegistry,
				ReferenceQueue<? super WrappingProxyCloseable<? extends Closeable>> q) {

			super(referent, q);
			this.innerCloseable = Preconditions.checkNotNull(WrappingProxyUtil.stripProxy(referent));
			this.closeableRegistry = Preconditions.checkNotNull(closeableRegistry);
			this.debugString = referent.toString();
		}

		public String getDebugString() {
			return debugString;
		}

		@Override
		public void close() throws IOException {
			synchronized (closeableRegistry.getSynchronizationLock()) {
				closeableRegistry.closeableToRef.remove(innerCloseable);
			}
			innerCloseable.close();
		}
	}

	/**
	 * Reaper runnable collects and closes leaking resources
	 */
	static final class CloseableReaperThread extends Thread {

		private final ReferenceQueue<WrappingProxyCloseable<? extends Closeable>> referenceQueue;

		private volatile boolean running;

		private CloseableReaperThread() {
			super("CloseableReaperThread");
			this.setDaemon(true);

			this.referenceQueue = new ReferenceQueue<>();
			this.running = true;
		}

		@Override
		public void run() {
			try {
				while (running) {
					final PhantomDelegatingCloseableRef toClose = (PhantomDelegatingCloseableRef) referenceQueue.remove();
					
					if (toClose != null) {
						try {
							LOG.warn("Closing unclosed resource via safety-net: {}", toClose.getDebugString());
							toClose.close();
						}
						catch (Throwable t) {
							LOG.debug("Error while closing resource via safety-net", t);
						}
					}
				}
			} catch (InterruptedException e) {
				// done
			}
		}

		@Override
		public void interrupt() {
			this.running = false;
			super.interrupt();
		}
	}
}
