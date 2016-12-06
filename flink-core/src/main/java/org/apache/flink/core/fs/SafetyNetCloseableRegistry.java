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

import org.apache.flink.util.AbstractCloseableRegistry;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.WrappingProxyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
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
public class SafetyNetCloseableRegistry extends
		AbstractCloseableRegistry<WrappingProxyCloseable<? extends Closeable>,
				SafetyNetCloseableRegistry.PhantomDelegatingCloseableRef> {

	private static final Logger LOG = LoggerFactory.getLogger(SafetyNetCloseableRegistry.class);
	private final ReferenceQueue<WrappingProxyCloseable<? extends Closeable>> referenceQueue;
	private final Thread reaperThread;

	public SafetyNetCloseableRegistry() {
		super(new IdentityHashMap<Closeable, PhantomDelegatingCloseableRef>());
		this.referenceQueue = new ReferenceQueue<>();
		this.reaperThread = new CloseableReaperThread();
		reaperThread.start();
	}

	@Override
	protected void doRegister(
			WrappingProxyCloseable<? extends Closeable> wrappingProxyCloseable,
			Map<Closeable, PhantomDelegatingCloseableRef> closeableMap) throws IOException {

		Closeable innerCloseable = WrappingProxyUtil.stripProxy(wrappingProxyCloseable.getWrappedDelegate());

		if (null == innerCloseable) {
			return;
		}

		PhantomDelegatingCloseableRef phantomRef =
				new PhantomDelegatingCloseableRef(wrappingProxyCloseable, referenceQueue);

		closeableMap.put(innerCloseable, phantomRef);
	}

	@Override
	protected void doUnRegister(
			WrappingProxyCloseable<? extends Closeable> closeable,
			Map<Closeable, PhantomDelegatingCloseableRef> closeableMap) {

		Closeable innerCloseable = WrappingProxyUtil.stripProxy(closeable.getWrappedDelegate());

		if (null == innerCloseable) {
			return;
		}

		closeableMap.remove(innerCloseable);
	}

	/**
	 * Phantom reference to {@link WrappingProxyCloseable}.
	 */
	static final class PhantomDelegatingCloseableRef
			extends PhantomReference<WrappingProxyCloseable<? extends Closeable>>
			implements Closeable {

		private final Closeable innerCloseable;
		private final String debugString;

		public PhantomDelegatingCloseableRef(
				WrappingProxyCloseable<? extends Closeable> referent,
				ReferenceQueue<? super WrappingProxyCloseable<? extends Closeable>> q) {

			super(referent, q);
			this.innerCloseable = Preconditions.checkNotNull(WrappingProxyUtil.stripProxy(referent));
			this.debugString = referent.toString();
		}

		public Closeable getInnerCloseable() {
			return innerCloseable;
		}

		public String getDebugString() {
			return debugString;
		}

		@Override
		public void close() throws IOException {
			innerCloseable.close();
		}
	}

	/**
	 * Reaper runnable collects and closes leaking resources
	 */
	final class CloseableReaperThread extends Thread {

		public CloseableReaperThread() {
			super("CloseableReaperThread");
			this.running = false;
		}

		private volatile boolean running;

		@Override
		public void run() {
			this.running = true;
			try {
				List<PhantomDelegatingCloseableRef> closeableList = new LinkedList<>();
				while (running) {
					PhantomDelegatingCloseableRef oldRef = (PhantomDelegatingCloseableRef) referenceQueue.remove();
					synchronized (getSynchronizationLock()) {
						do {
							closeableList.add(oldRef);
							closeableToRef.remove(oldRef.getInnerCloseable());
						}
						while ((oldRef = (PhantomDelegatingCloseableRef) referenceQueue.poll()) != null);
					}

					// close outside the synchronized block in case this is blocking
					for (PhantomDelegatingCloseableRef closeableRef : closeableList) {
						IOUtils.closeQuietly(closeableRef);
						if (LOG.isDebugEnabled()) {
							LOG.debug("Closing unclosed resource: " + closeableRef.getDebugString());
						}
					}

					closeableList.clear();
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

	@Override
	public void close() throws IOException {
		super.close();
		reaperThread.interrupt();
	}
}
