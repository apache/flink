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
package org.apache.flink.runtime.checkpoint;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.checkpoint.CheckpointCache.CachedOutputStream;
import org.apache.flink.runtime.state.CachedStateHandle;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * It mainly has two member fields:
 * 1, cacheId {@link StateHandleID}
 * 2, remoteHandle {@link StreamStateHandle}
 * When building the input stream via {@link CachedStreamStateHandle}, it first try to build the local input stream from {@link CheckpointCache}
 * based on the cache id, and use remoteHandle to build the input stream from the remote end only when building the local input stream failed.
 */
public class CachedStreamStateHandle implements StreamStateHandle, CachedStateHandle {

	private static final long serialVersionUID = 350284443258002366L;

	private static Logger LOG = LoggerFactory.getLogger(CachedStreamStateHandle.class);

	private transient CheckpointCache cache;
	private transient boolean reCache;

	private final StateHandleID cacheId;
	private final StreamStateHandle remoteHandle;

	public CachedStreamStateHandle(StateHandleID cacheId, StreamStateHandle remoteHandle) {
		this.cacheId = cacheId;
		this.remoteHandle = remoteHandle;
	}

	@Override
	public FSDataInputStream openInputStream() throws IOException {
		FSDataInputStream in = cache.openInputStream(cacheId);
		if (in != null) {
			LOG.info("Open the local input stream.");
			return in;
		}

		CachedOutputStream output = null;
		if (reCache) {
			output = cache.createOutputStream(CheckpointCache.CHECKPOINT_ID_FOR_RESTORE, cacheId);
		}
		LOG.info("Open the remote input stream, re-cache: {}.", reCache);
		return new CachedInputStream(remoteHandle.openInputStream(), output);
	}

	@Override
	public void discardState() throws Exception {
		remoteHandle.discardState();
	}

	@Override
	public long getStateSize() {
		return remoteHandle.getStateSize();
	}

	public StreamStateHandle getRemoteHandle() {
		return this.remoteHandle;
	}

	public void setCheckpointCache(CheckpointCache cache) {
		this.cache = cache;
	}

	public void reCache(boolean reCache) {
		this.reCache = reCache;
	}

	@Override
	public StateHandleID getStateHandleId() {
		return cacheId;
	}

	public static class CachedInputStream extends FSDataInputStream {

		private final FSDataInputStream remoteInputStream ;
		private final CachedOutputStream cacheOut;

		public CachedInputStream(FSDataInputStream fsDataInputStream, CachedOutputStream output) {
			this.cacheOut = output;
			this.remoteInputStream = fsDataInputStream;
		}

		@Override
		public int read() throws IOException {
			int o = this.remoteInputStream.read();
			if (o != -1) {
				//re-cache and ignore exception
				if (cacheOut != null && !cacheOut.isDiscarded()) {
					try {
						cacheOut.write(o);
					} catch (Exception ignore) {
						cacheOut.discard();
					}
				}
			}
			return o;
		}

		@Override
		public int read(byte[] b) throws IOException {
			int n = this.remoteInputStream.read(b);
			if (n != -1) {
				//re-cache and ignore exception
				if (cacheOut != null && !cacheOut.isDiscarded()) {
					try {
						this.cacheOut.write(b, 0, n);
					} catch (Exception ignore) {
						cacheOut.discard();
					}
				}
			}
			return n;
		}

		@Override
		public void seek(long desired) throws IOException {
			this.remoteInputStream.seek(desired);
			if (cacheOut != null) {
				cacheOut.discard();
			}
		}

		@Override
		public long getPos() throws IOException {
			return this.remoteInputStream.getPos();
		}

		@Override
		public void close() throws IOException {
			if (this.cacheOut != null) {
				this.cacheOut.end();
			}
			this.remoteInputStream.close();
		}
	}
}
