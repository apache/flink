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
package org.apache.flink.runtime.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CachedStreamStateHandle;
import org.apache.flink.runtime.checkpoint.CheckpointCache;
import org.apache.flink.runtime.checkpoint.CheckpointCache.CachedOutputStream;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * {@link CachedCheckpointStreamFactory} is used to build an output stream that writes data to both remote end (e.g:DFS) and local end.
 * Local data is managed by {@link CheckpointCache}. It simply wraps {@link CheckpointCache} and {@link CheckpointStreamFactory} and
 * create a hybrid output stream by {@link CheckpointCache} and {@link CheckpointStreamFactory}, this hybrid output stream will write
 * to both remote end and local end.
 */
public class CachedCheckpointStreamFactory implements CheckpointStreamFactory {

	private static Logger LOG = LoggerFactory.getLogger(CachedCheckpointStreamFactory.class);

	private final CheckpointCache cache;
	private final CheckpointStreamFactory remoteFactory;

	public CachedCheckpointStreamFactory(CheckpointCache cache, CheckpointStreamFactory factory) {
		this.cache = cache;
		this.remoteFactory = Preconditions.checkNotNull(factory, "Remote stream factory is null.");
	}

	public CheckpointStateOutputStream createCheckpointStateOutputStream(long checkpointID, long timestamp, StateHandleID handleID) throws Exception {
		return createCheckpointStateOutputStream(checkpointID, timestamp, handleID, false);
	}

	public CheckpointStateOutputStream createCheckpointStateOutputStream(long checkpointID, long timestamp, StateHandleID handleID, boolean placeholder) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("create cache output stream: cpkID:{} placeHolder:{}", checkpointID, placeholder);
		}
		CachedOutputStream cachedOut = null;
		if (cache != null) {
			cachedOut = cache.createOutputStream(checkpointID, handleID, placeholder);
		}
		CheckpointStateOutputStream remoteOut = null;
		if (!placeholder) {
			remoteOut = remoteFactory.createCheckpointStateOutputStream(checkpointID, timestamp);
		}
		CachedCheckpointStateOutputStream output = new CachedCheckpointStateOutputStream(cachedOut, remoteOut);
		return output;
	}

	@Override
	public CheckpointStateOutputStream createCheckpointStateOutputStream(long checkpointID, long timestamp) throws Exception {
		LOG.warn("create output stream which is not cacheable.");
		return remoteFactory.createCheckpointStateOutputStream(checkpointID, timestamp);
	}

	@Override
	public void close() throws Exception {
		remoteFactory.close();
	}

	/**
	 * A hybrid checkpoint output stream which write data to both remote end and local end,
	 * writing data locally failed won't stop writing to remote. This hybrid output stream
	 * will return a {@link CachedStreamStateHandle} in closeAndGetHandle(), it can be used for read data locally.
	 */
	public static class CachedCheckpointStateOutputStream extends CheckpointStateOutputStream {

		private CachedOutputStream cacheOut = null;
		private CheckpointStateOutputStream remoteOut = null;

		public CachedCheckpointStateOutputStream(CachedOutputStream cacheOut, CheckpointStateOutputStream remoteOut) {
			this.cacheOut = cacheOut;
			this.remoteOut = remoteOut;
		}

		@Override
		public StreamStateHandle closeAndGetHandle() throws IOException {
			if (cacheOut != null) {
				// finalize cache data
				StateHandleID cacheId = cacheOut.getCacheID();
				cacheOut.end();

				StreamStateHandle remoteHandle;
				if (remoteOut != null) {
					remoteHandle = remoteOut.closeAndGetHandle();
				} else {
					remoteHandle = new PlaceholderStreamStateHandle(cacheId);
				}
				return new CachedStreamStateHandle(cacheId, remoteHandle);
			} else {
				if (remoteOut != null) {
					return remoteOut.closeAndGetHandle();
				} else {
					return null;
				}
			}
		}

		@Override
		public long getPos() throws IOException {
			return remoteOut != null ? remoteOut.getPos() :-1L;
		}

		@Override
		public void write(int b) throws IOException {
			// write to local
			if (cacheOut != null) {
				try {
					cacheOut.write(b);
				} catch (Exception e) {
					//discard
					cacheOut.discard();
				}
			}

			// write to remote
			if (remoteOut != null) {
				remoteOut.write(b);
			}
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			// write to local
			if (cacheOut != null) {
				try {
					cacheOut.write(b, off, len);
				} catch (Exception e) {
					//discard
					cacheOut.discard();
				}
			}

			// write to remote
			if (remoteOut != null) {
				remoteOut.write(b, off, len);
			}
		}

		@Override
		public void flush() throws IOException {
			if (cacheOut != null) {
				cacheOut.flush();
			}
			if (remoteOut != null) {
				remoteOut.flush();
			}
		}

		@Override
		public void sync() throws IOException {
			if (remoteOut != null) {
				remoteOut.sync();
			}
		}

		@Override
		public void close() throws IOException {
			if (cacheOut != null) {
				cacheOut.close();
			}
			if (remoteOut != null) {
				remoteOut.close();
			}
		}

		@VisibleForTesting
		public void setCacheOut(CachedOutputStream cacheOut) {
			this.cacheOut = cacheOut;
		}

		@VisibleForTesting
		public void setRemoteOut(CheckpointStateOutputStream remoteOut) {
			this.remoteOut = remoteOut;
		}
	}
}
