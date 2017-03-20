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

package org.apache.flink.migration.runtime.state;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;

import java.io.IOException;
import java.io.Serializable;

/**
 * A state backend defines how state is stored and snapshotted during checkpoints.
 *
 * @deprecated Internal class for savepoint backwards compatibility. Don't use for other purposes.
 */
@Deprecated
@SuppressWarnings("deprecation")
public abstract class AbstractStateBackend implements Serializable {
	
	private static final long serialVersionUID = 4620413814639220247L;

	/**
	 * Simple state handle that resolved a {@link DataInputView} from a StreamStateHandle.
	 */
	public static final class DataInputViewHandle implements StateHandle<DataInputView> {

		private static final long serialVersionUID = 2891559813513532079L;

		private final StreamStateHandle stream;

		private DataInputViewHandle(StreamStateHandle stream) {
			this.stream = stream;
		}

		public StreamStateHandle getStreamStateHandle() {
			return stream;
		}

		@Override
		public DataInputView getState(ClassLoader userCodeClassLoader) throws Exception {
			return new DataInputViewStreamWrapper(stream.getState(userCodeClassLoader));
		}

		@Override
		public void discardState() throws Exception {
			throw new UnsupportedOperationException();
		}

		@Override
		public long getStateSize() throws Exception {
			return stream.getStateSize();
		}

		@Override
		public void close() throws IOException {
			throw new UnsupportedOperationException();
		}
	}
}
