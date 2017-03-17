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

package org.apache.flink.migration.v0.runtime;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;

import java.io.Serializable;

/**
 * A state backend defines how state is stored and snapshotted during checkpoints.
 */
@Deprecated
@SuppressWarnings("deprecation")
public abstract class AbstractStateBackendV0 implements Serializable {
	
	private static final long serialVersionUID = 4620413814639220247L;

	/**
	 * Simple state handle that resolved a {@link DataInputView} from a StreamStateHandle.
	 */
	public static final class DataInputViewHandle implements StateHandleV0<DataInputView> {

		private static final long serialVersionUID = 2891559813513532079L;

		private final StreamStateHandleV0 stream;

		private DataInputViewHandle(StreamStateHandleV0 stream) {
			this.stream = stream;
		}

		public StreamStateHandleV0 getStreamStateHandle() {
			return stream;
		}

		@Override
		public DataInputView getState(ClassLoader userCodeClassLoader) throws Exception {
			return new DataInputViewStreamWrapper(stream.getState(userCodeClassLoader));
		}
	}
}
