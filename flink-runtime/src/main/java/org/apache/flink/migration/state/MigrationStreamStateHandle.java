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

package org.apache.flink.migration.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataInputStreamWrapper;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Migration;

import java.io.IOException;

/**
 * This class is just a StreamStateHandle that is tagged as migration, to figure out which restore logic to apply, e.g.
 * when restoring backend data from a state handle.
 *
 * @deprecated Internal class for savepoint backwards compatibility. Don't use for other purposes.
 */
@Internal
@Deprecated
public class MigrationStreamStateHandle implements StreamStateHandle, Migration {

	private static final long serialVersionUID = -2332113722532150112L;
	private final StreamStateHandle delegate;

	public MigrationStreamStateHandle(StreamStateHandle delegate) {
		this.delegate = delegate;
	}

	@Override
	public FSDataInputStream openInputStream() throws IOException {
		return new MigrationFSInputStream(delegate.openInputStream());
	}

	@Override
	public void discardState() throws Exception {
		delegate.discardState();
	}

	@Override
	public long getStateSize() {
		return delegate.getStateSize();
	}

	static class MigrationFSInputStream extends FSDataInputStreamWrapper implements Migration {

		public MigrationFSInputStream(FSDataInputStream inputStream) {
			super(inputStream);
		}
	}
}
