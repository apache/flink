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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

/**
 * A placeholder state handle for shared state that will replaced by an original that was
 * created in a previous checkpoint. So we don't have to send the handle twice, e.g. in
 * case of {@link ByteStreamStateHandle}. To be used in the referenced states of
 * {@link IncrementalKeyedStateHandle}.
 * <p>
 * IMPORTANT: This class currently overrides equals and hash code only for testing purposes. They
 * should not be called from production code. This means this class is also not suited to serve as
 * a key, e.g. in hash maps.
 */
public class PlaceholderStreamStateHandle implements StreamStateHandle {

	private static final long serialVersionUID = 1L;

	/** We remember the size of the original file for which this is a placeholder */
	private final long originalSize;

	public PlaceholderStreamStateHandle(long originalSize) {
		this.originalSize = originalSize;
	}

	@Override
	public FSDataInputStream openInputStream() {
		throw new UnsupportedOperationException(
			"This is only a placeholder to be replaced by a real StreamStateHandle in the checkpoint coordinator.");
	}

	@Override
	public void discardState() throws Exception {
		// nothing to do.
	}

	@Override
	public long getStateSize() {
		return originalSize;
	}

	/**
	 * This method is should only be called in tests! This should never serve as key in a hash map.
	 */
	@VisibleForTesting
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		PlaceholderStreamStateHandle that = (PlaceholderStreamStateHandle) o;

		return originalSize == that.originalSize;
	}

	/**
	 * This method is should only be called in tests! This should never serve as key in a hash map.
	 */
	@VisibleForTesting
	@Override
	public int hashCode() {
		return (int) (originalSize ^ (originalSize >>> 32));
	}
}
