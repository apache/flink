/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.io.compression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractEvent;

public class CompressionEvent extends AbstractEvent {

	private int internalCompressionLibraryIndex = 0;

	public CompressionEvent(final int internalCompressionLibraryIndex) {
		this.internalCompressionLibraryIndex = internalCompressionLibraryIndex;
	}

	public CompressionEvent() {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		this.internalCompressionLibraryIndex = in.readInt();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		out.writeInt(this.internalCompressionLibraryIndex);
	}

	public int getCurrentInternalCompressionLibraryIndex() {

		return this.internalCompressionLibraryIndex;
	}
}
