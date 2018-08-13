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

package org.apache.flink.core.memory;

import javax.annotation.Nonnull;

/**
 * Adapter to {@link DataOutputView} that operates on a byte-array and offers read/write access to the current position.
 */
public class ByteArrayDataOutputView extends DataOutputViewStreamWrapper {

	@Nonnull
	private final ByteArrayOutputStreamWithPos outputStreamWithPos;

	public ByteArrayDataOutputView() {
		this(64);
	}

	public ByteArrayDataOutputView(int initialSize) {
		super(new ByteArrayOutputStreamWithPos(initialSize));
		this.outputStreamWithPos = (ByteArrayOutputStreamWithPos) out;
	}

	public void reset() {
		outputStreamWithPos.reset();
	}

	@Nonnull
	public byte[] toByteArray() {
		return outputStreamWithPos.toByteArray();
	}

	public int getPosition() {
		return outputStreamWithPos.getPosition();
	}

	public void setPosition(int position) {
		outputStreamWithPos.setPosition(position);
	}

	@Nonnull
	public byte[] getInternalBufferReference() {
		return outputStreamWithPos.getBuf();
	}
}
