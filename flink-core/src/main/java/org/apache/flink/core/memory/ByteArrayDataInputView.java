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
 * Reusable adapter to {@link DataInputView} that operates on given byte-arrays.
 */
public class ByteArrayDataInputView extends DataInputViewStreamWrapper {

	@Nonnull
	private final ByteArrayInputStreamWithPos inStreamWithPos;

	public ByteArrayDataInputView() {
		super(new ByteArrayInputStreamWithPos());
		this.inStreamWithPos = (ByteArrayInputStreamWithPos) in;
	}

	public ByteArrayDataInputView(@Nonnull byte[] buffer) {
		this(buffer, 0, buffer.length);
	}

	public ByteArrayDataInputView(@Nonnull byte[] buffer, int offset, int length) {
		this();
		setData(buffer, offset, length);
	}

	public int getPosition() {
		return inStreamWithPos.getPosition();
	}

	public void setPosition(int pos) {
		inStreamWithPos.setPosition(pos);
	}

	public void setData(@Nonnull byte[] buffer, int offset, int length) {
		inStreamWithPos.setBuffer(buffer, offset, length);
	}
}
