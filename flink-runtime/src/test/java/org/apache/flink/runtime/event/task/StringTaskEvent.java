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

package org.apache.flink.runtime.event.task;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.util.StringUtils;

import java.io.IOException;

/**
 * This class provides a simple implementation of an event that holds a string value.
 */
public class StringTaskEvent extends TaskEvent {

	/**
	 * The string encapsulated by this event.
	 */
	private String message;

	/**
	 * The default constructor implementation. It should only be used for deserialization.
	 */
	public StringTaskEvent() {}

	/**
	 * Constructs a new string task event with the given string message.
	 *
	 * @param message
	 *        the string message that shall be stored in this event
	 */
	public StringTaskEvent(final String message) {
		this.message = message;
	}

	/**
	 * Returns the stored string.
	 *
	 * @return the stored string or <code>null</code> if no string is set
	 */
	public String getString() {
		return this.message;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		StringUtils.writeNullableString(this.message, out);
	}

	@Override
	public void read(final DataInputView in) throws IOException {
		this.message = StringUtils.readNullableString(in);
	}

	@Override
	public int hashCode() {
		if (this.message == null) {
			return 0;
		}

		return this.message.hashCode();
	}

	@Override
	public boolean equals(final Object obj) {
		if (!(obj instanceof StringTaskEvent)) {
			return false;
		}

		final StringTaskEvent ste = (StringTaskEvent) obj;
		if (this.message == null) {
			return ste.getString() == null;
		}

		return this.message.equals(ste.getString());
	}
}
