/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

/*
 *  Copyright 2010 casp.
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  under the License.
 */
package eu.stratosphere.nephele.event.task;

/**
 * This class provides a simple implementation of an event that holds a string value.
 * <p>
 * This class is thread-safe.
 * 
 * @author casp
 */
public class StringTaskEvent extends AbstractTaskEvent {

	/**
	 * The string encapsulated by this event.
	 */
	private final String message;

	/**
	 * Default constructor required by kryo.
	 */
	@SuppressWarnings("unused")
	private StringTaskEvent() {
		this.message = null;
	}

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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		if (this.message == null) {
			return 0;
		}

		return this.message.hashCode();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {

		if (!(obj instanceof StringTaskEvent)) {
			return false;
		}

		final StringTaskEvent ste = (StringTaskEvent) obj;

		if (this.message == null) {
			if (ste.getString() == null) {
				return true;
			}

			return false;
		}

		return this.message.equals(ste.getString());
	}
}
