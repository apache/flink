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

package eu.stratosphere.nephele.util;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Auxiliary class which provides atomic operations on enumerations. Internally, the class uses an
 * {@link AtomicReference} object to guarantee the atomicity.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class AtomicEnum<T extends Enum<T>> {

	/**
	 * The atomic reference which provides the atomicity internally.
	 */
	private final AtomicReference<T> ref;

	/**
	 * Constructs a new atomic enumeration object.
	 * 
	 * @param initialValue
	 *        the initial value of the enumeration
	 */
	public AtomicEnum(final T initialValue) {

		this.ref = new AtomicReference<T>(initialValue);
	}

	/**
	 * Sets to the given value.
	 * 
	 * @param newValue
	 *        the new value
	 */
	public void set(final T newValue) {

		this.ref.set(newValue);
	}

	/**
	 * Gets the current value.
	 * 
	 * @return the current value.
	 */
	public T get() {

		return this.ref.get();
	}

	/**
	 * Sets the given value and returns the old value.
	 * 
	 * @param newValue
	 *        the new value
	 * @return the previous value
	 */
	public T getAndSet(final T newValue) {

		return this.ref.getAndSet(newValue);
	}

	/**
	 * Atomically set the value to the given updated value if the current value == the expected value.
	 * 
	 * @param expect
	 *        the expected value
	 * @param update
	 *        the new value
	 * @return <code>true</code> if successful, <code>false</code> otherwise
	 */
	public boolean compareAndSet(final T expect, final T update) {

		return this.ref.compareAndSet(expect, update);
	}
}
