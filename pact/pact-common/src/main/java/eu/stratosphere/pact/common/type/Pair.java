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

package eu.stratosphere.pact.common.type;

/**
 * @TODO
 * @author DIMA
 */
public class Pair<K extends Key, V extends Value> {
	protected K key;

	protected V value;

	public Pair(final K key, final V value) {
		if (key == null)
			throw new NullPointerException("key must not be null");
		if (value == null)
			throw new NullPointerException("value must not be null");

		this.key = key;
		this.value = value;
	}

	/**
	 * Implementing classes must set the key/value to non-null values in their constructor.
	 */
	protected Pair() {
	}

	/**
	 * Returns the key.
	 * 
	 * @return the key
	 */
	public K getKey() {
		return this.key;
	}

	/**
	 * Sets the key to the specified value.
	 * 
	 * @param key
	 *        the key to set
	 */
	public void setKey(final K key) {
		if (key == null)
			throw new NullPointerException("key must not be null");

		this.key = key;
	}

	/**
	 * Returns the value.
	 * 
	 * @return the value
	 */
	public V getValue() {
		return this.value;
	}

	/**
	 * Sets the value to the specified value.
	 * 
	 * @param value
	 *        the value to set
	 */
	public void setValue(final V value) {
		if (value == null)
			throw new NullPointerException("value must not be null");

		this.value = value;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.key.hashCode();
		result = prime * result + this.value.hashCode();
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final Pair<?, ?> other = (Pair<?, ?>) obj;
		return this.key.equals(other.key) && this.value.equals(other.value);
	}

}
