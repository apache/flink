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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.types.Record;

/**
 * Implementation of a PACT KeyValuePair, the basic record in the PACT programming model, 
 * for the Nephele execution engine.
 * <p> 
 * Parallelization Contracts (PACTs) consume and produce KeyValuePairs. Within PACTs these pairs organized 
 * (e.g., grouped, joined, or combined) depending on the concrete PACT and split up into keys and value. 
 * Keys and Values are forwarded to user functions (stub implementations). 
 * <p>
 * KeyValuePair implements {@link eu.stratosphere.nephele.types.Record} in order to be 
 * processable by Nephele. 
 * 
 * @see eu.stratosphere.nephele.types.Record
 * 
 * @param <K> Type of the pair's key element.
 * @param <V> Type of the pair's value element.
 * 
 * @author Erik Nijkamp
 * 
 */
final public class KeyValuePair<K extends Key, V extends Value> implements Record {

	// key object
	protected K key;
	// value object
	protected V value;
	
	/**
	 * Initializes key and value with null.
	 */
	public KeyValuePair() {
	}

	/**
	 * Initializes key and value with the provided values. 
	 * 
	 * @param key Initial key of the pair.
	 * @param value Initial value of the pair.
	 */
	public KeyValuePair(final K key, final V value) {
		if (key == null)
			throw new NullPointerException("key must not be null");
		if (value == null)
			throw new NullPointerException("value must not be null");

		this.key = key;
		this.value = value;
	}

	/**
	 * Returns the key.
	 * 
	 * @return The key.
	 */
	public K getKey() {
		return this.key;
	}

	/**
	 * Sets the key to the specified value.
	 * 
	 * @param key
	 *        The new key.
	 */
	public void setKey(final K key) {
		if (key == null)
			throw new NullPointerException("key must not be null");

		this.key = key;
	}

	/**
	 * Returns the value.
	 * 
	 * @return The value.
	 */
	public V getValue() {
		return this.value;
	}

	/**
	 * Sets the value to the specified value.
	 * 
	 * @param value
	 *        The new value.
	 */
	public void setValue(final V value) {
		if (value == null)
			throw new NullPointerException("value must not be null");

		this.value = value;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.types.Record#read(java.io.DataInput)
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		this.key.read(in);
		this.value.read(in);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.types.Record#write(java.io.DataOutput)
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		this.key.write(out);
		this.value.write(out);
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
		final KeyValuePair<?, ?> other = (KeyValuePair<?, ?>) obj;
		return this.key.equals(other.key) && this.value.equals(other.value);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "(" + this.key + "," + this.value + ")";
	}
}
