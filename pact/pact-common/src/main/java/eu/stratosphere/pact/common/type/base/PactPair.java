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

package eu.stratosphere.pact.common.type.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.util.ReflectionUtil;

/**
 * Generic pair base type for PACT programs that implements the Key interfaces.
 * PactList encapsulates two objects that implement the Key interface. 
 * 
 * @see eu.stratosphere.pact.common.type.Key
 * 
 * @param <U> Type of the pair's first element.
 * @param <V> Type of the pair's second element.
 *  
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 * 
 */
public abstract class PactPair<U extends Key, V extends Key> implements Key {
	
	// class of the first pair element
	private final Class<U> firstClass;
	// class of the second pair element
	private final Class<V> secondClass;

	// the first pair element
	private U first;
	// the second pair element
	private V second;

	/**
	 * Initializes both encapsulated pair elements with empty objects.
	 */
	public PactPair() {
		this.firstClass = ReflectionUtil.<U> getTemplateType1(this.getClass());
		this.secondClass = ReflectionUtil.<V> getTemplateType2(this.getClass());

		try {
			this.first = this.firstClass.newInstance();
			this.second = this.secondClass.newInstance();
		} catch (final InstantiationException e) {
			throw new RuntimeException(e);
		} catch (final IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Initializes the encapsulated pair elements with the provided values.
	 * 
	 * @param first Initial value of the first encapsulated pair element.
	 * @param second Initial value of the second encapsulated pair element.
	 */
	public PactPair(final U first, final V second) {
		this.firstClass = ReflectionUtil.<U> getTemplateType1(this.getClass());
		this.secondClass = ReflectionUtil.<V> getTemplateType1(this.getClass());

		this.first = first;
		this.second = second;
	}

	/**
	 * Returns the first encapsulated pair element.
	 * 
	 * @return The first encapsulated pair element.
	 */
	public U getFirst() {
		return this.first;
	}

	/**
	 * Sets the first encapsulated pair element to the specified value.
	 * 
	 * @param first
	 *        The new value of the first encapsulated pair element.
	 */
	public void setFirst(final U first) {
		if (first == null)
			throw new NullPointerException("first must not be null");

		this.first = first;
	}

	/**
	 * Returns the second encapsulated pair element.
	 * 
	 * @return The second encapsulated pair element.
	 */
	public V getSecond() {
		return this.second;
	}

	/**
	 * Sets the second encapsulated pair element to the specified value.
	 * 
	 * @param second
	 *        The new value of the second encapsulated pair element.
	 */
	public void setSecond(final V second) {
		if (second == null)
			throw new NullPointerException("second must not be null");

		this.second = second;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "<" + this.first.toString() + "|" + this.second.toString() + ">";
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		this.first.read(in);
		this.second.read(in);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		this.first.write(out);
		this.second.write(out);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(final Key o) {
		if (!(o instanceof PactPair<?, ?>))
			throw new ClassCastException("Cannot compare " + o.getClass().getName() + " to N_Pair.");

		int result = this.first.compareTo(((PactPair<?, ?>) o).first);
		if (result == 0)
			result = this.second.compareTo(((PactPair<?, ?>) o).second);
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.first.hashCode();
		result = prime * result + this.second.hashCode();
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
		final PactPair<?, ?> other = (PactPair<?, ?>) obj;
		return this.first.equals(other.first) && this.second.equals(other.second);
	}
}
