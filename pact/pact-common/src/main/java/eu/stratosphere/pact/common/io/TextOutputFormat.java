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

package eu.stratosphere.pact.common.io;

import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType1;
import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType2;

import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

/**
 * Base implementation for delimiter based output formats. It can be adjusted to specific
 * formats by implementing the writeLine() method.
 * 
 * @author Moritz Kaufmann
 * @param <K>
 * @param <V>
 */
public abstract class TextOutputFormat<K extends Key, V extends Value> extends OutputFormat<K, V> {

	/**
	 * Serializes the given key/value pair and returns it as byte stream. The delimiter
	 * between different pairs has to be included at the end of the byte array (e.g.
	 * '\n' for line based formats).
	 * 
	 * @param pair
	 * @return
	 */
	public abstract byte[] writeLine(KeyValuePair<K, V> pair);

	/**
	 * {@inheritDoc}
	 */
	@Override
	public KeyValuePair<K, V> createPair() {
		try {
			return new KeyValuePair<K, V>(ok.newInstance(), ov.newInstance());
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void initTypes() {
		super.ok = getTemplateType1(getClass());
		super.ov = getTemplateType2(getClass());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() {

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void open() {

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writePair(KeyValuePair<K, V> pair) {
		byte[] line = writeLine(pair);
		try {
			stream.write(line, 0, line.length);
		} catch (IOException e) {
			// TODO Log properly
			throw new RuntimeException("Should not happen", e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void configure(Configuration parameters) {
	}

}
