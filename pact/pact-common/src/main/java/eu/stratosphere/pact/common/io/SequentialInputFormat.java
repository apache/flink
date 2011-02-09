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

import java.io.DataInputStream;
import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.util.ReflectionUtil;

/**
 * Reads the key/value pairs from the native format which is deserialable without configuration. The type parameter can
 * be omitted (no need to subclass this format) since all type information are stored in the file.
 * 
 * @author Arvid Heise
 * @param <K>
 *        the type of the key to read
 * @param <V>
 *        the type of the value to read
 * @see SequentialOutputFormat
 */
public class SequentialInputFormat<K extends Key, V extends Value> extends InputFormat<K, V> {
	private DataInputStream dataInputStream;

	@Override
	public void close() throws IOException {
		this.dataInputStream.close();
	}

	@Override
	public void configure(final Configuration parameters) {
		final Class<K> keyClass = parameters.getClass("key", null, (Class<K>) null);
		if (keyClass != null)
			super.ok = keyClass;
		final Class<V> valueClass = parameters.getClass("value", null, (Class<V>) null);
		if (keyClass != null)
			super.ov = valueClass;
	}

	@Override
	public KeyValuePair<K, V> createPair() {
		return new KeyValuePair<K, V>(ReflectionUtil.newInstance(this.ok), ReflectionUtil.newInstance(this.ov));
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void initTypes() {
		if (this.getClass() != SequentialInputFormat.class) {
			super.ok = ReflectionUtil.getTemplateType1(this.getClass());
			super.ov = ReflectionUtil.getTemplateType2(this.getClass());
		} else {
			// default values -> should be overwritten when file is opened and nonempty
			super.ok = (Class<K>) PactInteger.class;
			super.ov = (Class<V>) PactInteger.class;
		}
	}

	@Override
	public boolean nextPair(final KeyValuePair<K, V> pair) throws IOException {
		if (this.dataInputStream.available() == 0)
			return false;

		pair.read(this.dataInputStream);
		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void open() throws IOException {
		this.dataInputStream = new DataInputStream(this.stream);

		if (this.dataInputStream.available() > 0) {
			try {
				super.ok = (Class<K>) Class.forName(this.dataInputStream.readUTF());
			} catch (final ClassNotFoundException e) {
				throw new IOException("Cannot resolve key type " + e);
			}
			try {
				super.ov = (Class<V>) Class.forName(this.dataInputStream.readUTF());
			} catch (final ClassNotFoundException e) {
				throw new IOException("Cannot resolve value type " + e);
			}
		}
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return this.dataInputStream.available() == 0;
	}
}