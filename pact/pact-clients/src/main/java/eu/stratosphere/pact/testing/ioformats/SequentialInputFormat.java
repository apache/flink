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
package eu.stratosphere.pact.testing.ioformats;

import java.io.DataInputStream;
import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
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
public class SequentialInputFormat<K extends Key, V extends Value> extends FileInputFormat<K, V>
{
	private Class<K> keyClass;
	
	private Class<V> valueClass;
	
	private DataInputStream dataInputStream;

	@SuppressWarnings("unchecked")
	public SequentialInputFormat() {
		if (this.getClass() != SequentialInputFormat.class) {
			this.keyClass = ReflectionUtil.getTemplateType1(this.getClass());
			this.valueClass = ReflectionUtil.getTemplateType2(this.getClass());
		} else {
			// default values -> should be overwritten when file is opened and nonempty
			this.keyClass = (Class<K>) PactInteger.class;
			this.valueClass = (Class<V>) PactInteger.class;
		}
	}
	
	
	@Override
	public void close() throws IOException {
		super.close();
		this.dataInputStream.close();
	}

	@Override
	public void configure(final Configuration parameters)
	{
		super.configure(parameters);
		
		final Class<K> keyClass = parameters.getClass("key", null, (Class<K>) null);
		if (keyClass != null)
			this.keyClass = keyClass;
		final Class<V> valueClass = parameters.getClass("value", null, (Class<V>) null);
		if (keyClass != null)
			this.valueClass = valueClass;
	}

	@Override
	public KeyValuePair<K, V> createPair() {
		return new KeyValuePair<K, V>(ReflectionUtil.newInstance(this.keyClass), ReflectionUtil.newInstance(this.valueClass));
	}

	@Override
	public boolean nextRecord(final KeyValuePair<K, V> pair) throws IOException {
		if (this.dataInputStream.available() == 0)
			return false;

		pair.read(this.dataInputStream);
		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void open(FileInputSplit split) throws IOException
	{
		super.open(split);
		
		this.dataInputStream = new DataInputStream(this.stream);

		if (this.dataInputStream.available() > 0) {
			try {
				this.keyClass = (Class<K>) Class.forName(this.dataInputStream.readUTF());
			} catch (final ClassNotFoundException e) {
				throw new IOException("Cannot resolve key type " + e);
			}
			try {
				this.valueClass = (Class<V>) Class.forName(this.dataInputStream.readUTF());
			} catch (final ClassNotFoundException e) {
				throw new IOException("Cannot resolve value type " + e);
			}
		}
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return this.dataInputStream.available() == 0;
	}


	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#getStatistics()
	 */
	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		return null;
	}
}