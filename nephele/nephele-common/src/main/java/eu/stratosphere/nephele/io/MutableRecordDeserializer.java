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

package eu.stratosphere.nephele.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class implements a record deserializer for mutable objects.
 * 
 * @author warneke
 * @param <T>
 *        the type of record deserialized by this record deserializer
 */
public final class MutableRecordDeserializer<T extends IOReadableWritable> implements RecordDeserializer<T> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T deserialize(final T target, final DataInput in) throws IOException {

		if (target == null) {
			throw new IllegalArgumentException("target must not be null");
		}

		target.read(in);
		return target;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T getInstance() {

		throw new IllegalStateException("getInstance called on MutableRecordDeserializer");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setClassLoader(final ClassLoader classLoader) {

		// Nothing to do here
	}

}
