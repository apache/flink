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
 * @author Erik Nijkamp
 * @param <K>
 * @param <V>
 */
public final class KeyValuePair<K extends Key, V extends Value> extends Pair<K, V> implements Record {

	public KeyValuePair() {
		super();
	}

	public KeyValuePair(final K key, final V value) {
		super(key, value);
	}

	@Override
	public void read(final DataInput in) throws IOException {
		this.key.read(in);
		this.value.read(in);
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		this.key.write(out);
		this.value.write(out);
	}

	@Override
	public String toString() {
		return "(" + this.key + "," + this.value + ")";
	}
}
