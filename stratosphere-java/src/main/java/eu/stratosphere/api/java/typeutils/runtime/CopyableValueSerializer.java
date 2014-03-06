/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.api.java.typeutils.runtime;

import java.io.IOException;

import eu.stratosphere.api.common.typeutils.Serializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.types.CopyableValue;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.InstantiationUtil;


public class CopyableValueSerializer<T extends CopyableValue<T>> extends Serializer<T> {

	private static final long serialVersionUID = 1L;
	
	
	private final Class<T> valueClass;
	
	private final T instance;
	
	
	public CopyableValueSerializer(Class<T> valueClass) {
		this.valueClass = valueClass;
		this.instance = createInstance();
	}


	@Override
	public T createInstance() {
		return InstantiationUtil.instantiate(this.valueClass, Value.class);
	}

	@Override
	public T copy(T from, T reuse) {
		from.copyTo(reuse);
		return reuse;
	}

	@Override
	public int getLength() {
		return instance.getBinaryLength();
	}

	@Override
	public void serialize(T value, DataOutputView target) throws IOException {
		value.write(target);
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		reuse.read(source);
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		instance.copy(source, target);
	}
}
