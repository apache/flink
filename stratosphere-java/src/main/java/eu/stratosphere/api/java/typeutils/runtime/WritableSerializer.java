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

import org.apache.hadoop.io.Writable;

import com.esotericsoftware.kryo.Kryo;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.util.InstantiationUtil;

public class WritableSerializer<T extends Writable> extends TypeSerializer<T> {
	
	private static final long serialVersionUID = 1L;
	
	private final Class<T> typeClass;
	
	private transient Kryo kryo;
	
	private transient T copyInstance;
	
	public WritableSerializer(Class<T> typeClass) {
		this.typeClass = typeClass;
	}
	
	@Override
	public T createInstance() {
		return InstantiationUtil.instantiate(typeClass);
	}
	
	@Override
	public T copy(T from, T reuse) {
		checkKryoInitialized();
		reuse = this.kryo.copy(from);
		return reuse;
	}
	
	@Override
	public int getLength() {
		return -1;
	}
	
	@Override
	public void serialize(T record, DataOutputView target) throws IOException {
		record.write(target);
	}
	
	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		reuse.readFields(source);
		return reuse;
	}
	
	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		ensureInstanceInstantiated();
		copyInstance.readFields(source);
		copyInstance.write(target);
	}
	
	@Override
	public boolean isImmutableType() {
		return false;
	}
	
	@Override
	public boolean isStateful() {
		return true;
	}
	
	// --------------------------------------------------------------------------------------------
	
	private final void ensureInstanceInstantiated() {
		if (copyInstance == null) {
			copyInstance = createInstance();
		}
	}
	
	private final void checkKryoInitialized() {
		if (this.kryo == null) {
			this.kryo = new Kryo();
			this.kryo.setAsmEnabled(true);
			this.kryo.register(typeClass);
		}
	}
}
