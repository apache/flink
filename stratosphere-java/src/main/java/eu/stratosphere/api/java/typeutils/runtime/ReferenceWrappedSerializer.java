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
import java.io.Serializable;

import eu.stratosphere.api.common.typeutils.Serializer;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.util.InstantiationUtil;
import eu.stratosphere.util.Reference;


/**
 *
 */
public class ReferenceWrappedSerializer<T> extends TypeSerializer<Reference<T>> implements Serializable {

	private static final long serialVersionUID = 1L;
	
	
	private final Serializer<T> serializer;
	
	
	public ReferenceWrappedSerializer(Serializer<T> serializer) {
		this.serializer = serializer;
	}


	@Override
	public Reference<T> createInstance() {
		Reference<T> ref = new Reference<T>();
		ref.ref = serializer.createInstance();
		return ref;
	}

	@Override
	public Reference<T> createCopy(Reference<T> from) {
		Reference<T> copy = createInstance();
		copyTo(from, copy);
		return copy;
	}

	@Override
	public void copyTo(Reference<T> from, Reference<T> to) {
		to.ref = serializer.copy(from.ref, to.ref);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(Reference<T> value, DataOutputView target) throws IOException {
		this.serializer.serialize(value.ref, target);
	}

	@Override
	public void deserialize(Reference<T> value, DataInputView source) throws IOException {
		value.ref = this.serializer.deserialize(value.ref, source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		this.serializer.copy(source, target);
	}
	
	
	// --------------------------------------------------------------------------------------------
	// --------------------------------------------------------------------------------------------
	
	
	public static final class ReferenceWrappedSerializerFactory<T> implements TypeSerializerFactory<Reference<T>>, java.io.Serializable {

		private static final long serialVersionUID = 1L;
		

		private static final String CONFIG_KEY = "SER_DATA";
		
		private ReferenceWrappedSerializer<T> serializer;
		
		
		public ReferenceWrappedSerializerFactory() {}
		
		public ReferenceWrappedSerializerFactory(ReferenceWrappedSerializer<T> serializer) {
			this.serializer = serializer;
		}
		
		
		@Override
		public void writeParametersToConfig(Configuration config) {
			try {
				InstantiationUtil.writeObjectToConfig(serializer, config, CONFIG_KEY);
			}
			catch (Exception e) {
				throw new RuntimeException("Could not serialize serializer into the configuration.", e);
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void readParametersFromConfig(Configuration config, ClassLoader cl) throws ClassNotFoundException {
			try {
				serializer = (ReferenceWrappedSerializer<T>) InstantiationUtil.readObjectFromConfig(config, CONFIG_KEY, cl);
			}
			catch (ClassNotFoundException e) {
				throw e;
			}
			catch (Exception e) {
				throw new RuntimeException("Could not serialize serializer into the configuration.", e);
			}
		}

		@Override
		public TypeSerializer<Reference<T>> getSerializer() {
			if (serializer != null) {
				return serializer;
			} else {
				throw new RuntimeException("SerializerFactory ahas not been initialized from configuration.");
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public Class<Reference<T>> getDataType() {
			return (Class<Reference<T>>) (Class<?>) Reference.class;
		}

	}
}
