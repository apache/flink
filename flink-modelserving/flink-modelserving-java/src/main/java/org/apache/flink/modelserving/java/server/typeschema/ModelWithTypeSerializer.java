/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.modelserving.java.server.typeschema;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.modelserving.java.model.DataConverter;
import org.apache.flink.modelserving.java.model.Model;
import org.apache.flink.modelserving.java.model.ModelWithType;

import java.io.IOException;
import java.util.Optional;

/**
 * Type serializer for model with type - used by Flink checkpointing.
 */
public class ModelWithTypeSerializer<RECORD, RESULT> extends TypeSerializer<ModelWithType<RECORD, RESULT>> {

	/**
	 * Create model with state instatnce.
	 * @return model's instance.
	 */
	@Override
	public ModelWithType<RECORD, RESULT> createInstance() {

		return new ModelWithType<RECORD, RESULT>();
	}

	/**
	 * Check if Type serializers can be equal.
	 * @param obj another object.
	 * @return boolean specifying whether serializires can be equal.
	 */
//	@Override
	public boolean canEqual(Object obj) {

		return obj instanceof ModelWithTypeSerializer;
	}

	/**
	 * Duplicate type serializer.
	 * @return duplicate of serializer.
	 */
	@Override
	public TypeSerializer<ModelWithType<RECORD, RESULT>> duplicate() {

		return new ModelWithTypeSerializer<RECORD, RESULT>();
	}

	/**
	 * Serialize model with state.
	 * @param model model.
	 * @param target output.
	 */
	@Override
	public void serialize(ModelWithType<RECORD, RESULT> model, DataOutputView target)
		throws IOException {
		target.writeUTF(model.getDataType());
		if (model.getModel().isPresent()) {
			target.writeBoolean(true);
			byte[] content = model.getModel().get().getBytes();
			target.writeLong(model.getModel().get().getType());
			target.writeLong(content.length);
			target.write(content);
		}
		else {
			target.writeBoolean(false);
		}
	}

	/**
	 * Check whether type is immutable.
	 * @return boolean specifying whether type is immutable.
	 */
	@Override
	public boolean isImmutableType() {
		return false;
	}

	/**
	 * Get model serialized length.
	 * @return model's serialized length.
	 */
	@Override
	public int getLength() {
		return -1;
	}

	/**
	 * Get snapshot's configuration.
	 * @return snapshot's configuration.
	 */
	@Override
	public TypeSerializerSnapshot<ModelWithType<RECORD, RESULT>> snapshotConfiguration() {
		return new ModelWithTypeSerializerConfigSnapshot<RECORD, RESULT>();
	}

	/**
	 * Check if type serializer's are equal.
	 * @param obj Byte array message.
	 * @return boolean specifying whether type serializers are equal.
	 */
	@Override
	public boolean equals(Object obj) {
		return obj instanceof ModelWithTypeSerializer;
	}

	/**
	 * Get hash code.
	 * @return hash code.
	 */
	@Override
	public int hashCode() {
		return 42;
	}

	/**
	 * Copy model with type.
	 * @param from original model with state.
	 * @return model's with state copy.
	 */
	@Override
	public ModelWithType<RECORD, RESULT> copy(ModelWithType<RECORD, RESULT> from) {
		Model<RECORD, RESULT> model = DataConverter.copy(from.getModel().orElse(null));
		return new ModelWithType(from.getDataType(),
			(model == null) ? Optional.empty() : Optional.of(model));
	}

	/**
	 * Copy model with state (with reuse).
	 * @param from original model with state.
	 * @param reuse model with state to reuse.
	 * @return model's with state copy.
	 */
	@Override
	public ModelWithType copy(ModelWithType from, ModelWithType reuse) {
		return copy(from);
	}

	/**
	 * Copy model using data streams.
	 * @param source original data stream.
	 * @param target resulting data stream.
	 */
	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeUTF(source.readUTF());
		boolean exist = source.readBoolean();
		target.writeBoolean(exist);
		if (!exist){
			return;
		}
		target.writeLong (source.readLong ());
		int clen = (int) source.readLong ();
		target.writeLong (clen);
		byte[] content = new byte[clen];
		source.read (content);
		target.write (content);
	}

	/**
	 * Deserialize byte array message.
	 * @param source Byte array message.
	 * @return deserialized model with state.
	 */
	@Override
	public ModelWithType deserialize(DataInputView source) throws IOException {
		String dataType = source.readUTF();
		boolean exist = source.readBoolean();
		Optional<Model> model = Optional.empty();
		if (exist) {
			int type = (int) source.readLong();
			int size = (int) source.readLong();
			byte[] content = new byte[size];
			source.read(content);
			Model m = DataConverter.restore(type, content);
			if (m != null) {
				model = Optional.of(m);
			}
		}
		return new ModelWithType(dataType, model);	}

	/**
	 * Deserialize byte array message (with reuse).
	 * @param source Byte array message.
	 * @param reuse model to reuse.
	 * @return deserialized model.
	 */
	@Override
	public ModelWithType deserialize(ModelWithType reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}
}
