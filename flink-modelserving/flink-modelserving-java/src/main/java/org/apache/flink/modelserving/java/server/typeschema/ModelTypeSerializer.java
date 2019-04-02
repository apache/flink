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

import java.io.IOException;

/**
 * Type serializer for model - used by Flink checkpointing.
 */
public class ModelTypeSerializer<RECORD, RESULT> extends TypeSerializer<Model<RECORD, RESULT>> {

	/**
	 * Create model instatnce.
	 * @return model's instance.
	 */
	@Override
	public Model createInstance() {
		return null;
	}

	/**
	 * Check if Type serializers can be equal.
	 * @param obj another object.
	 * @return boolean specifying whether serializires can be equal.
	 */
//	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof ModelTypeSerializer;
	}

	/**
	 * Duplicate type serializer.
	 * @return duplicate of serializer.
	 */
	@Override
	public TypeSerializer<Model<RECORD, RESULT>> duplicate() {
		return new ModelTypeSerializer<RECORD, RESULT>();
	}

	/**
	 * Serialize model.
	 * @param model model.
	 * @param target output.
	 */
	@Override
	public void serialize(Model model, DataOutputView target) throws IOException {
		if (model == null) {
			target.writeBoolean(false);
		}
		else {
			target.writeBoolean(true);
			byte[] content = model.getBytes();
			target.writeLong(model.getType());
			target.writeLong(content.length);
			target.write(content);
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
	public TypeSerializerSnapshot<Model<RECORD, RESULT>> snapshotConfiguration() {
		return new ModelSerializerConfigSnapshot<RECORD, RESULT>();
	}

	/**
	 * Check if type serializer's are equal.
	 * @param obj Byte array message.
	 * @return boolean specifying whether type serializers are equal.
	 */
	@Override
	public boolean equals(Object obj) {
		return obj instanceof ModelTypeSerializer;
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
	 * Copy model.
	 * @param from original model.
	 * @return model's copy.
	 */
	@Override
	public Model copy(Model from) {
		return DataConverter.copy(from);
	}

	/**
	 * Copy model (with reuse).
	 * @param from original model.
	 * @param reuse model to reuse.
	 * @return model's copy.
	 */
	@Override
	public Model copy(Model from, Model reuse) {
		return DataConverter.copy(from);
	}

	/**
	 * Copy model using data streams.
	 * @param source original data stream.
	 * @param target resulting data stream.
	 */
	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
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
	 * @return deserialized model.
	 */
	@Override
	public Model deserialize(DataInputView source) throws IOException {
		boolean exist = source.readBoolean();
		if (!exist){
			return null;
		}
		int type = (int) source.readLong();
		int size = (int) source.readLong();
		byte[] content = new byte[size];
		source.read(content);
		return DataConverter.restore(type, content);
	}

	/**
	 * Deserialize byte array message (with reuse).
	 * @param source Byte array message.
	 * @param reuse model to reuse.
	 * @return deserialized model.
	 */
	@Override
	public Model deserialize(Model reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}
}
