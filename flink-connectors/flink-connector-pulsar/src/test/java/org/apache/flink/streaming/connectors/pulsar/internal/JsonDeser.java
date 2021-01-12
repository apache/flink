/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Preconditions;

import org.apache.pulsar.client.impl.schema.JSONSchema;

import java.io.IOException;

/**
 * Read JSON encoded data from Pulsar.
 *
 * @param <T> the type of record class.
 */
public class JsonDeser<T> implements DeserializationSchema<T> {

	private final Class<T> recordClazz;

	private transient JSONSchema<T> pulsarSchema;

	private JsonDeser(Class<T> recordClazz) {
		Preconditions.checkNotNull(recordClazz, "JSON record class must not be null");
		this.recordClazz = recordClazz;
	}

	public static <T> JsonDeser<T> of(Class<T> recordClazz) {
		return new JsonDeser<>(recordClazz);
	}

	@Override
	public T deserialize(byte[] message) throws IOException {
		checkPulsarJsonSchemaInitialized();
		return pulsarSchema.decode(message);
	}

	@Override
	public boolean isEndOfStream(T nextElement) {
		return false;
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return TypeInformation.of(recordClazz);
	}

	private void checkPulsarJsonSchemaInitialized() {
		if (pulsarSchema != null) {
			return;
		}

		this.pulsarSchema = JSONSchema.of(recordClazz);
	}
}
