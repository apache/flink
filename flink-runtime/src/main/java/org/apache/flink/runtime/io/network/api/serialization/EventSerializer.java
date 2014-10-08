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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.util.DataInputDeserializer;
import org.apache.flink.runtime.util.DataOutputSerializer;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.nio.ByteBuffer;

public class EventSerializer {

	public final static BufferRecycler RECYCLER = new BufferRecycler() {
		@Override
		public void recycle(MemorySegment memorySegment) {
			memorySegment.free();
		}
	};

	public static ByteBuffer toSerializedEvent(AbstractEvent event) {
		try {
			final DataOutputSerializer serializer = new DataOutputSerializer(128);

			serializer.writeUTF(event.getClass().getName());
			event.write(serializer);

			return serializer.wrapAsByteBuffer();
		}
		catch (IOException e) {
			throw new RuntimeException("Error while serializing event.", e);
		}
	}

	public static AbstractEvent fromSerializedEvent(ByteBuffer buffer, ClassLoader classLoader) {
		try {
			final DataInputDeserializer deserializer = new DataInputDeserializer(buffer);

			final String className = deserializer.readUTF();

			final Class<? extends AbstractEvent> clazz;
			try {
				clazz = classLoader.loadClass(className).asSubclass(AbstractEvent.class);
			}
			catch (ClassNotFoundException e) {
				throw new RuntimeException("Could not load event class '" + className + "'.", e);
			}
			catch (ClassCastException e) {
				throw new RuntimeException("The class '" + className + "' is not a valid subclass of '" + AbstractEvent.class.getName() + "'.", e);
			}

			final AbstractEvent event = InstantiationUtil.instantiate(clazz, AbstractEvent.class);
			event.read(deserializer);

			return event;
		}
		catch (IOException e) {
			throw new RuntimeException("Error while deserializing event.", e);
		}
	}

	// ------------------------------------------------------------------------
	// Buffer helpers
	// ------------------------------------------------------------------------

	public static Buffer toBuffer(AbstractEvent event) {
		final ByteBuffer serializedEvent = EventSerializer.toSerializedEvent(event);

		final Buffer buffer = new Buffer(new MemorySegment(serializedEvent.array()), RECYCLER, false);
		buffer.setSize(serializedEvent.remaining());

		return buffer;
	}

	public static AbstractEvent fromBuffer(Buffer buffer, ClassLoader classLoader) {
		return fromSerializedEvent(buffer.getNioBuffer(), classLoader);
	}
}
