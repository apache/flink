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
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.util.DataInputDeserializer;
import org.apache.flink.runtime.util.DataOutputSerializer;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Utility class to serialize and deserialize task events.
 */
public class EventSerializer {
	
	private static final int END_OF_PARTITION_EVENT = 0;

	private static final int CHECKPOINT_BARRIER_EVENT = 1;

	private static final int END_OF_SUPERSTEP_EVENT = 2;

	private static final int OTHER_EVENT = 3;
	
	// ------------------------------------------------------------------------
	
	public static ByteBuffer toSerializedEvent(AbstractEvent event) {
		final Class<?> eventClass = event.getClass();
		if (eventClass == EndOfPartitionEvent.class) {
			return ByteBuffer.wrap(new byte[] { 0, 0, 0, END_OF_PARTITION_EVENT });
		}
		else if (eventClass == CheckpointBarrier.class) {
			CheckpointBarrier barrier = (CheckpointBarrier) event;
			
			ByteBuffer buf = ByteBuffer.allocate(20);
			buf.putInt(0, CHECKPOINT_BARRIER_EVENT);
			buf.putLong(4, barrier.getId());
			buf.putLong(12, barrier.getTimestamp());
			return buf;
		}
		else if (eventClass == EndOfSuperstepEvent.class) {
			return ByteBuffer.wrap(new byte[] { 0, 0, 0, END_OF_SUPERSTEP_EVENT });
		}
		else {
			try {
				final DataOutputSerializer serializer = new DataOutputSerializer(128);
				serializer.writeInt(OTHER_EVENT);
				serializer.writeUTF(event.getClass().getName());
				event.write(serializer);
	
				return serializer.wrapAsByteBuffer();
			}
			catch (IOException e) {
				throw new RuntimeException("Error while serializing event.", e);
			}
		}
	}

	public static AbstractEvent fromSerializedEvent(ByteBuffer buffer, ClassLoader classLoader) {
		if (buffer.remaining() < 4) {
			throw new RuntimeException("Incomplete event");
		}
		
		final ByteOrder bufferOrder = buffer.order();
		buffer.order(ByteOrder.BIG_ENDIAN);
		
		try {
			int type = buffer.getInt();
				
			if (type == END_OF_PARTITION_EVENT) {
				return EndOfPartitionEvent.INSTANCE;
			}
			else if (type == CHECKPOINT_BARRIER_EVENT) {
				long id = buffer.getLong();
				long timestamp = buffer.getLong();
				return new CheckpointBarrier(id, timestamp);
			}
			else if (type == END_OF_SUPERSTEP_EVENT) {
				return EndOfSuperstepEvent.INSTANCE;
			}
			else if (type == OTHER_EVENT) {
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
						throw new RuntimeException("The class '" + className + "' is not a valid subclass of '"
								+ AbstractEvent.class.getName() + "'.", e);
					}
		
					final AbstractEvent event = InstantiationUtil.instantiate(clazz, AbstractEvent.class);
					event.read(deserializer);
		
					return event;
				}
				catch (Exception e) {
					throw new RuntimeException("Error while deserializing or instantiating event.", e);
				}
			} 
			else {
				throw new RuntimeException("Corrupt byte stream for event");
			}
		}
		finally {
			buffer.order(bufferOrder);
		}
	}

	// ------------------------------------------------------------------------
	// Buffer helpers
	// ------------------------------------------------------------------------

	public static Buffer toBuffer(AbstractEvent event) {
		final ByteBuffer serializedEvent = EventSerializer.toSerializedEvent(event);

		MemorySegment data = MemorySegmentFactory.wrap(serializedEvent.array());
		
		final Buffer buffer = new Buffer(data, FreeingBufferRecycler.INSTANCE, false);
		buffer.setSize(serializedEvent.remaining());

		return buffer;
	}

	public static AbstractEvent fromBuffer(Buffer buffer, ClassLoader classLoader) {
		return fromSerializedEvent(buffer.getNioBuffer(), classLoader);
	}
}
