/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.runtime.operators.windowing.buffers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.Serializable;

/**
 * A factory for {@link WindowBuffer WindowBuffers}.
 *
 * @param <T> The type of elements that the created {@code WindowBuffer} can store.
 * @param <O> The type of elements that the created buffer will return when asked for its contents.
 * @param <B> The type of the created {@code WindowBuffer}
 */
@Internal
public interface WindowBufferFactory<T, O, B extends WindowBuffer<T, O>> extends Serializable {

	/**
	 * Creates a new {@code WindowBuffer}.
	 */
	B create();

	/**
	 * Restores a {@code WindowBuffer} from a previous snapshot written using
	 * {@link WindowBuffer#snapshot(DataOutputView)}.
	 */
	B restoreFromSnapshot(DataInputView in) throws IOException;
}
