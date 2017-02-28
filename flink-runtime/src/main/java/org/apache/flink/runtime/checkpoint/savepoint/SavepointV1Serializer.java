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

package org.apache.flink.runtime.checkpoint.savepoint;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.TaskState;

/**
 * Serializer for {@link SavepointV1} instances.
 *
 * <p>In contrast to previous savepoint versions, this serializer makes sure
 * that no default Java serialization is used for serialization. Therefore, we
 * don't rely on any involved Java classes to stay the same.
 *
 * @deprecated Deprecated in favour of {@link SavepointV2Serializer}. This
 * serializer is only used to deserialize V1 savepoints.
 */
@Deprecated
class SavepointV1Serializer extends AbstractSavepointSerializer<SavepointV1> {

	public static final SavepointV1Serializer INSTANCE = new SavepointV1Serializer();

	private SavepointV1Serializer() {
	}

	@Override
	SavepointV1 createSavepoint(long checkpointId, Collection<TaskState> taskStates) {
		return new SavepointV1(checkpointId, taskStates);
	}

	@Override
	public void serialize(SavepointV1 savepoint, Path basePath, DataOutputStream dos) throws IOException {
		throw new UnsupportedOperationException("This serializer has been deprecated for "
			+ "serializing savepoints. You should only use it to _de_serialize SavepointV1 instances.");
	}
}
