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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;

/**
 * Interface to handle file state handle serialization in the {@link GenericSavepointSerializer}.
 */
interface FileStateHandleSerializer {

	/**
	 * Serialize the file stream state handle <strong>without</strong> writing
	 * the leading byte for the stream handle type. Only worry about the actual
	 * file stream handle, please.
	 *
	 * @param fileStateHandle FileStateHandle to serialize
	 * @param basePath Base path of the savepoint
	 * @param dos DataOutputStream to serialize handle to
	 * @throws IOException Failures during serialization are forwarded
	 */
	void serializeFileStreamStateHandle(FileStateHandle fileStateHandle, Path basePath, DataOutputStream dos) throws IOException;

	/**
	 * Deserialize the file stream state handle <strong>without</strong> reading
	 * the leading byte for the stream handle type. Only worry about the actual
	 * file stream handle, please.
	 *
	 * @param basePath Base path of the savepoint
	 * @param dis DataInputStream to deserialize handle from
	 * @return Deserialized FileStateHandle
	 * @throws IOException Failures during serialization are forwarded
	 */
	FileStateHandle deserializeFileStreamStateHandle(Path basePath, DataInputStream dis) throws IOException;

}
