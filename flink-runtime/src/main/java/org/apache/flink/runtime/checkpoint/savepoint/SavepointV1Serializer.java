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

import org.apache.flink.annotation.Internal;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * Deserializer for checkpoints written in format {@code 1} (Flink 1.2.x format).
 * This class is only retained to give a better error message: Rather than getting a "unknown version",
 * the user gets a "version no longer supported".
 */
@Internal
public class SavepointV1Serializer implements SavepointSerializer {

	/** The savepoint version. */
	public static final int VERSION = 1;

	public static final SavepointV1Serializer INSTANCE = new SavepointV1Serializer();

	private SavepointV1Serializer() {}

	@Override
	public SavepointV2 deserialize(DataInputStream dis, ClassLoader cl) throws IOException {
		throw new IOException("This savepoint / checkpoint version (Flink 1.1 / 1.2) is no longer supported.");
	}
}
