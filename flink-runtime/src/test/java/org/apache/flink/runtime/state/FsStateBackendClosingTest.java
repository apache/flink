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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend.FsCheckpointStateOutputStream;
import org.apache.flink.runtime.util.EnvironmentInformation;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FsStateBackendClosingTest {

	@Test
	public void testStateBackendClosesStreams() throws Exception {
		final URI tempFolder = new File(EnvironmentInformation.getTemporaryFileDirectory()).toURI();
		final FsStateBackend backend = new FsStateBackend(tempFolder);

		backend.initializeForJob(new DummyEnvironment("test", 1, 0), "test_op", IntSerializer.INSTANCE);

		FsCheckpointStateOutputStream stream = backend.createCheckpointStateOutputStream(17L, 12345L);

		// stream is open, this should succeed
		assertFalse(stream.isClosed());
		stream.write(1);

		// close the backend - that should close the stream
		backend.close();

		assertTrue(stream.isClosed());

		try {
			stream.write(2);
			fail("stream is closed, 'write(int)' should fail with an exception");
		}
		catch (IOException e) {
			// expected
		}
	}
}
