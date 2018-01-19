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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Random;

import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage.decodePathFromReference;
import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage.encodePathAsReference;
import static org.junit.Assert.assertEquals;

/**
 * Tests for the encoding / decoding of storage location references.
 */
public class FsStorageLocationReferenceTest extends TestLogger {

	@Test
	public void testEncodeAndDecode() throws Exception {
		final Path path = randomPath(new Random());

		try {
			CheckpointStorageLocationReference ref = encodePathAsReference(path);
			Path decoded = decodePathFromReference(ref);

			assertEquals(path, decoded);
		}
		catch (Exception | Error e) {
			// if something goes wrong, help by printing the problematic path
			log.error("ERROR FOR PATH " + path);
			throw e;
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDecodingTooShortReference() {
		decodePathFromReference(new CheckpointStorageLocationReference(new byte[2]));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDecodingGarbage() {
		final byte[] bytes = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C };
		decodePathFromReference(new CheckpointStorageLocationReference(bytes));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDecodingDefaultReference() {
		decodePathFromReference(CheckpointStorageLocationReference.getDefault());
	}

	// ------------------------------------------------------------------------

	private static Path randomPath(Random rnd) {
		final StringBuilder path = new StringBuilder();

		// scheme
		path.append(StringUtils.getRandomString(rnd, 1, 5, 'a', 'z'));
		path.append("://");
		path.append(StringUtils.getRandomString(rnd, 10, 20)); // authority
		path.append(rnd.nextInt(50000) + 1); // port

		for (int i = rnd.nextInt(5) + 1; i > 0; i--) {
			path.append('/');
			path.append(StringUtils.getRandomString(rnd, 3, 15));
		}

		return new Path(path.toString());
	}
}
