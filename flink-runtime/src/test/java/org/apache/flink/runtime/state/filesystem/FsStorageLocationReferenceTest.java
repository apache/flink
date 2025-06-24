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

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess.decodePathFromReference;
import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess.encodePathAsReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the encoding / decoding of storage location references. */
class FsStorageLocationReferenceTest {

    private static final Logger LOG = LoggerFactory.getLogger(FsStorageLocationReferenceTest.class);

    @Test
    void testEncodeAndDecode() throws Exception {
        final Path path = randomPath(new Random());

        try {
            CheckpointStorageLocationReference ref = encodePathAsReference(path);
            Path decoded = decodePathFromReference(ref);

            assertThat(decoded).isEqualTo(path);
        } catch (Exception | Error e) {
            // if something goes wrong, help by printing the problematic path
            LOG.error("ERROR FOR PATH " + path);
            throw e;
        }
    }

    @Test
    void testDecodingTooShortReference() {
        assertThatThrownBy(
                        () ->
                                decodePathFromReference(
                                        new CheckpointStorageLocationReference(new byte[2])))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDecodingGarbage() {
        final byte[] bytes =
                new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C};
        assertThatThrownBy(
                        () ->
                                decodePathFromReference(
                                        new CheckpointStorageLocationReference(bytes)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDecodingDefaultReference() {
        assertThatThrownBy(
                        () ->
                                decodePathFromReference(
                                        CheckpointStorageLocationReference.getDefault()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------------------------------------------------

    private static Path randomPath(Random rnd) {
        while (true) {
            try {
                final StringBuilder path = new StringBuilder();

                // scheme
                path.append(StringUtils.getRandomString(rnd, 1, 5, 'a', 'z'));
                path.append("://");
                path.append(StringUtils.getRandomString(rnd, 10, 20)); // authority
                path.append(":");
                path.append(rnd.nextInt(50000) + 1); // port

                for (int j = rnd.nextInt(5) + 1; j > 0; j--) {
                    path.append('/');
                    path.append(StringUtils.getRandomString(rnd, 3, 15));
                }

                return new Path(path.toString());
            } catch (Throwable t) {
                // ignore the exception and retry
            }
        }
    }
}
