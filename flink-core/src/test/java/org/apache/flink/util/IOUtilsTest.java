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

package org.apache.flink.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Tests for the {@link IOUtils}. */
public class IOUtilsTest extends TestLogger {

    @Test
    public void testTryReadFullyFromLongerStream() throws IOException {
        ByteArrayInputStream inputStream =
                new ByteArrayInputStream("test-data".getBytes(StandardCharsets.UTF_8));

        byte[] out = new byte[4];
        int read = IOUtils.tryReadFully(inputStream, out);

        Assert.assertArrayEquals(
                "test".getBytes(StandardCharsets.UTF_8), Arrays.copyOfRange(out, 0, read));
    }

    @Test
    public void testTryReadFullyFromShorterStream() throws IOException {
        ByteArrayInputStream inputStream =
                new ByteArrayInputStream("t".getBytes(StandardCharsets.UTF_8));

        byte[] out = new byte[4];
        int read = IOUtils.tryReadFully(inputStream, out);

        Assert.assertArrayEquals(
                "t".getBytes(StandardCharsets.UTF_8), Arrays.copyOfRange(out, 0, read));
    }
}
