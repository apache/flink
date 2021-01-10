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

package org.apache.flink.core.fs;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/** Various utility functions for testing {@link FileSystem} implementations. */
public class FileSystemTestUtils {

    /**
     * Verifies that the given path eventually appears on / disappears from <tt>fs</tt> within
     * <tt>consistencyToleranceNS</tt> nanoseconds.
     */
    public static void checkPathEventualExistence(
            FileSystem fs, Path path, boolean expectedExists, long consistencyToleranceNS)
            throws IOException, InterruptedException {
        boolean dirExists;
        long deadline = System.nanoTime() + consistencyToleranceNS;
        while ((dirExists = fs.exists(path)) != expectedExists
                && System.nanoTime() - deadline < 0) {
            Thread.sleep(10);
        }
        assertEquals(expectedExists, dirExists);
    }
}
