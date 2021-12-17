/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.runtime.source.coordinator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** A serialization util class for the {@link SourceCoordinator}. */
public class SourceCoordinatorSerdeUtils {

    public static final int VERSION_0 = 0;
    public static final int VERSION_1 = 1;

    /** The current source coordinator serde version. */
    private static final int CURRENT_VERSION = VERSION_1;

    /** Private constructor for utility class. */
    private SourceCoordinatorSerdeUtils() {}

    /** Write the current serde version. */
    static void writeCoordinatorSerdeVersion(DataOutputStream out) throws IOException {
        out.writeInt(CURRENT_VERSION);
    }

    /** Read and verify the serde version. */
    static int readAndVerifyCoordinatorSerdeVersion(DataInputStream in) throws IOException {
        int version = in.readInt();
        if (version > CURRENT_VERSION) {
            throw new IOException("Unsupported source coordinator serde version " + version);
        }
        return version;
    }

    static byte[] readBytes(DataInputStream in, int size) throws IOException {
        byte[] bytes = new byte[size];
        in.readFully(bytes);
        return bytes;
    }
}
