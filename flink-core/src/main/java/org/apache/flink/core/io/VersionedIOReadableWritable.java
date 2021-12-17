/*
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

package org.apache.flink.core.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Arrays;

/**
 * This is the abstract base class for {@link IOReadableWritable} which allows to differentiate
 * between serialization versions. Concrete subclasses should typically override the {@link
 * #write(DataOutputView)} and {@link #read(DataInputView)}, thereby calling super to ensure version
 * checking.
 */
@Internal
public abstract class VersionedIOReadableWritable implements IOReadableWritable, Versioned {

    private int readVersion = Integer.MIN_VALUE;

    @Override
    public void write(DataOutputView out) throws IOException {
        out.writeInt(getVersion());
    }

    @Override
    public void read(DataInputView in) throws IOException {
        this.readVersion = in.readInt();
        resolveVersionRead(readVersion);
    }

    /**
     * Returns the found serialization version. If this instance was not read from serialized bytes
     * but simply instantiated, then the current version is returned.
     *
     * @return the read serialization version, or the current version if the instance was not read
     *     from bytes.
     */
    public int getReadVersion() {
        return (readVersion == Integer.MIN_VALUE) ? getVersion() : readVersion;
    }

    /**
     * Returns the compatible version values.
     *
     * <p>By default, the base implementation recognizes only the current version (identified by
     * {@link #getVersion()}) as compatible. This method can be used as a hook and may be overridden
     * to identify more compatible versions.
     *
     * @return an array of integers representing the compatible version values.
     */
    public int[] getCompatibleVersions() {
        return new int[] {getVersion()};
    }

    private void resolveVersionRead(int readVersion) throws VersionMismatchException {

        int[] compatibleVersions = getCompatibleVersions();
        for (int compatibleVersion : compatibleVersions) {
            if (compatibleVersion == readVersion) {
                return;
            }
        }

        throw new VersionMismatchException(
                "Incompatible version: found "
                        + readVersion
                        + ", compatible versions are "
                        + Arrays.toString(compatibleVersions));
    }
}
