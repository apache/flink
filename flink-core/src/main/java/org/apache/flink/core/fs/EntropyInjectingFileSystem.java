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

import org.apache.flink.annotation.PublicEvolving;

import javax.annotation.Nullable;

/**
 * An interface to be implemented by a {@link FileSystem} that is aware of entropy injection.
 *
 * <p>Entropy injection is a technique to spread files/objects across more parallel shards of a
 * distributed storage (typically object store) by adding random characters to the beginning of the
 * path/key and hence spearing the keys across a wider domain of prefixes.
 *
 * <p>Entropy injection typically works by having a recognized marker string in paths and replacing
 * that marker with random characters.
 *
 * <p>This interface is used in conjunction with the {@link EntropyInjector} (as a poor man's way to
 * build a mix-in in Java).
 */
@PublicEvolving
public interface EntropyInjectingFileSystem {

    /**
     * Gets the marker string that represents the substring of a path to be replaced by the entropy
     * characters.
     *
     * <p>You can disable entropy injection if you return null here.
     */
    @Nullable
    String getEntropyInjectionKey();

    /** Creates a string with random entropy to be injected into a path. */
    String generateEntropy();
}
