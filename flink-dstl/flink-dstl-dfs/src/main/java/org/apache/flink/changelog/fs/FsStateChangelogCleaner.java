/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.changelog.fs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.ExceptionUtils;

import static org.apache.flink.changelog.fs.FsStateChangelogOptions.NUM_THREADS_CLEANUP;

/**
 * Cleans up the state NOT owned by JM. Currently, this is only non-reported to JM or aborted by JM.
 */
@Internal
public interface FsStateChangelogCleaner {

    void cleanupAsync(StoreResult storeResult);

    FsStateChangelogCleaner NO_OP = storeResult -> {};
    FsStateChangelogCleaner DIRECT =
            storeResult -> {
                try {
                    storeResult.getStreamStateHandle().discardState();
                } catch (Exception e) {
                    ExceptionUtils.rethrow(e);
                }
            };

    static FsStateChangelogCleaner fromConfig(ReadableConfig config) {
        return new FsStateChangelogCleanerImpl(config.get(NUM_THREADS_CLEANUP));
    }
}
