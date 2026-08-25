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

package org.apache.flink.runtime.webmonitor.history.retaining;

import org.apache.flink.core.fs.FileStatus;

/** To define the strategy interface to judge whether the file should be retained. */
public interface ArchiveRetainedStrategy {

    /**
     * Judge whether the file should be retained.
     *
     * @param file the target file to judge.
     * @param fileOrderedIndex the specified order index position of the target file,
     * @return The result that indicates whether the file should be retained.
     */
    boolean shouldRetain(FileStatus file, int fileOrderedIndex);

    /**
     * Judge whether the file is rejected specifically because it has exceeded its configured
     * time-to-live, as opposed to being rejected by a count-based retention limit.
     *
     * <p>This allows callers that want to treat count-limit rejections differently from TTL expiry
     * (e.g. to only stop archiving locally without affecting TTL-based remote deletion) to
     * distinguish the two cases.
     *
     * @param file the target file to judge.
     * @return {@code true} if the file is rejected due to TTL expiry.
     */
    default boolean isExpiredByTtl(FileStatus file) {
        return false;
    }
}
