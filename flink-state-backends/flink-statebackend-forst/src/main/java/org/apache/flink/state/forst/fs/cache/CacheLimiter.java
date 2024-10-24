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

package org.apache.flink.state.forst.fs.cache;

/** Space checker. */
public interface CacheLimiter {
    /** Type of file space checker. */
    enum Type {
        // default production space checker of
        OS,
        FIXED_CAPACITY,
    }

    /**
     * Whether the cache usage is exceeded the upperbound.
     *
     * @param toAddSize
     * @return true if the cache usage is overflow, false otherwise.
     */
    boolean isOverflow(long toAddSize);

    /**
     * Acquire cache.
     *
     * @param toAddSize
     * @return true if the space is enough, false otherwise.
     */
    boolean acquire(long toAddSize);

    /**
     * Release cache.
     *
     * @param toReleaseSize
     */
    void release(long toReleaseSize);
}
