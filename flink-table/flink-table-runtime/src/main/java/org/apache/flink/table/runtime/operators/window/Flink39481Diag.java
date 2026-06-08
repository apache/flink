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

package org.apache.flink.table.runtime.operators.window;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TEMPORARY diagnostic helper for FLINK-39481 (window-aggregate exactly-once data loss after
 * restore). DO NOT MERGE.
 *
 * <p>All output goes to the dedicated logger {@code org.apache.flink.FLINK39481} and is prefixed
 * with the token {@code FLINK39481} so it can be grepped out of CI logs with a single pattern. The
 * logger is at {@code DEBUG}, so it is a no-op in production (root logger is INFO/WARN) and only
 * emits when a test/CI log configuration explicitly enables it. Hot paths must still be guarded
 * with {@link #on()} to avoid building messages when disabled.
 */
public final class Flink39481Diag {

    /** Dedicated, greppable logger. Enable at DEBUG in the relevant log4j2 test configuration. */
    public static final Logger LOG = LoggerFactory.getLogger("org.apache.flink.FLINK39481");

    private Flink39481Diag() {}

    /** Whether diagnostic logging is enabled. Guard hot paths with this. */
    public static boolean on() {
        return LOG.isDebugEnabled();
    }

    /** Log a single diagnostic line (prefixed with the {@code FLINK39481} grep token). */
    public static void log(String format, Object... args) {
        LOG.debug("FLINK39481 " + format, args);
    }
}
