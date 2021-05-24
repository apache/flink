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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import org.apache.flink.annotation.Internal;

import com.esotericsoftware.minlog.Log;
import com.esotericsoftware.minlog.Log.Logger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** An implementation of the Minlog Logger that forwards to slf4j. */
@Internal
class MinlogForwarder extends Logger {

    private final org.slf4j.Logger log;

    MinlogForwarder(org.slf4j.Logger log) {
        this.log = checkNotNull(log);
    }

    @Override
    public void log(int level, String category, String message, Throwable ex) {
        final String logString = "[KRYO " + category + "] " + message;
        switch (level) {
            case Log.LEVEL_ERROR:
                log.error(logString, ex);
                break;
            case Log.LEVEL_WARN:
                log.warn(logString, ex);
                break;
            case Log.LEVEL_INFO:
                log.info(logString, ex);
                break;
            case Log.LEVEL_DEBUG:
                log.debug(logString, ex);
                break;
            case Log.LEVEL_TRACE:
                log.trace(logString, ex);
                break;
        }
    }
}
