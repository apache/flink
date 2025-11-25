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

package org.apache.flink.python;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.runners.fnexecution.logging.LogWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Writer to append Python worker's log. */
public class FlinkSlf4jLogWriter implements LogWriter {

    static final Logger LOGGER = LoggerFactory.getLogger("PythonWorker");

    @Override
    public void log(BeamFnApi.LogEntry entry) {
        String location = entry.getLogLocation();
        String message = entry.getMessage();
        String trace = entry.getTrace();
        switch (entry.getSeverity()) {
            case ERROR:
            case CRITICAL:
                if (trace == null) {
                    LOGGER.error("{} {}", location, message);
                } else {
                    LOGGER.error("{} {} {}", location, message, trace);
                }
                break;
            case WARN:
                if (trace == null) {
                    LOGGER.warn("{} {}", location, message);
                } else {
                    LOGGER.warn("{} {} {}", location, message, trace);
                }
                break;
            case INFO:
            case NOTICE:
                LOGGER.info("{} {}", location, message);
                break;
            case DEBUG:
                LOGGER.debug("{} {}", location, message);
                break;
            case UNSPECIFIED:
            case TRACE:
                LOGGER.trace("{} {}", location, message);
                break;
            default:
                LOGGER.warn("Unknown message severity {}", entry.getSeverity());
                LOGGER.info("{} {}", location, message);
                break;
        }
    }
}
