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

package org.apache.flink.connector.kafka.sink;

import org.apache.kafka.common.errors.RecordTooLargeException;
import org.slf4j.Logger;

/**
 * Handle exception for kafka sink to discard error records, it could be expended to other sinks if
 * needed.
 */
public interface WriterExceptionHandler {
    void onException(Exception originalException, Exception thrownException) throws Exception;

    static WriterExceptionHandler createHandler(boolean discardTooLargeRecords, Logger log) {
        if (discardTooLargeRecords) {
            return (original, thrown) -> {
                if (!(original instanceof RecordTooLargeException)) {
                    throw thrown;
                }
                log.warn("Discard record", original);
            };
        } else {
            return (original, thrown) -> {
                throw thrown;
            };
        }
    }
}
