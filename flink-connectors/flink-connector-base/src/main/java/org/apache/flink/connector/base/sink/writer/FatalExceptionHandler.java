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

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.List;

/**
 * This interface specifies how the sink should handle an exception raised by {@code
 * AsyncSinkWriter} that should not be retried. The mapping is provided by the end-user of a sink,
 * not the sink creator.
 *
 * @param <RequestEntryT> Corresponds to the type parameter of the same name in {@code *
 *     AsyncSinkWriter}
 */
@PublicEvolving
public interface FatalExceptionHandler<RequestEntryT> extends Serializable {
    void handle(List<RequestEntryT> failedRequestEntries, Exception fatalException)
            throws Exception;
}
