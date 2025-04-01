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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.rest.handler.RestHandlerException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** Indicates coordinator of the operator does not exist. */
public class CoordinatorNotExistException extends RestHandlerException {
    private static final long serialVersionUID = 1L;

    public CoordinatorNotExistException(
            OperatorID operator, String otherPossibleCause, boolean logException) {
        super(
                "Coordinator of operator "
                        + operator
                        + " does not exist"
                        + (otherPossibleCause == null ? "." : " or " + otherPossibleCause),
                HttpResponseStatus.NOT_FOUND,
                logException ? LoggingBehavior.LOG : LoggingBehavior.IGNORE);
    }

    public CoordinatorNotExistException(OperatorID operator) {
        this(operator, null, true);
    }
}
