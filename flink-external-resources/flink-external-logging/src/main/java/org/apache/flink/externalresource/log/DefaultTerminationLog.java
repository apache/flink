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

package org.apache.flink.externalresource.log;

import org.apache.flink.core.execution.TerminationLog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultTerminationLog implements TerminationLog {

    protected static final Logger LOG = LoggerFactory.getLogger(DefaultTerminationLog.class);

    /**
     * Default class used where it doesnt perform any operation.
     *
     * @param error
     */
    @Override
    public void writeTerminationLog(Throwable error, String errorCode) {
        LOG.info("Not performing any operation..!!");
        return;
    }
}
