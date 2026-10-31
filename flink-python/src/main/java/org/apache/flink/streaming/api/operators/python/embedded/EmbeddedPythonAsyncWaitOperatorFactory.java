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

package org.apache.flink.streaming.api.operators.python.embedded;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.legacy.YieldingOperatorFactory;

/**
 * Factory for {@link EmbeddedPythonAsyncWaitOperator}. It stays a {@link SimpleOperatorFactory} so
 * that the Python configuration still applies, and implements {@link YieldingOperatorFactory} so
 * that the operator is not chained to a legacy source, whose thread cannot yield to the mailbox.
 */
@Internal
public class EmbeddedPythonAsyncWaitOperatorFactory<IN, OUT> extends SimpleOperatorFactory<OUT>
        implements OneInputStreamOperatorFactory<IN, OUT>, YieldingOperatorFactory<OUT> {

    private static final long serialVersionUID = 1L;

    public EmbeddedPythonAsyncWaitOperatorFactory(
            EmbeddedPythonAsyncWaitOperator<IN, OUT> operator) {
        super(operator);
    }
}
