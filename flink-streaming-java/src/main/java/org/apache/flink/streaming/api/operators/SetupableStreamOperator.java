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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

/**
 * Stream operators can implement this interface if they need access to the context and the output.
 *
 * @param <OUT> The output type of the operator
 * @deprecated This class is deprecated in favour of using {@link StreamOperatorFactory} and it's
 *     {@link StreamOperatorFactory#createStreamOperator} and passing the required parameters to the
 *     Operator's constructor in create method.
 */
@Deprecated
@PublicEvolving
public interface SetupableStreamOperator<OUT> {

    /** Initializes the operator. Sets access to the context and the output. */
    void setup(
            StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output);

    ChainingStrategy getChainingStrategy();

    void setChainingStrategy(ChainingStrategy strategy);
}
