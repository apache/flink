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

package org.apache.flink.runtime.operators.coordination;

/**
 * Interface for handlers of operator events on the operator side. Operator events are sent between
 * an {@link OperatorCoordinator} and a runtime operator (which registers this handler).
 *
 * <p>The counterpart to this handler is the {@link OperatorCoordinator#handleEventFromOperator(int,
 * int, OperatorEvent)} method.
 */
public interface OperatorEventHandler {

    /**
     * Handles an event sent by the {@link OperatorCoordinator}.
     *
     * <p>Events are handled in the order in which the coordinator sent them, and before any
     * checkpoint that the coordinator takes after sending them. They are not ordered with respect
     * to other work of the task, such as timers or checkpoint notifications: for example, an event
     * that the coordinator sends from {@link OperatorCoordinator#notifyCheckpointComplete} may be
     * handled before the operator's own {@code notifyCheckpointComplete} call for the same
     * checkpoint.
     */
    void handleOperatorEvent(OperatorEvent evt);
}
