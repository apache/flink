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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.AvailabilityProvider;

/**
 * Interface for operators that absorb downstream availability internally, handling back-pressure by
 * deferring emits and yielding the mailbox rather than blocking.
 *
 * <p>{@code OperatorChain} injects the composite downstream {@link AvailabilityProvider} via {@link
 * #setDownstreamAvailabilityProvider} and lets the operator represent the chain's availability.
 */
@Internal
public interface SupportsSoftBackpressure extends AvailabilityProvider {

    /** Called once by {@code OperatorChain} during construction to inject downstream availability. */
    void setDownstreamAvailabilityProvider(AvailabilityProvider provider);
}
