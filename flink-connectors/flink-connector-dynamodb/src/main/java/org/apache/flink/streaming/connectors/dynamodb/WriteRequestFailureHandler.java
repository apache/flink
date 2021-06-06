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

package org.apache.flink.streaming.connectors.dynamodb;

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

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * An implementation of {@link WriteRequestFailureHandler} is provided by the user to define how
 * failed {@link ProducerWriteRequest WriteRequest} should be handled, e.g. dropping them,
 * reprocessing malformed documents, or sending them to a dead letter queue.
 */
@PublicEvolving
public interface WriteRequestFailureHandler extends Serializable {

    /**
     * Handle a failed {@link ProducerWriteRequest}.
     *
     * @param request the {@link ProducerWriteRequest} that failed due to the failure
     * @param failure the cause of failure
     * @throws Throwable if the sink should fail on this failure, the implementation should rethrow
     *     the exception or a custom one
     */
    void onFailure(ProducerWriteRequest request, Throwable failure) throws Throwable;
}
