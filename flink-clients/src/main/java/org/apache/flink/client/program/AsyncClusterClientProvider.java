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
 *
 */

package org.apache.flink.client.program;

import org.apache.flink.annotation.Internal;

/** Provider for {@link ClusterClient ClusterClients}. */
@Internal
public abstract class AsyncClusterClientProvider<T> implements ClusterClientProvider<T> {
    private volatile boolean submissionFinished = false;

    protected abstract ClusterClient<T> buildClusterClient();

    protected abstract boolean waitForSubmissionFinished();

    @Override
    public ClusterClient<T> getClusterClient() {
        if (!submissionFinished) {
            synchronized (AsyncClusterClientProvider.class) {
                if (!submissionFinished && waitForSubmissionFinished()) {
                    this.submissionFinished = true;
                }
            }
        }
        return buildClusterClient();
    }
}
