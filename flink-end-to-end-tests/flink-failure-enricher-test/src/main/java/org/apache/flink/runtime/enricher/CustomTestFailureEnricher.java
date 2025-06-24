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

package org.apache.flink.runtime.enricher;

import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.util.FlinkException;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** The custom enricher for test. */
public class CustomTestFailureEnricher implements FailureEnricher {

    private final Set<String> outputKeys;

    public CustomTestFailureEnricher() {
        this.outputKeys = Collections.singleton("type");
    }

    @Override
    public Set<String> getOutputKeys() {
        return outputKeys;
    }

    @Override
    public CompletableFuture<Map<String, String>> processFailure(Throwable cause, Context context) {
        if (cause instanceof FlinkException) {
            return CompletableFuture.completedFuture(Collections.singletonMap("type", "system"));
        } else {
            return CompletableFuture.completedFuture(Collections.singletonMap("type", "user"));
        }
    }
}
