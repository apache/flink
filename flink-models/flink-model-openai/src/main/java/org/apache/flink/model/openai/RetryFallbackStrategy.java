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

package org.apache.flink.model.openai;

import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.flink.configuration.description.TextElement.text;

/**
 * The fallback strategy for when retry attempts are exhausted. It is identical to {@link
 * ErrorHandlingStrategy} except that it does not support {@link ErrorHandlingStrategy#RETRY}.
 */
public enum RetryFallbackStrategy implements DescribedEnum {
    FAILOVER(ErrorHandlingStrategy.FAILOVER),
    IGNORE(ErrorHandlingStrategy.IGNORE);

    final ErrorHandlingStrategy strategy;

    RetryFallbackStrategy(ErrorHandlingStrategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public InlineElement getDescription() {
        return text(strategy.description);
    }
}
