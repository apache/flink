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

/** Strategy for handling errors during OpenAI model requests. */
public enum ErrorHandlingStrategy implements DescribedEnum {
    RETRY("Retry sending the request."),
    FAILOVER("Throw exceptions and fail the Flink job."),
    IGNORE(
            "Ignore the input that caused the error and continue. The error itself would be recorded in log.");

    final String description;

    ErrorHandlingStrategy(String description) {
        this.description = description;
    }

    @Override
    public InlineElement getDescription() {
        return text(description);
    }
}
