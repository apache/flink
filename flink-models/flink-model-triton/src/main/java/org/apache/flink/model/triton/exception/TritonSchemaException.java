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

package org.apache.flink.model.triton.exception;

/**
 * Exception for schema/shape/type mismatch errors.
 *
 * <p>Indicates that the Flink data type or tensor shape does not match what the Triton model
 * expects. These errors are NOT retryable as they require schema or configuration fixes.
 *
 * <p><b>Common Scenarios:</b>
 *
 * <ul>
 *   <li>Shape mismatch: Sent [1,224,224,3] but model expects [1,3,224,224]
 *   <li>Type mismatch: Sent FP32 but model expects INT8
 *   <li>Dimension error: Sent scalar but model expects array
 * </ul>
 *
 * <p>This exception includes detailed information about both expected and actual schemas to help
 * users diagnose and fix the issue.
 */
public class TritonSchemaException extends TritonException {
    private static final long serialVersionUID = 1L;

    private final String expectedSchema;
    private final String actualSchema;

    /**
     * Creates a new schema exception.
     *
     * @param message The detailed error message
     * @param expectedSchema The schema/shape expected by Triton model
     * @param actualSchema The schema/shape actually sent
     */
    public TritonSchemaException(String message, String expectedSchema, String actualSchema) {
        super(
                String.format(
                        "%s\n=== Expected Schema ===\n%s\n=== Actual Schema ===\n%s",
                        message, expectedSchema, actualSchema));
        this.expectedSchema = expectedSchema;
        this.actualSchema = actualSchema;
    }

    /**
     * Returns the schema/shape expected by the Triton model.
     *
     * @return The expected schema description
     */
    public String getExpectedSchema() {
        return expectedSchema;
    }

    /**
     * Returns the schema/shape that was actually sent.
     *
     * @return The actual schema description
     */
    public String getActualSchema() {
        return actualSchema;
    }

    @Override
    public boolean isRetryable() {
        // Schema errors require configuration fixes, not retries
        return false;
    }

    @Override
    public ErrorCategory getCategory() {
        return ErrorCategory.SCHEMA_ERROR;
    }
}
