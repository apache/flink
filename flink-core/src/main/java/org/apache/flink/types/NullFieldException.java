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

package org.apache.flink.types;

import org.apache.flink.annotation.Public;

/**
 * An exception specifying that a required field was not set in a record, i.e. was <code>null</code>
 * .
 */
@Public
public class NullFieldException extends RuntimeException {
    /** UID for serialization interoperability. */
    private static final long serialVersionUID = -8820467525772321173L;

    private final int fieldPos;

    /** Constructs an {@code NullFieldException} with {@code null} as its error detail message. */
    public NullFieldException() {
        super();
        this.fieldPos = -1;
    }

    /**
     * Constructs an {@code NullFieldException} with the specified detail message.
     *
     * @param message The detail message.
     */
    public NullFieldException(String message) {
        super(message);
        this.fieldPos = -1;
    }

    /**
     * Constructs an {@code NullFieldException} with a default message, referring to given field
     * number as the null field.
     *
     * @param fieldIdx The index of the field that was null, but expected to hold a value.
     */
    public NullFieldException(int fieldIdx) {
        super("Field " + fieldIdx + " is null, but expected to hold a value.");
        this.fieldPos = fieldIdx;
    }

    /**
     * Constructs an {@code NullFieldException} with a default message, referring to given field
     * number as the null field and a cause (Throwable)
     *
     * @param fieldIdx The index of the field that was null, but expected to hold a value.
     * @param cause Pass the root cause of the error
     */
    public NullFieldException(int fieldIdx, Throwable cause) {
        super("Field " + fieldIdx + " is null, but expected to hold a value.", cause);
        this.fieldPos = fieldIdx;
    }

    /**
     * Gets the field number that was attempted to access. If the number is not set, this method
     * returns {@code -1}.
     *
     * @return The field number that was attempted to access, or {@code -1}, if not set.
     */
    public int getFieldPos() {
        return this.fieldPos;
    }
}
