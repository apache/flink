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

package org.apache.flink.client.program;

import org.apache.flink.api.common.JobID;

/** Exception used to indicate that there is an error during the invocation of a Flink program. */
public class ProgramInvocationException extends Exception {
    /** Serial version UID for serialization interoperability. */
    private static final long serialVersionUID = -2417524218857151612L;

    /**
     * Creates a <tt>ProgramInvocationException</tt> with the given message.
     *
     * @param message The message for the exception.
     */
    public ProgramInvocationException(String message) {
        super(message);
    }

    /**
     * Creates a <tt>ProgramInvocationException</tt> with the given message which contains job id.
     *
     * @param message The additional message.
     * @param jobID ID of failed job.
     */
    public ProgramInvocationException(String message, JobID jobID) {
        super(message + " (JobID: " + jobID + ")");
    }

    /**
     * Creates a <tt>ProgramInvocationException</tt> for the given exception.
     *
     * @param cause The exception that causes the program invocation to fail.
     */
    public ProgramInvocationException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates a <tt>ProgramInvocationException</tt> for the given exception with an additional
     * message.
     *
     * @param message The additional message.
     * @param cause The exception that causes the program invocation to fail.
     */
    public ProgramInvocationException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a <tt>ProgramInvocationException</tt> for the given exception with an additional
     * message which contains job id.
     *
     * @param message The additional message.
     * @param jobID ID of failed job.
     * @param cause The exception that causes the program invocation to fail.
     */
    public ProgramInvocationException(String message, JobID jobID, Throwable cause) {
        super(message + " (JobID: " + jobID + ")", cause);
    }
}
