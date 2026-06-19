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

package org.apache.flink.runtime.throwable;

/** Enum for the classification of {@link Throwable} objects into failure/recovery classes. */
public enum ThrowableType {

    /**
     * This indicates error that would not succeed even with retry, such as DivideZeroException. No
     * recovery attempt should happen for such an error. Instead, the job should fail immediately.
     */
    NonRecoverableError,

    /** Data consumption error, which indicates that we should revoke the producer. */
    PartitionDataMissingError,

    /**
     * This indicates an error related to the running environment, such as hardware error, service
     * issue, in which case we should consider blacklisting the machine.
     */
    EnvironmentError,

    /** This indicates a problem that is recoverable. */
    RecoverableError
}
