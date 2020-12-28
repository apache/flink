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

package org.apache.flink.runtime.registration;

import java.io.Serializable;

/** Base class for responses given to registration attempts from {@link RetryingRegistration}. */
public abstract class RegistrationResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    // ----------------------------------------------------------------------------

    /**
     * Base class for a successful registration. Concrete registration implementations will
     * typically extend this class to attach more information.
     */
    public static class Success extends RegistrationResponse {
        private static final long serialVersionUID = 1L;

        @Override
        public String toString() {
            return "Registration Successful";
        }
    }

    // ----------------------------------------------------------------------------

    /** A rejected (declined) registration. */
    public static final class Decline extends RegistrationResponse {
        private static final long serialVersionUID = 1L;

        /** The rejection reason. */
        private final String reason;

        /**
         * Creates a new rejection message.
         *
         * @param reason The reason for the rejection.
         */
        public Decline(String reason) {
            this.reason = reason != null ? reason : "(unknown)";
        }

        /** Gets the reason for the rejection. */
        public String getReason() {
            return reason;
        }

        @Override
        public String toString() {
            return "Registration Declined (" + reason + ')';
        }
    }
}
