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

package org.apache.flink.queryablestate.client;

import org.apache.flink.annotation.Internal;

import java.io.ObjectStreamException;

/**
 * Singleton placeholder class for state without a namespace.
 *
 * <p><b>THIS WAS COPIED FROM RUNTIME SO THAT WE AVOID THE DEPENDENCY.</b>
 */
@Internal
public final class VoidNamespace {

    // ------------------------------------------------------------------------
    //  Singleton instance.
    // ------------------------------------------------------------------------

    /** The singleton instance. */
    public static final VoidNamespace INSTANCE = new VoidNamespace();

    /** Getter for the singleton instance. */
    public static VoidNamespace get() {
        return INSTANCE;
    }

    /** This class should not be instantiated. */
    private VoidNamespace() {}

    // ------------------------------------------------------------------------
    //  Standard Utilities
    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return 99;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    // ------------------------------------------------------------------------
    //  Singleton serialization
    // ------------------------------------------------------------------------

    // make sure that we preserve the singleton properly on serialization
    private Object readResolve() throws ObjectStreamException {
        return INSTANCE;
    }
}
