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

package org.apache.flink.formats.avro.typeutils;

import org.apache.flink.annotation.Internal;

/**
 * Simple helper class to initialize the concurrency checks for tests.
 *
 * <p>The flag is automatically set to true when assertions are activated (tests) and can be set to
 * true manually in other tests as well;
 */
@Internal
class AvroSerializerDebugInitHelper {

    /**
     * This captures the initial setting after initialization. It is used to validate in tests that
     * we never change the default to true.
     */
    static final boolean INITIAL_SETTING;

    /** The flag that is used to initialize the KryoSerializer's concurrency check flag. */
    static boolean setToDebug = false;

    static {
        // capture the default setting, for tests
        INITIAL_SETTING = setToDebug;

        // if assertions are active, the check should be activated
        //noinspection AssertWithSideEffects,ConstantConditions
        assert setToDebug = true;
    }
}
