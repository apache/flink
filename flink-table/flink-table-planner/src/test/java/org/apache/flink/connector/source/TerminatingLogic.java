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

package org.apache.flink.connector.source;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Tells sources created from {@link TestValuesTableFactory} how they should behave after they
 * produced all data. It is separate from 'bounded', because even if a source is unbounded it can
 * stop producing records and shutdown.
 */
public enum TerminatingLogic {
    INFINITE,
    FINITE;

    public static TerminatingLogic readFrom(DataInputStream in) throws IOException {
        final boolean isInfinite = in.readBoolean();
        return isInfinite ? TerminatingLogic.INFINITE : TerminatingLogic.FINITE;
    }

    public static void writeTo(DataOutputStream out, TerminatingLogic terminatingLogic)
            throws IOException {
        out.writeBoolean(terminatingLogic == TerminatingLogic.INFINITE);
    }
}
