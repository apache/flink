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

package org.apache.flink.formats.protobuf;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Singleton class for generating variable suffix number globally to avoid conflict in codegen
 * sections. It can be only used in protobuf format code.
 */
public class PbCodegenVarId {
    private static PbCodegenVarId varUid = new PbCodegenVarId();
    private AtomicInteger atomicInteger = new AtomicInteger();

    private PbCodegenVarId() {}

    public static PbCodegenVarId getInstance() {
        return varUid;
    }

    public int getAndIncrement() {
        return atomicInteger.getAndIncrement();
    }
}
