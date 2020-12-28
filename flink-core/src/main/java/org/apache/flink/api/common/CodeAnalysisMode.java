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

package org.apache.flink.api.common;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;

/**
 * @deprecated The code analysis code has been removed and this enum has no effect. <b>NOTE</b> It
 *     can not be removed from the codebase for now, because it had been serialized as part of the
 *     {@link ExecutionConfig} which in turn had been serialized as part of the {@link
 *     PojoSerializer}.
 *     <p>This class can be removed when we drop support for pre 1.8 serializer snapshots that
 *     contained java serialized serializers ({@link TypeSerializerConfigSnapshot}).
 */
@PublicEvolving
@Deprecated
public enum CodeAnalysisMode {

    /** Code analysis does not take place. */
    DISABLE,

    /** Hints for improvement of the program are printed to the log. */
    HINT,

    /** The program will be automatically optimized with knowledge from code analysis. */
    OPTIMIZE;
}
