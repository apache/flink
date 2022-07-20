/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.slf4j.Logger;

/** Interface to interact with optional Scala field accessors. */
public interface ScalaProductFieldAccessorFactory {

    /** Returns a product {@link FieldAccessor} that does not support recursion. */
    <T, F> FieldAccessor<T, F> createSimpleProductFieldAccessor(
            int pos, TypeInformation<T> typeInfo, ExecutionConfig config);

    /** Returns a product {@link FieldAccessor} that does support recursion. */
    <T, R, F> FieldAccessor<T, F> createRecursiveProductFieldAccessor(
            int pos,
            TypeInformation<T> typeInfo,
            FieldAccessor<R, F> innerAccessor,
            ExecutionConfig config);

    /**
     * Loads the implementation, if it is accessible.
     *
     * @param log Logger to be used in case the loading fails
     * @return Loaded implementation, if it is accessible.
     */
    static ScalaProductFieldAccessorFactory load(Logger log) {
        try {
            final Object factory =
                    Class.forName(
                                    "org.apache.flink.streaming.util.typeutils.DefaultScalaProductFieldAccessorFactory")
                            .getDeclaredConstructor()
                            .newInstance();
            return (ScalaProductFieldAccessorFactory) factory;
        } catch (Exception e) {
            log.debug("Unable to load Scala API extension.", e);
            return null;
        }
    }
}
