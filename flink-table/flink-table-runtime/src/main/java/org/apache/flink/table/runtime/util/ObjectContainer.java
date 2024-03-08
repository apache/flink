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
 *
 */

package org.apache.flink.table.runtime.util;

import org.apache.flink.annotation.Internal;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * This class is used for scalar function, in that it is used for comparing Objects using code
 * generated hashCode and equals instead of using the Object#equals/Object#hashcode versions.
 */
@Internal
public class ObjectContainer {

    private final Object o;

    private final BiFunction<Object, Object, Boolean> equalsMethod;

    private final Function<Object, Integer> hashCodeMethod;

    public ObjectContainer(
            Object o,
            BiFunction<Object, Object, Boolean> equalsMethod,
            Function<Object, Integer> hashCodeMethod) {
        this.o = o;
        this.equalsMethod = equalsMethod;
        this.hashCodeMethod = hashCodeMethod;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ObjectContainer)) {
            return false;
        }
        ObjectContainer that = (ObjectContainer) other;
        return equalsMethod.apply(this.o, that.o);
    }

    @Override
    public int hashCode() {
        return hashCodeMethod.apply(o);
    }
}
