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

package org.apache.flink.testutils.junit;

import java.io.Serializable;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Represents an object managed in a {@link SharedObjects}. The reference can be serialized and will
 * still point to the same instance after deserialization in the same JVM. The underlying object may
 * change the state but this reference will never point to another object.
 *
 * @param <T> the type of the represented object
 */
public interface SharedReference<T> extends Serializable {
    /**
     * Returns the referenced object without giving any visibility guarantees. This method should
     * only be used on thread-safe classes.
     */
    T get();

    /**
     * Executes the code on the referenced object in a synchronized fashion. Note that this method
     * is prone to deadlock if multiple references are accessed in a synchronized fashion in a
     * nested call-chain.
     */
    default <R> R applySync(Function<T, R> function) {
        T object = get();
        synchronized (object) {
            return function.apply(object);
        }
    }

    /**
     * Executes the code on the referenced object in a synchronized fashion. Note that this method
     * is prone to deadlock if multiple references are accessed in a synchronized fashion in a
     * nested call-chain.
     */
    default void consumeSync(Consumer<T> consumer) {
        T object = get();
        synchronized (object) {
            consumer.accept(object);
        }
    }
}
