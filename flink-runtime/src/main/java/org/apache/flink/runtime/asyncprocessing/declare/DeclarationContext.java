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

package org.apache.flink.runtime.asyncprocessing.declare;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import javax.annotation.Nullable;

import java.util.function.Supplier;

/** A context to declare parts of process in user-defined function/operator. */
public class DeclarationContext {

    private static final String AUTO_NAME_PREFIX = "F";

    private final DeclarationManager manager;

    public DeclarationContext(DeclarationManager manager) {
        this.manager = manager;
    }

    // ------------- Declaring Callback part ----------------

    /** Declare a callback with a name. */
    public <T> NamedConsumer<T> declare(
            String name, ThrowingConsumer<T, ? extends Exception> callback)
            throws DeclarationException {
        return manager.register(new NamedConsumer<>(name, callback));
    }

    /** Declare a callback with a name. */
    public <T, V> NamedFunction<T, V> declare(
            String name, FunctionWithException<T, V, ? extends Exception> callback)
            throws DeclarationException {
        return manager.register(new NamedFunction<>(name, callback));
    }

    /** Declare a callback with a name. */
    public <T, U, V> NamedBiFunction<T, U, V> declare(
            String name, BiFunctionWithException<T, U, V, ? extends Exception> callback)
            throws DeclarationException {
        return manager.register(new NamedBiFunction<>(name, callback));
    }

    /** Declare a callback with an automatically assigned name. */
    public <T> NamedConsumer<T> declare(ThrowingConsumer<T, ? extends Exception> callback)
            throws DeclarationException {
        return declare(manager.nextAssignedName(AUTO_NAME_PREFIX), callback);
    }

    /** Declare a callback with an automatically assigned name. */
    public <T, V> NamedFunction<T, V> declare(
            FunctionWithException<T, V, ? extends Exception> callback) throws DeclarationException {
        return declare(manager.nextAssignedName(AUTO_NAME_PREFIX), callback);
    }

    /** Declare a callback with an automatically assigned name. */
    public <T, U, V> NamedBiFunction<T, U, V> declare(
            BiFunctionWithException<T, U, V, ? extends Exception> callback)
            throws DeclarationException {
        return declare(manager.nextAssignedName(AUTO_NAME_PREFIX), callback);
    }

    /**
     * Declare a variable that will keep value across callback with same context.
     *
     * @param serializer the serializer of variable.
     * @param name the name.
     * @param initialValue the initializer of variable. Can be null if no need to initialize.
     * @param <T> The type of value.
     */
    public <T> DeclaredVariable<T> declareVariable(
            TypeSerializer<T> serializer, String name, @Nullable Supplier<T> initialValue)
            throws DeclarationException {
        return manager.registerVariable(serializer, name, initialValue);
    }

    /**
     * Declare a variable that will keep value across callback with same context. This value cannot
     * be serialized into checkpoint.
     *
     * @param initializer the initializer of variable. Can be null if no need to initialize.
     * @param <T> The type of value.
     */
    public <T> ContextVariable<T> declareVariable(@Nullable Supplier<T> initializer)
            throws DeclarationException {
        return manager.registerVariable(initializer);
    }

    /**
     * Declaring a processing in chain-style. This method start a chain with an input type.
     *
     * @return the chain itself.
     * @param <IN> the in type of the first block.
     */
    public <IN> DeclarationChain<IN>.DeclarationStage<IN> declareChain()
            throws DeclarationException {
        return new DeclarationChain<IN>(this).firstStage();
    }
}
