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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.table.functions.ProcessTableFunction;

import javax.annotation.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * A resolved PTF method (eval or onTimer) paired with whether its first parameter accepts a
 * Context.
 */
class ResolvedMethod<T extends ProcessTableFunction.Context> {
    final Method method;
    final boolean takesContext;

    static <T extends ProcessTableFunction.Context> ResolvedMethod<T> of(
            Method method, Class<T> contextClass) {
        boolean takesContext =
                method.getParameterTypes().length > 0
                        && contextClass.isAssignableFrom(method.getParameterTypes()[0]);
        return new ResolvedMethod<>(method, takesContext);
    }

    private ResolvedMethod(Method method, boolean takesContext) {
        this.method = method;
        this.takesContext = takesContext;
    }

    void invoke(Object target, @Nullable T context, Object[] methodArgs)
            throws InvocationTargetException, IllegalAccessException {
        if (takesContext) {
            Object[] args = new Object[1 + methodArgs.length];
            args[0] = context;
            System.arraycopy(methodArgs, 0, args, 1, methodArgs.length);
            method.invoke(target, args);
        } else {
            method.invoke(target, methodArgs);
        }
    }
}
