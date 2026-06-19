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

package org.apache.flink.table.runtime.operators;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.runtime.generated.GeneratedClass;

/** Stream operator factory for code gen operator. */
public class CodeGenOperatorFactory<OUT> extends AbstractStreamOperatorFactory<OUT> {

    private final GeneratedClass<? extends StreamOperator<OUT>> generatedClass;

    public CodeGenOperatorFactory(GeneratedClass<? extends StreamOperator<OUT>> generatedClass) {
        this.generatedClass = generatedClass;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(
            StreamOperatorParameters<OUT> parameters) {
        return (T)
                generatedClass.newInstance(
                        parameters.getContainingTask().getUserCodeClassLoader(),
                        generatedClass.getReferences(),
                        parameters.getContainingTask(),
                        parameters.getStreamConfig(),
                        parameters.getOutput(),
                        processingTimeService);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return generatedClass.getClass(classLoader);
    }

    public GeneratedClass<? extends StreamOperator<OUT>> getGeneratedClass() {
        return generatedClass;
    }
}
