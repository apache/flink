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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * {@link org.apache.flink.api.common.io.OutputFormat}s can implement this interface to be
 * configured with the data type they will operate on. The method {@link
 * #setInputType(TypeInformation, ExecutionConfig)} will be called when the output format is used
 * with an output API method.
 */
@Public
public interface InputTypeConfigurable {

    /**
     * Method that is called on an {@link org.apache.flink.api.common.io.OutputFormat} when it is
     * passed to the DataSet's output method. May be used to configures the output format based on
     * the data type.
     *
     * @param type The data type of the input.
     * @param executionConfig The execution config for this parallel execution.
     */
    void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig);
}
