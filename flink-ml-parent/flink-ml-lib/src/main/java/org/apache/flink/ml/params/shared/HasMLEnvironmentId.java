/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;
import org.apache.flink.ml.common.MLEnvironmentFactory;

/** An interface for classes with a parameter specifying the id of MLEnvironment. */
public interface HasMLEnvironmentId<T> extends WithParams<T> {

    ParamInfo<Long> ML_ENVIRONMENT_ID =
            ParamInfoFactory.createParamInfo("MLEnvironmentId", Long.class)
                    .setDescription("ID of ML environment.")
                    .setHasDefaultValue(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID)
                    .build();

    default Long getMLEnvironmentId() {
        return get(ML_ENVIRONMENT_ID);
    }

    default T setMLEnvironmentId(Long value) {
        return set(ML_ENVIRONMENT_ID, value);
    }
}
