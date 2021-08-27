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

package org.apache.flink.api.common.externalresource;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Set;

/**
 * Driver which takes the responsibility to manage and provide the information of external resource.
 *
 * <p>Drivers that should be instantiated via a {@link ExternalResourceDriverFactory}.
 *
 * <p>TaskExecutor will retrieve the {@link ExternalResourceInfo} set of the external resource from
 * the drivers.
 */
@PublicEvolving
public interface ExternalResourceDriver {

    /**
     * Retrieve the information of the external resources according to the amount.
     *
     * @param amount of the required external resources
     * @return information set of the required external resources
     * @throws Exception if there is something wrong during retrieving
     */
    Set<? extends ExternalResourceInfo> retrieveResourceInfo(long amount) throws Exception;
}
