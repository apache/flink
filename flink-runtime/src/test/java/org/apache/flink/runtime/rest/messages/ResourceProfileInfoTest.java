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

package org.apache.flink.runtime.rest.messages;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/** Test for (un)marshalling of the {@link ResourceProfileInfo}. */
public class ResourceProfileInfoTest extends RestResponseMarshallingTestBase<ResourceProfileInfo> {

    private static final Random random = new Random();

    @Override
    protected Class<ResourceProfileInfo> getTestResponseClass() {
        return ResourceProfileInfo.class;
    }

    @Override
    protected ResourceProfileInfo getTestResponseInstance() throws Exception {
        return createRandomResourceProfileInfo();
    }

    private static ResourceProfileInfo createRandomResourceProfileInfo() {
        final Map<String, Double> extendedResources = new HashMap<>();
        extendedResources.put("randome-key-1", random.nextDouble());
        extendedResources.put("randome-key-2", random.nextDouble());

        return new ResourceProfileInfo(
                random.nextDouble(),
                random.nextInt(),
                random.nextInt(),
                random.nextInt(),
                random.nextInt(),
                extendedResources);
    }
}
