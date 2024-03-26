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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.LoadableResourceProfile;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.util.ResourceCounter;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the {@link BiDirectionalResourceToRequirementMapping}. */
class BiDirectionalResourceToRequirementMappingTest {

    @Test
    void testIncrement() {
        BiDirectionalResourceToRequirementMapping mapping =
                new BiDirectionalResourceToRequirementMapping();

        LoadableResourceProfile requirement = ResourceProfile.UNKNOWN.toEmptyLoadsResourceProfile();
        LoadableResourceProfile resource = ResourceProfile.ANY.toEmptyLoadsResourceProfile();

        mapping.incrementCount(requirement, resource, 1);

        assertThat(mapping.getLoadableRequirementsFulfilledBy(resource))
                .isEqualTo(ResourceCounter.withResource(requirement, 1));
        assertThat(mapping.getLoadableResourcesFulfilling(requirement))
                .isEqualTo(ResourceCounter.withResource(resource, 1));

        assertThat(mapping.getAllRequirementLoadableProfiles()).contains(requirement);
        assertThat(mapping.getAllLoadableResourceProfiles()).contains(resource);
    }

    @Test
    void testDecrement() {
        BiDirectionalResourceToRequirementMapping mapping =
                new BiDirectionalResourceToRequirementMapping();

        LoadableResourceProfile requirement = ResourceProfile.UNKNOWN.toEmptyLoadsResourceProfile();
        LoadableResourceProfile resource = ResourceProfile.ANY.toEmptyLoadsResourceProfile();

        mapping.incrementCount(requirement, resource, 1);
        mapping.decrementCount(requirement, resource, 1);

        assertThat(mapping.getLoadableRequirementsFulfilledBy(resource).isEmpty()).isTrue();
        assertThat(mapping.getLoadableResourcesFulfilling(requirement).isEmpty()).isTrue();

        assertThat(mapping.getAllRequirementLoadableProfiles()).isEmpty();
        assertThat(mapping.getAllLoadableResourceProfiles()).isEmpty();

        assertThat(mapping.isEmpty()).isTrue();
    }
}
