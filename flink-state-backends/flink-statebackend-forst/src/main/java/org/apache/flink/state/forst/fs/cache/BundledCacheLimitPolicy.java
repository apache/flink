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

package org.apache.flink.state.forst.fs.cache;

import java.util.ArrayList;
import java.util.Collection;

/** A bundled cache limit policy, any two cache policies can be combined. */
public class BundledCacheLimitPolicy implements CacheLimitPolicy {

    /** The cache policy set. */
    private Collection<CacheLimitPolicy> policies;

    public BundledCacheLimitPolicy(CacheLimitPolicy... inputPolicies) {
        this.policies = new ArrayList<>();
        for (CacheLimitPolicy policy : inputPolicies) {
            policies.add(policy);
        }
    }

    @Override
    public boolean isSafeToAdd(long toAddSize) {
        return policies.stream().allMatch(policy -> policy.isSafeToAdd(toAddSize));
    }

    @Override
    public boolean isOverflow(long toAddSize) {
        return policies.stream().anyMatch(policy -> policy.isOverflow(toAddSize));
    }

    @Override
    public void acquire(long toAddSize) {
        policies.forEach(policy -> policy.acquire(toAddSize));
    }

    @Override
    public void release(long toReleaseSize) {
        policies.forEach(policy -> policy.release(toReleaseSize));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("BundledCacheLimitPolicy{");
        for (CacheLimitPolicy policy : policies) {
            sb.append(policy.toString()).append(",");
        }
        sb.append("}");
        return sb.toString();
    }
}
