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

package org.apache.flink.kubernetes.kubeclient.resources;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;

import java.util.Map;

/** Represents EnvVar resource in Kubernetes. */
public class KubernetesSecretEnvVar extends KubernetesResource<EnvVar> {

    private static final String ENV = "env";
    private static final String SECRET = "secret";
    private static final String KEY = "key";

    private KubernetesSecretEnvVar(EnvVar envVar) {
        super(envVar);
    }

    public static KubernetesSecretEnvVar fromMap(Map<String, String> stringMap) {
        final EnvVarBuilder envVarBuilder =
                new EnvVarBuilder()
                        .withName(stringMap.get(ENV))
                        .withNewValueFrom()
                        .withNewSecretKeyRef()
                        .withName(stringMap.get(SECRET))
                        .withKey(stringMap.get(KEY))
                        .endSecretKeyRef()
                        .endValueFrom();
        return new KubernetesSecretEnvVar(envVarBuilder.build());
    }
}
