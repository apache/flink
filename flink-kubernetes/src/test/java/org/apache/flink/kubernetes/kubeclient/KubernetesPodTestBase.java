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

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.kubernetes.KubernetesTestBase;

import java.util.HashMap;
import java.util.Map;

/**
 * Base test class for the Kubernetes Pod.
 */
public class KubernetesPodTestBase extends KubernetesTestBase {

	protected final Map<String, String> customizedEnvs = new HashMap<String, String>() {
		{
			put("key1", "value1");
			put("key2", "value2");
		}
	};

	protected final Map<String, String> userLabels = new HashMap<String, String>() {
		{
			put("label1", "value1");
			put("label2", "value2");
		}
	};

	protected final Map<String, String> nodeSelector = new HashMap<String, String>() {
		{
			put("env", "production");
			put("disk", "ssd");
		}
	};

	protected FlinkPod baseFlinkPod = new FlinkPod.Builder().build();
}
