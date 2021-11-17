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

package org.apache.flink.kubernetes.utils;

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/** Collection of kubernetes labels and helper methods. */
public enum KubernetesLabel {
    CONFIG_MAP("flink-config-", ""),
    HADOOP_CONF_CONFIG_MAP("hadoop-config-", ""),
    KERBEROS_KEYTAB_SECRET("kerberos-keytab-", ""),
    KERBEROS_KRB5CONF_CONFIG_MAP("kerberos-krb5conf-", ""),
    POD_TEMPLATE_CONFIG_MAP("pod-template-", ""),
    FLINK_REST_SERVICE("", "-rest");

    private static final int KUBERNETES_LABEL_MAX_LENGTH = 63;

    private final String prefix;
    private final String suffix;

    KubernetesLabel(String prefix, String suffix) {
        this.prefix = prefix;
        this.suffix = suffix;
    }

    private int getTotalLength() {
        return prefix.length() + suffix.length();
    }

    public static int getClusterIdMaxLength() {
        int longestAffixLength =
                Arrays.stream(KubernetesLabel.values())
                        .map(KubernetesLabel::getTotalLength)
                        .max(Integer::compareTo)
                        .orElseThrow(() -> new IllegalStateException("No enum value is present."));

        return KUBERNETES_LABEL_MAX_LENGTH - longestAffixLength;
    }

    /**
     * Generates the kubernetes label containing the clusterId.
     *
     * @param clusterId The clusterId
     * @return a String containing the kubernetes label with the clusterId
     */
    public String generateWith(String clusterId) {
        checkArgument(
                !isNullOrWhitespaceOnly(clusterId),
                "%s must not be blank.",
                KubernetesConfigOptions.CLUSTER_ID.key());

        String labelWithClusterId = prefix + clusterId + suffix;

        checkArgument(
                labelWithClusterId.length() <= KUBERNETES_LABEL_MAX_LENGTH,
                "%s must be no more than %s characters. Please change %s.",
                labelWithClusterId,
                KUBERNETES_LABEL_MAX_LENGTH,
                KubernetesConfigOptions.CLUSTER_ID.key());

        return labelWithClusterId;
    }
}
