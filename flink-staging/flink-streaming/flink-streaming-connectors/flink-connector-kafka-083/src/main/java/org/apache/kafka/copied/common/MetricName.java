/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.copied.common;

import org.apache.kafka.copied.common.utils.Utils;

import java.util.HashMap;
import java.util.Map;

/**
 * The <code>MetricName</code> class encapsulates a metric's name, logical group and its related attributes
 * <p>
 * This class captures the following parameters
 * <pre>
 *  <b>name</b> The name of the metric
 *  <b>group</b> logical group name of the metrics to which this metric belongs.
 *  <b>description</b> A human-readable description to include in the metric. This is optional.
 *  <b>tags</b> additional key/value attributes of the metric. This is optional.
 * </pre>
 * group, tags parameters can be used to create unique metric names while reporting in JMX or any custom reporting.
 * <p>
 * Ex: standard JMX MBean can be constructed like  <b>domainName:type=group,key1=val1,key2=val2</b>
 * <p>
 * Usage looks something like this:
 * <pre>{@code
 * // set up metrics:
 * Metrics metrics = new Metrics(); // this is the global repository of metrics and sensors
 * Sensor sensor = metrics.sensor("message-sizes");
 *
 * Map<String, String> metricTags = new LinkedHashMap<String, String>();
 * metricTags.put("client-id", "producer-1");
 * metricTags.put("topic", "topic");
 *
 * MetricName metricName = new MetricName("message-size-avg", "producer-metrics", "average message size", metricTags);
 * sensor.add(metricName, new Avg());
 *
 * metricName = new MetricName("message-size-max", "producer-metrics", metricTags);
 * sensor.add(metricName, new Max());
 *
 * metricName = new MetricName("message-size-min", "producer-metrics", "message minimum size", "client-id", "my-client", "topic", "my-topic");
 * sensor.add(metricName, new Min());
 *
 * // as messages are sent we record the sizes
 * sensor.record(messageSize);
 * }</pre>
 */
public final class MetricName {

    private final String name;
    private final String group;
    private final String description;
    private Map<String, String> tags;
    private int hash = 0;

    /**
     * @param name        The name of the metric
     * @param group       logical group name of the metrics to which this metric belongs
     * @param description A human-readable description to include in the metric
     * @param tags        additional key/value attributes of the metric
     */
    public MetricName(String name, String group, String description, Map<String, String> tags) {
        this.name = Utils.notNull(name);
        this.group = Utils.notNull(group);
        this.description = Utils.notNull(description);
        this.tags = Utils.notNull(tags);
    }

    /**
     * @param name          The name of the metric
     * @param group         logical group name of the metrics to which this metric belongs
     * @param description   A human-readable description to include in the metric
     * @param keyValue      additional key/value attributes of the metric (must come in pairs)
     */
    public MetricName(String name, String group, String description, String... keyValue) {
        this(name, group, description, getTags(keyValue));
    }

    private static Map<String, String> getTags(String... keyValue) {
        if ((keyValue.length % 2) != 0)
            throw new IllegalArgumentException("keyValue needs to be specified in paris");
        Map<String, String> tags = new HashMap<String, String>();

        for (int i = 0; i < keyValue.length / 2; i++)
            tags.put(keyValue[i], keyValue[i + 1]);
        return tags;
    }

    /**
     * @param name  The name of the metric
     * @param group logical group name of the metrics to which this metric belongs
     * @param tags  key/value attributes of the metric
     */
    public MetricName(String name, String group, Map<String, String> tags) {
        this(name, group, "", tags);
    }

    /**
     * @param name        The name of the metric
     * @param group       logical group name of the metrics to which this metric belongs
     * @param description A human-readable description to include in the metric
     */
    public MetricName(String name, String group, String description) {
        this(name, group, description, new HashMap<String, String>());
    }

    /**
     * @param name  The name of the metric
     * @param group logical group name of the metrics to which this metric belongs
     */
    public MetricName(String name, String group) {
        this(name, group, "", new HashMap<String, String>());
    }

    public String name() {
        return this.name;
    }

    public String group() {
        return this.group;
    }

    public Map<String, String> tags() {
        return this.tags;
    }

    public String description() {
        return this.description;
    }

    @Override
    public int hashCode() {
        if (hash != 0)
            return hash;
        final int prime = 31;
        int result = 1;
        result = prime * result + ((group == null) ? 0 : group.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((tags == null) ? 0 : tags.hashCode());
        this.hash = result;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MetricName other = (MetricName) obj;
        if (group == null) {
            if (other.group != null)
                return false;
        } else if (!group.equals(other.group))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (tags == null) {
            if (other.tags != null)
                return false;
        } else if (!tags.equals(other.tags))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "MetricName [name=" + name + ", group=" + group + ", description="
                + description + ", tags=" + tags + "]";
    }
}