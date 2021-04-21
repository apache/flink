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

package org.apache.flink.annotation.docs;

import org.apache.flink.annotation.Internal;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Collection of annotations to modify the behavior of the documentation generators. */
public final class Documentation {

    /** Annotation used on config option fields to override the documented default. */
    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    @Internal
    public @interface OverrideDefault {
        String value();
    }

    /**
     * Annotation used on config option fields to include them in specific sections. Sections are
     * groups of options that are aggregated across option classes, with each group being placed
     * into a dedicated file.
     *
     * <p>The {@link Section#position()} argument controls the position in the generated table, with
     * lower values being placed at the top. Fields with the same position are sorted alphabetically
     * by key.
     */
    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    @Internal
    public @interface Section {

        /** The sections in the config docs where this option should be included. */
        String[] value() default {};

        /** The relative position of the option in its section. */
        int position() default Integer.MAX_VALUE;
    }

    /** Constants for section names. */
    public static final class Sections {

        public static final String COMMON_HOST_PORT = "common_host_port";
        public static final String COMMON_STATE_BACKENDS = "common_state_backends";
        public static final String COMMON_HIGH_AVAILABILITY = "common_high_availability";
        public static final String COMMON_HIGH_AVAILABILITY_ZOOKEEPER =
                "common_high_availability_zk";
        public static final String COMMON_MEMORY = "common_memory";
        public static final String COMMON_MISCELLANEOUS = "common_miscellaneous";

        public static final String SECURITY_SSL = "security_ssl";
        public static final String SECURITY_AUTH_KERBEROS = "security_auth_kerberos";
        public static final String SECURITY_AUTH_ZOOKEEPER = "security_auth_zk";

        public static final String STATE_BACKEND_ROCKSDB = "state_backend_rocksdb";

        public static final String STATE_BACKEND_LATENCY_TRACKING =
                "state_backend_latency_tracking";

        public static final String EXPERT_CLASS_LOADING = "expert_class_loading";
        public static final String EXPERT_DEBUGGING_AND_TUNING = "expert_debugging_and_tuning";
        public static final String EXPERT_SCHEDULING = "expert_scheduling";
        public static final String EXPERT_FAULT_TOLERANCE = "expert_fault_tolerance";
        public static final String EXPERT_STATE_BACKENDS = "expert_state_backends";
        public static final String EXPERT_REST = "expert_rest";
        public static final String EXPERT_HIGH_AVAILABILITY = "expert_high_availability";
        public static final String EXPERT_ZOOKEEPER_HIGH_AVAILABILITY =
                "expert_high_availability_zk";
        public static final String EXPERT_KUBERNETES_HIGH_AVAILABILITY =
                "expert_high_availability_k8s";
        public static final String EXPERT_SECURITY_SSL = "expert_security_ssl";
        public static final String EXPERT_ROCKSDB = "expert_rocksdb";
        public static final String EXPERT_CLUSTER = "expert_cluster";

        public static final String ALL_JOB_MANAGER = "all_jobmanager";
        public static final String ALL_TASK_MANAGER = "all_taskmanager";
        public static final String ALL_TASK_MANAGER_NETWORK = "all_taskmanager_network";

        public static final String DEPRECATED_FILE_SINKS = "deprecated_file_sinks";

        private Sections() {}
    }

    /**
     * Annotation used on table config options for adding meta data labels.
     *
     * <p>The {@link TableOption#execMode()} argument indicates the execution mode the config works
     * for (batch, streaming or both).
     */
    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    @Internal
    public @interface TableOption {
        ExecMode execMode();
    }

    /** The execution mode the config works for. */
    public enum ExecMode {
        BATCH("Batch"),
        STREAMING("Streaming"),
        BATCH_STREAMING("Batch and Streaming");

        private final String name;

        ExecMode(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * Annotation used on config option fields or options class to mark them as a suffix-option;
     * i.e., a config option where the key is only a suffix, with the prefix being dynamically
     * provided at runtime.
     */
    @Target({ElementType.FIELD, ElementType.TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    @Internal
    public @interface SuffixOption {}

    /**
     * Annotation used on config option fields or REST API message headers to exclude it from
     * documentation.
     */
    @Target({ElementType.FIELD, ElementType.TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    @Internal
    public @interface ExcludeFromDocumentation {
        /** The optional reason why it is excluded from documentation. */
        String value() default "";
    }

    private Documentation() {}
}
