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

package org.apache.flink.tools.ci.utils.shared;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/**
 * Represents a dependency.
 *
 * <p>For some properties we return an {@link Optional}, for those that, depending on the plugin
 * goal, may not be determinable. For example, {@code dependency:copy} never prints the scope or
 * optional flag.
 */
public final class Dependency {

    private final String groupId;
    private final String artifactId;
    private final String version;
    @Nullable private final String classifier;
    @Nullable private final String scope;
    @Nullable private final Boolean isOptional;

    private Dependency(
            String groupId,
            String artifactId,
            String version,
            @Nullable String classifier,
            @Nullable String scope,
            @Nullable Boolean isOptional) {
        this.groupId = Objects.requireNonNull(groupId);
        this.artifactId = Objects.requireNonNull(artifactId);
        this.version = Objects.requireNonNull(version);
        this.classifier = classifier;
        this.scope = scope;
        this.isOptional = isOptional;
    }

    public static Dependency create(
            String groupId,
            String artifactId,
            String version,
            String classifier,
            String scope,
            boolean isOptional) {
        return new Dependency(
                groupId,
                artifactId,
                version,
                classifier,
                Objects.requireNonNull(scope),
                isOptional);
    }

    public static Dependency create(
            String groupId, String artifactId, String version, String classifier) {
        return new Dependency(groupId, artifactId, version, classifier, null, null);
    }

    public String getGroupId() {
        return groupId;
    }

    public String getArtifactId() {
        return artifactId;
    }

    public String getVersion() {
        return version;
    }

    public Optional<String> getClassifier() {
        return Optional.ofNullable(classifier);
    }

    public Optional<String> getScope() {
        return Optional.ofNullable(scope);
    }

    public Optional<Boolean> isOptional() {
        return Optional.ofNullable(isOptional);
    }

    @Override
    public String toString() {
        return groupId
                + ":"
                + artifactId
                + ":"
                + version
                + (classifier != null ? ":" + classifier : "")
                + (scope != null ? ":" + scope : "")
                + (isOptional != null && isOptional ? " (optional)" : "");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Dependency that = (Dependency) o;
        return Objects.equals(groupId, that.groupId)
                && Objects.equals(artifactId, that.artifactId)
                && Objects.equals(version, that.version)
                && Objects.equals(classifier, that.classifier)
                && Objects.equals(scope, that.scope)
                && Objects.equals(isOptional, that.isOptional);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, artifactId, version, classifier, scope, isOptional);
    }
}
