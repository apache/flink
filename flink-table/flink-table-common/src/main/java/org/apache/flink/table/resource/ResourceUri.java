/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.resource;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Objects;

/** Description of resource information. */
@PublicEvolving
public class ResourceUri {

    private final ResourceType resourceType;
    private final String uri;

    public ResourceUri(ResourceType resourceType, String uri) {
        this.resourceType = resourceType;
        this.uri = uri;
    }

    /** Get resource type info. */
    public ResourceType getResourceType() {
        return resourceType;
    }

    /** Get resource unique path info. */
    public String getUri() {
        return uri;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourceUri that = (ResourceUri) o;
        return resourceType == that.resourceType && Objects.equals(uri, that.uri);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceType, uri);
    }

    @Override
    public String toString() {
        return "ResourceUri{" + "resourceType=" + resourceType + ", uri='" + uri + '\'' + '}';
    }
}
