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

package org.apache.flink.streaming.api.lineage;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Objects;

/** Default implementation of {@link TypeDatasetFacet}. */
@PublicEvolving
public class DefaultTypeDatasetFacet implements TypeDatasetFacet {

    public static final String TYPE_FACET_NAME = "type";

    private final TypeInformation typeInformation;

    public DefaultTypeDatasetFacet(TypeInformation typeInformation) {
        this.typeInformation = typeInformation;
    }

    public TypeInformation getTypeInformation() {
        return typeInformation;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultTypeDatasetFacet that = (DefaultTypeDatasetFacet) o;
        return Objects.equals(typeInformation, that.typeInformation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeInformation);
    }

    @Override
    public String name() {
        return TYPE_FACET_NAME;
    }
}
