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

package org.apache.flink.state.catalog;

import java.util.Objects;

/**
 * Simple POJO used as a nested field inside a Tuple value state in {@link
 * StateCatalogGeneratedSavepointITCase.TupleKeyAndValue}.
 *
 * <p>Must remain in the normal test compilation scope (not in resources/generator/) so that it is
 * on the classpath during test runs and the TupleSerializer can deserialize it.
 */
public class TuplePojoField {
    public String name;
    public long score;

    public TuplePojoField() {}

    public TuplePojoField(String name, long score) {
        this.name = name;
        this.score = score;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TuplePojoField)) {
            return false;
        }
        TuplePojoField other = (TuplePojoField) o;
        return Objects.equals(name, other.name) && score == other.score;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, score);
    }

    @Override
    public String toString() {
        return "TuplePojoField{name='" + name + "', score=" + score + "}";
    }
}
