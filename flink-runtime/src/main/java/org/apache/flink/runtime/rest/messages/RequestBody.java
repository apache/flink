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

package org.apache.flink.runtime.rest.messages;

/**
 * Marker interface for all requests of the REST API. This class represents the http body of a request.
 *
 * <p>Subclass instances are converted to JSON using jackson-databind. Subclasses must have a constructor that accepts
 * all fields of the JSON request, that should be annotated with {@code @JsonCreator}.
 *
 * <p>All fields that should part of the JSON request must be accessible either by being public or having a getter.
 *
 * <p>When adding methods that are prefixed with {@code get/is} make sure to annotate them with {@code @JsonIgnore}.
 */
public interface RequestBody {
}
