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

package org.apache.flink.streaming.connectors.elasticsearch2;

import org.elasticsearch.action.ActionRequest;

import java.io.Serializable;

/**
 * @deprecated Deprecated since 1.2, to be removed at 2.0.
 *             This class has been deprecated due to package relocation.
 *             Please use {@link org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer} instead.
 */
@Deprecated
public interface RequestIndexer extends Serializable {
	void add(ActionRequest... actionRequests);
}
