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

package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.annotation.PublicEvolving;

import org.elasticsearch.action.ActionRequest;

import java.io.Serializable;

/**
 * An implementation of {@link ActionRequestSuccessHandler} is provided by the user to define how
 * success {@link ActionRequest ActionRequests} should be handled again, e.g. write to other third
 * party system for data consistency check.
 *
 * <p>Example:
 *
 * <pre>{@code
 * private static class ExampleActionRequestFailureHandler implements ActionRequestSuccessHandler {
 *
 * 	@Override
 * 	void onSuccess(ActionRequest action) {
 * 	    try {
 * 	        //do your self to write success data into third party system if you need
 * 	    } catch (Exception e) {
 *      }
 * 	}
 * }
 *
 * }</pre>
 */
@PublicEvolving
public interface ActionRequestSuccessHandler extends Serializable {

    /**
     * Handle a success {@link ActionRequest}.
     *
     * @param action the {@link ActionRequest} that successfully write into ES
     */
    void onSuccess(ActionRequest action);
}
