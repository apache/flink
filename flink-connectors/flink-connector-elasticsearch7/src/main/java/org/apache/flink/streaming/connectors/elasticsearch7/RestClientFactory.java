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

package org.apache.flink.streaming.connectors.elasticsearch7;

import org.apache.flink.annotation.PublicEvolving;

import org.elasticsearch.client.RestClientBuilder;

import java.io.Serializable;

/**
 * A factory that is used to configure the {@link org.elasticsearch.client.RestHighLevelClient} internally
 * used in the {@link ElasticsearchSink}.
 */
@PublicEvolving
public interface RestClientFactory extends Serializable {

	/**
	 * Configures the rest client builder.
	 *
	 * @param restClientBuilder the configured rest client builder.
	 */
	void configureRestClientBuilder(RestClientBuilder restClientBuilder);

}
