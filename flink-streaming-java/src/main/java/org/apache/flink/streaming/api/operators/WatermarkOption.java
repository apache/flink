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

package org.apache.flink.streaming.api.operators;

/**
 *
 * Defines the watermark options for all "TwoInputStreamOperator".
 *
 * The default value used by the StreamOperator is {@link #ALL}, which means that
 * the operator receive two stream watermark. {@link #STREAM1} and {@link #STREAM2} mean that
 * the operator receive one stream watermatrk.
 *
 */
public enum WatermarkOption {
	STREAM1,
	STREAM2,
	ALL
}
