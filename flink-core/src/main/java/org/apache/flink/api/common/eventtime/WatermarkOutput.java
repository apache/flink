/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Public;

/**
 * An output for watermarks. The output accepts watermarks and idleness (inactivity) status.
 */
@Public
public interface WatermarkOutput {

	/**
	 * Emits the given watermark.
	 *
	 * <p>Emitting a watermark also implicitly marks the stream as <i>active</i>, ending
	 * previously marked idleness.
	 */
	void emitWatermark(Watermark watermark);

	/**
	 * Marks this output as idle, meaning that downstream operations do not
	 * wait for watermarks from this output.
	 *
	 * <p>An output becomes active again as soon as the next watermark is emitted.
	 */
	void markIdle();
}
