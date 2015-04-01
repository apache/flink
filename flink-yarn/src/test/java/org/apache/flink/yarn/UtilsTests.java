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
package org.apache.flink.yarn;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class UtilsTests {

	@Test
	public void testHeapCutoff() {
		Configuration conf = new Configuration();
		// ASSUMES DEFAULT Configuration values.
		Assert.assertEquals(800, Utils.calculateHeapSize(1000, conf) );
		Assert.assertEquals(9300, Utils.calculateHeapSize(10000, conf) );

		// test different configuration
		Assert.assertEquals(3300, Utils.calculateHeapSize(4000, conf) );

		conf.setString(ConfigConstants.YARN_HEAP_LIMIT_CAP, "1000");
		conf.setString(ConfigConstants.YARN_HEAP_CUTOFF_RATIO, "0.3");
		Assert.assertEquals(3000, Utils.calculateHeapSize(4000, conf));

		conf.setString(ConfigConstants.YARN_HEAP_LIMIT_CAP, "6000000");
		Assert.assertEquals(1200, Utils.calculateHeapSize(4000, conf));
	}
}
