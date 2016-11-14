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

package org.apache.flink.runtime.clusterframework;

import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class BootstrapToolsTest {

	@Test
	public void testSubstituteConfigKey() {
		String deprecatedKey1 ="deprecated-key";
		String deprecatedKey2 ="another-out_of-date_key";
		String deprecatedKey3 ="yet-one-more";

		String designatedKey1 ="newkey1";
		String designatedKey2 ="newKey2";
		String designatedKey3 ="newKey3";

		String value1 = "value1";
		String value2_designated = "designated-value2";
		String value2_deprecated = "deprecated-value2";

		// config contains only deprecated key 1, and for key 2 both deprecated and designated
		Configuration cfg = new Configuration();
		cfg.setString(deprecatedKey1, value1);
		cfg.setString(deprecatedKey2, value2_deprecated);
		cfg.setString(designatedKey2, value2_designated);

		BootstrapTools.substituteDeprecatedConfigKey(cfg, deprecatedKey1, designatedKey1);
		BootstrapTools.substituteDeprecatedConfigKey(cfg, deprecatedKey2, designatedKey2);
		BootstrapTools.substituteDeprecatedConfigKey(cfg, deprecatedKey3, designatedKey3);

		// value 1 should be set to designated
		assertEquals(value1, cfg.getString(designatedKey1, null));

		// value 2 should not have been set, since it had a value already
		assertEquals(value2_designated, cfg.getString(designatedKey2, null));

		// nothing should be in there for key 3
		assertNull(cfg.getString(designatedKey3, null));
		assertNull(cfg.getString(deprecatedKey3, null));
	}

	@Test
	public void testSubstituteConfigKeyPrefix() {
		String deprecatedPrefix1 ="deprecated-prefix";
		String deprecatedPrefix2 ="-prefix-2";
		String deprecatedPrefix3 ="prefix-3";

		String designatedPrefix1 ="p1";
		String designatedPrefix2 ="ppp";
		String designatedPrefix3 ="zzz";

		String depr1 = deprecatedPrefix1 + "var";
		String depr2 = deprecatedPrefix2 + "env";
		String depr3 = deprecatedPrefix2 + "x";

		String desig1 = designatedPrefix1 + "var";
		String desig2 = designatedPrefix2 + "env";
		String desig3 = designatedPrefix2 + "x";

		String val1 = "1";
		String val2 = "2";
		String val3_depr = "3-";
		String val3_desig = "3+";

		// config contains only deprecated key 1, and for key 2 both deprecated and designated
		Configuration cfg = new Configuration();
		cfg.setString(depr1, val1);
		cfg.setString(depr2, val2);
		cfg.setString(depr3, val3_depr);
		cfg.setString(desig3, val3_desig);

		BootstrapTools.substituteDeprecatedConfigPrefix(cfg, deprecatedPrefix1, designatedPrefix1);
		BootstrapTools.substituteDeprecatedConfigPrefix(cfg, deprecatedPrefix2, designatedPrefix2);
		BootstrapTools.substituteDeprecatedConfigPrefix(cfg, deprecatedPrefix3, designatedPrefix3);

		assertEquals(val1, cfg.getString(desig1, null));
		assertEquals(val2, cfg.getString(desig2, null));
		assertEquals(val3_desig, cfg.getString(desig3, null));

		// check that nothing with prefix 3 is contained
		for (String key : cfg.keySet()) {
			assertFalse(key.startsWith(designatedPrefix3));
			assertFalse(key.startsWith(deprecatedPrefix3));
		}
	}
}
