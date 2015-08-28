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

package org.apache.flink.optimizer.dag;

import org.apache.flink.api.common.operators.DualInputSemanticProperties;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.base.CoGroupOperatorBase;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CoGroupNodeTest {

	@Test
	public void testGetSemanticProperties() {

		DualInputSemanticProperties origProps = new DualInputSemanticProperties();
		// props for first input
		origProps.addForwardedField(0, 0, 1);
		origProps.addForwardedField(0, 2, 2);
		origProps.addForwardedField(0, 3, 4);
		origProps.addForwardedField(0, 6, 0);
		origProps.addReadFields(0, new FieldSet(0, 2, 4, 7));
		// props for second input
		origProps.addForwardedField(1, 1, 2);
		origProps.addForwardedField(1, 2, 8);
		origProps.addForwardedField(1, 3, 7);
		origProps.addForwardedField(1, 6, 6);
		origProps.addReadFields(1, new FieldSet(1, 3, 4));

		CoGroupOperatorBase<?,?,?,?> op = mock(CoGroupOperatorBase.class);
		when(op.getSemanticProperties()).thenReturn(origProps);
		when(op.getKeyColumns(0)).thenReturn(new int[]{3,2});
		when(op.getKeyColumns(1)).thenReturn(new int[]{6,3});
		when(op.getParameters()).thenReturn(new Configuration());

		CoGroupNode node = new CoGroupNode(op);

		SemanticProperties filteredProps = node.getSemanticPropertiesForLocalPropertyFiltering();

		// check first input props
		assertTrue(filteredProps.getForwardingTargetFields(0, 0).size() == 0);
		assertTrue(filteredProps.getForwardingTargetFields(0, 2).size() == 1);
		assertTrue(filteredProps.getForwardingTargetFields(0, 2).contains(2));
		assertTrue(filteredProps.getForwardingTargetFields(0, 3).size() == 1);
		assertTrue(filteredProps.getForwardingTargetFields(0, 3).contains(4));
		assertTrue(filteredProps.getForwardingTargetFields(0, 6).size() == 0);
		assertTrue(filteredProps.getForwardingSourceField(0, 1) < 0);
		assertTrue(filteredProps.getForwardingSourceField(0, 2) == 2);
		assertTrue(filteredProps.getForwardingSourceField(0, 4) == 3);
		assertTrue(filteredProps.getForwardingSourceField(0, 0) < 0);
		// check second input props
		assertTrue(filteredProps.getReadFields(0).size() == 4);
		assertTrue(filteredProps.getReadFields(0).contains(0));
		assertTrue(filteredProps.getReadFields(0).contains(2));
		assertTrue(filteredProps.getReadFields(0).contains(4));
		assertTrue(filteredProps.getReadFields(0).contains(7));

		assertTrue(filteredProps.getForwardingTargetFields(1, 1).size() == 0);
		assertTrue(filteredProps.getForwardingTargetFields(1, 2).size() == 0);
		assertTrue(filteredProps.getForwardingTargetFields(1, 3).size() == 1);
		assertTrue(filteredProps.getForwardingTargetFields(1, 3).contains(7));
		assertTrue(filteredProps.getForwardingTargetFields(1, 6).size() == 1);
		assertTrue(filteredProps.getForwardingTargetFields(1, 6).contains(6));
		assertTrue(filteredProps.getForwardingSourceField(1, 2) < 0);
		assertTrue(filteredProps.getForwardingSourceField(1, 8) < 0);
		assertTrue(filteredProps.getForwardingSourceField(1, 7) == 3);
		assertTrue(filteredProps.getForwardingSourceField(1, 6) == 6);

		assertTrue(filteredProps.getReadFields(1).size() == 3);
		assertTrue(filteredProps.getReadFields(1).contains(1));
		assertTrue(filteredProps.getReadFields(1).contains(3));
		assertTrue(filteredProps.getReadFields(1).contains(4));

	}
}
