/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.compiler;

import static org.junit.Assert.*;
import static eu.stratosphere.compiler.plan.PlanNode.FeedbackPropertiesMeetRequirementsReport.*;

import org.junit.Test;

import eu.stratosphere.api.common.functions.GenericJoiner;
import eu.stratosphere.api.common.functions.GenericMap;
import eu.stratosphere.api.common.operators.BinaryOperatorInformation;
import eu.stratosphere.api.common.operators.OperatorInformation;
import eu.stratosphere.api.common.operators.Order;
import eu.stratosphere.api.common.operators.Ordering;
import eu.stratosphere.api.common.operators.UnaryOperatorInformation;
import eu.stratosphere.api.common.operators.base.GenericDataSourceBase;
import eu.stratosphere.api.common.operators.base.JoinOperatorBase;
import eu.stratosphere.api.common.operators.base.MapOperatorBase;
import eu.stratosphere.api.common.operators.util.FieldList;
import eu.stratosphere.api.common.operators.util.FieldSet;
import eu.stratosphere.api.java.io.TextInputFormat;
import eu.stratosphere.api.java.typeutils.BasicTypeInfo;
import eu.stratosphere.compiler.dag.DataSourceNode;
import eu.stratosphere.compiler.dag.MapNode;
import eu.stratosphere.compiler.dag.MatchNode;
import eu.stratosphere.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.compiler.dataproperties.LocalProperties;
import eu.stratosphere.compiler.dataproperties.RequestedGlobalProperties;
import eu.stratosphere.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.compiler.plan.Channel;
import eu.stratosphere.compiler.plan.DualInputPlanNode;
import eu.stratosphere.compiler.plan.SingleInputPlanNode;
import eu.stratosphere.compiler.plan.SourcePlanNode;
import eu.stratosphere.compiler.plan.PlanNode.FeedbackPropertiesMeetRequirementsReport;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.pact.compiler.testfunctions.DummyJoinFunction;
import eu.stratosphere.pact.compiler.testfunctions.IdentityMapper;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;


public class FeedbackPropertiesMatchTest {

	@Test
	public void testNoPartialSolutionFoundSingleInputOnly() {
		try {
			SourcePlanNode target = new SourcePlanNode(getSourceNode(), "Source");
			
			SourcePlanNode otherTarget = new SourcePlanNode(getSourceNode(), "Source");
			
			Channel toMap1 = new Channel(target);
			toMap1.setShipStrategy(ShipStrategyType.FORWARD);
			toMap1.setLocalStrategy(LocalStrategy.NONE);
			SingleInputPlanNode map1 = new SingleInputPlanNode(getMapNode(), "Mapper 1", toMap1, DriverStrategy.MAP);
			
			Channel toMap2 = new Channel(map1);
			toMap2.setShipStrategy(ShipStrategyType.FORWARD);
			toMap2.setLocalStrategy(LocalStrategy.NONE);
			SingleInputPlanNode map2 = new SingleInputPlanNode(getMapNode(), "Mapper 2", toMap2, DriverStrategy.MAP);
			
			{
				GlobalProperties gp = new GlobalProperties();
				LocalProperties lp = new LocalProperties();
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(otherTarget, gp, lp);
				assertTrue(report == NO_PARTIAL_SOLUTION);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSingleInputOperators() {
		try {
			SourcePlanNode target = new SourcePlanNode(getSourceNode(), "Source");
			
			Channel toMap1 = new Channel(target);
			toMap1.setShipStrategy(ShipStrategyType.FORWARD);
			toMap1.setLocalStrategy(LocalStrategy.NONE);
			SingleInputPlanNode map1 = new SingleInputPlanNode(getMapNode(), "Mapper 1", toMap1, DriverStrategy.MAP);
			
			Channel toMap2 = new Channel(map1);
			toMap2.setShipStrategy(ShipStrategyType.FORWARD);
			toMap2.setLocalStrategy(LocalStrategy.NONE);
			SingleInputPlanNode map2 = new SingleInputPlanNode(getMapNode(), "Mapper 2", toMap2, DriverStrategy.MAP);
			
			// no feedback properties and none are ever required and present
			{
				GlobalProperties gp = new GlobalProperties();
				LocalProperties lp = new LocalProperties();
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// some global feedback properties and none are ever required and present
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(2, 5));
				LocalProperties lp = new LocalProperties();
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// some local feedback properties and none are ever required and present
			{
				GlobalProperties gp = new GlobalProperties();
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(1, 2));
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// some global and local feedback properties and none are ever required and present
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(2, 5));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(1, 2));
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// --------------------------- requirements on channel 1 -----------------------
			
			// some required global properties, which are matched exactly
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(2, 5));
				LocalProperties lp = new LocalProperties();
				
				RequestedGlobalProperties reqGp = new RequestedGlobalProperties();
				reqGp.setHashPartitioned(new FieldList(2, 5));
				
				toMap1.setRequiredGlobalProps(reqGp);
				toMap1.setRequiredLocalProps(null);
				
				toMap2.setRequiredGlobalProps(null);
				toMap2.setRequiredLocalProps(null);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// some required local properties, which are matched exactly
			{
				GlobalProperties gp = new GlobalProperties();
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(1, 2));
				
				RequestedLocalProperties reqLp = new RequestedLocalProperties();
				reqLp.setGroupedFields(new FieldList(1, 2));
				
				toMap1.setRequiredGlobalProps(null);
				toMap1.setRequiredLocalProps(reqLp);
				
				toMap2.setRequiredGlobalProps(null);
				toMap2.setRequiredLocalProps(null);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// some required global and local properties, which are matched exactly
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(2, 5));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(1, 2));
				
				RequestedGlobalProperties reqGp = new RequestedGlobalProperties();
				reqGp.setHashPartitioned(new FieldList(2, 5));
				
				RequestedLocalProperties reqLp = new RequestedLocalProperties();
				reqLp.setGroupedFields(new FieldList(1, 2));
				
				toMap1.setRequiredGlobalProps(reqGp);
				toMap1.setRequiredLocalProps(reqLp);
				
				toMap2.setRequiredGlobalProps(null);
				toMap2.setRequiredLocalProps(null);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// some required global and local properties, which are over-fulfilled
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(2));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(1, 2));
				
				RequestedGlobalProperties reqGp = new RequestedGlobalProperties();
				reqGp.setHashPartitioned(new FieldList(2, 5));
				
				RequestedLocalProperties reqLp = new RequestedLocalProperties();
				reqLp.setGroupedFields(new FieldList(1));
				
				toMap1.setRequiredGlobalProps(reqGp);
				toMap1.setRequiredLocalProps(reqLp);
				
				toMap2.setRequiredGlobalProps(null);
				toMap2.setRequiredLocalProps(null);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// some required global properties that are not met
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(2, 1));
				LocalProperties lp = new LocalProperties();
				
				RequestedGlobalProperties reqGp = new RequestedGlobalProperties();
				reqGp.setHashPartitioned(new FieldList(2, 5));
				
				toMap1.setRequiredGlobalProps(reqGp);
				toMap1.setRequiredLocalProps(null);
				
				toMap2.setRequiredGlobalProps(null);
				toMap2.setRequiredLocalProps(null);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(NOT_MET, report);
			}
			
			// some required local properties that are not met
			{
				GlobalProperties gp = new GlobalProperties();
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(1));
				
				RequestedLocalProperties reqLp = new RequestedLocalProperties();
				reqLp.setGroupedFields(new FieldList(2, 1));
				
				toMap1.setRequiredGlobalProps(null);
				toMap1.setRequiredLocalProps(reqLp);
				
				toMap2.setRequiredGlobalProps(null);
				toMap2.setRequiredLocalProps(null);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(NOT_MET, report);
			}
			
			// some required global and local properties where the global properties are not met
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(2, 1));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(1));
				
				RequestedGlobalProperties reqGp = new RequestedGlobalProperties();
				reqGp.setAnyPartitioning(new FieldList(2, 5));
				
				RequestedLocalProperties reqLp = new RequestedLocalProperties();
				reqLp.setGroupedFields(new FieldList(1));
				
				toMap1.setRequiredGlobalProps(reqGp);
				toMap1.setRequiredLocalProps(reqLp);
				
				toMap2.setRequiredGlobalProps(null);
				toMap2.setRequiredLocalProps(null);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(NOT_MET, report);
			}
			
			// some required global and local properties where the local properties are not met
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(1));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(1));
				
				RequestedGlobalProperties reqGp = new RequestedGlobalProperties();
				reqGp.setAnyPartitioning(new FieldList(1));
				
				RequestedLocalProperties reqLp = new RequestedLocalProperties();
				reqLp.setGroupedFields(new FieldList(2));
				
				toMap1.setRequiredGlobalProps(reqGp);
				toMap1.setRequiredLocalProps(reqLp);
				
				toMap2.setRequiredGlobalProps(null);
				toMap2.setRequiredLocalProps(null);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(NOT_MET, report);
			}
			
			// --------------------------- requirements on channel 2 -----------------------
			
			// some required global properties, which are matched exactly
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(2, 5));
				LocalProperties lp = new LocalProperties();
				
				RequestedGlobalProperties reqGp = new RequestedGlobalProperties();
				reqGp.setHashPartitioned(new FieldList(2, 5));
				
				toMap1.setRequiredGlobalProps(null);
				toMap1.setRequiredLocalProps(null);
				
				toMap2.setRequiredGlobalProps(reqGp);
				toMap2.setRequiredLocalProps(null);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// some required local properties, which are matched exactly
			{
				GlobalProperties gp = new GlobalProperties();
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(1, 2));
				
				RequestedLocalProperties reqLp = new RequestedLocalProperties();
				reqLp.setGroupedFields(new FieldList(1, 2));
				
				toMap1.setRequiredGlobalProps(null);
				toMap1.setRequiredLocalProps(null);
				
				toMap2.setRequiredGlobalProps(null);
				toMap2.setRequiredLocalProps(reqLp);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// some required global and local properties, which are matched exactly
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(2, 5));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(1, 2));
				
				RequestedGlobalProperties reqGp = new RequestedGlobalProperties();
				reqGp.setHashPartitioned(new FieldList(2, 5));
				
				RequestedLocalProperties reqLp = new RequestedLocalProperties();
				reqLp.setGroupedFields(new FieldList(1, 2));
				
				toMap1.setRequiredGlobalProps(null);
				toMap1.setRequiredLocalProps(null);
				
				toMap2.setRequiredGlobalProps(reqGp);
				toMap2.setRequiredLocalProps(reqLp);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// some required global and local properties, which are over-fulfilled
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(2));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(1, 2));
				
				RequestedGlobalProperties reqGp = new RequestedGlobalProperties();
				reqGp.setHashPartitioned(new FieldList(2, 5));
				
				RequestedLocalProperties reqLp = new RequestedLocalProperties();
				reqLp.setGroupedFields(new FieldList(1));
				
				toMap1.setRequiredGlobalProps(null);
				toMap1.setRequiredLocalProps(null);
				
				toMap2.setRequiredGlobalProps(reqGp);
				toMap2.setRequiredLocalProps(reqLp);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// some required global properties that are not met
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(2, 1));
				LocalProperties lp = new LocalProperties();
				
				RequestedGlobalProperties reqGp = new RequestedGlobalProperties();
				reqGp.setHashPartitioned(new FieldList(2, 5));
				
				toMap1.setRequiredGlobalProps(null);
				toMap1.setRequiredLocalProps(null);
				
				toMap2.setRequiredGlobalProps(reqGp);
				toMap2.setRequiredLocalProps(null);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(NOT_MET, report);
			}
			
			// some required local properties that are not met
			{
				GlobalProperties gp = new GlobalProperties();
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(1));
				
				RequestedLocalProperties reqLp = new RequestedLocalProperties();
				reqLp.setGroupedFields(new FieldList(2, 1));
				
				toMap1.setRequiredGlobalProps(null);
				toMap1.setRequiredLocalProps(null);
				
				toMap2.setRequiredGlobalProps(null);
				toMap2.setRequiredLocalProps(reqLp);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(NOT_MET, report);
			}
			
			// some required global and local properties where the global properties are not met
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(2, 1));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(1));
				
				RequestedGlobalProperties reqGp = new RequestedGlobalProperties();
				reqGp.setAnyPartitioning(new FieldList(2, 5));
				
				RequestedLocalProperties reqLp = new RequestedLocalProperties();
				reqLp.setGroupedFields(new FieldList(1));
				
				toMap1.setRequiredGlobalProps(null);
				toMap1.setRequiredLocalProps(null);
				
				toMap2.setRequiredGlobalProps(reqGp);
				toMap2.setRequiredLocalProps(reqLp);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(NOT_MET, report);
			}
			
			// some required global and local properties where the local properties are not met
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(1));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(1));
				
				RequestedGlobalProperties reqGp = new RequestedGlobalProperties();
				reqGp.setAnyPartitioning(new FieldList(1));
				
				RequestedLocalProperties reqLp = new RequestedLocalProperties();
				reqLp.setGroupedFields(new FieldList(2));
				
				toMap1.setRequiredGlobalProps(null);
				toMap1.setRequiredLocalProps(null);
				
				toMap2.setRequiredGlobalProps(reqGp);
				toMap2.setRequiredLocalProps(reqLp);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(NOT_MET, report);
			}
			
			// ---------------------- requirements mixed on 1 and 2 -----------------------
			
			// some required global properties at step one and some more at step 2
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(1, 2));
				LocalProperties lp = LocalProperties.EMPTY;
				
				RequestedGlobalProperties reqGp1 = new RequestedGlobalProperties();
				reqGp1.setAnyPartitioning(new FieldList(1, 2));
				
				RequestedGlobalProperties reqGp2 = new RequestedGlobalProperties();
				reqGp2.setHashPartitioned(new FieldList(1, 2));
				
				toMap1.setRequiredGlobalProps(reqGp1);
				toMap1.setRequiredLocalProps(null);
				
				toMap2.setRequiredGlobalProps(reqGp2);
				toMap2.setRequiredLocalProps(null);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// some required local properties at step one and some more at step 2
			{
				GlobalProperties gp = new GlobalProperties();
				LocalProperties lp = LocalProperties.forOrdering(new Ordering(3, null, Order.ASCENDING).appendOrdering(1, null, Order.DESCENDING));
				
				RequestedLocalProperties reqLp1 = new RequestedLocalProperties();
				reqLp1.setGroupedFields(new FieldList(3, 1));
				
				RequestedLocalProperties reqLp2 = new RequestedLocalProperties();
				reqLp2.setOrdering(new Ordering(3, null, Order.ANY).appendOrdering(1, null, Order.ANY));
				
				toMap1.setRequiredGlobalProps(null);
				toMap1.setRequiredLocalProps(reqLp1);
				
				toMap2.setRequiredGlobalProps(null);
				toMap2.setRequiredLocalProps(reqLp2);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// some required global properties at step one and some local ones at step 2
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(1, 2));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				RequestedGlobalProperties reqGp = new RequestedGlobalProperties();
				reqGp.setAnyPartitioning(new FieldList(1, 2));
				
				RequestedLocalProperties reqLp = new RequestedLocalProperties();
				reqLp.setGroupedFields(new FieldList(2));
				
				toMap1.setRequiredGlobalProps(reqGp);
				toMap1.setRequiredLocalProps(null);
				
				toMap2.setRequiredGlobalProps(null);
				toMap2.setRequiredLocalProps(reqLp);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// some required local properties at step one and some global ones at step 2
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(1, 2));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				RequestedGlobalProperties reqGp = new RequestedGlobalProperties();
				reqGp.setAnyPartitioning(new FieldList(1, 2));
				
				RequestedLocalProperties reqLp = new RequestedLocalProperties();
				reqLp.setGroupedFields(new FieldList(2));
				
				toMap1.setRequiredGlobalProps(null);
				toMap1.setRequiredLocalProps(reqLp);
				
				toMap2.setRequiredGlobalProps(reqGp);
				toMap2.setRequiredLocalProps(null);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// some fulfilled global properties at step one and some non-fulfilled local ones at step 2
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(1, 2));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				RequestedGlobalProperties reqGp = new RequestedGlobalProperties();
				reqGp.setAnyPartitioning(new FieldList(1, 2));
				
				RequestedLocalProperties reqLp = new RequestedLocalProperties();
				reqLp.setGroupedFields(new FieldList(2, 3));
				
				toMap1.setRequiredGlobalProps(reqGp);
				toMap1.setRequiredLocalProps(null);
				
				toMap2.setRequiredGlobalProps(null);
				toMap2.setRequiredLocalProps(reqLp);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(NOT_MET, report);
			}
			
			// some fulfilled local properties at step one and some non-fulfilled global ones at step 2
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(1, 2));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				RequestedGlobalProperties reqGp = new RequestedGlobalProperties();
				reqGp.setAnyPartitioning(new FieldList(2, 3));
				
				RequestedLocalProperties reqLp = new RequestedLocalProperties();
				reqLp.setGroupedFields(new FieldList(2, 1));
				
				toMap1.setRequiredGlobalProps(null);
				toMap1.setRequiredLocalProps(reqLp);
				
				toMap2.setRequiredGlobalProps(reqGp);
				toMap2.setRequiredLocalProps(null);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(NOT_MET, report);
			}
			
			// some non-fulfilled global properties at step one and some fulfilled local ones at step 2
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(1, 2));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				RequestedGlobalProperties reqGp = new RequestedGlobalProperties();
				reqGp.setAnyPartitioning(new FieldList(2, 3));
				
				RequestedLocalProperties reqLp = new RequestedLocalProperties();
				reqLp.setGroupedFields(new FieldList(2, 1));
				
				toMap1.setRequiredGlobalProps(reqGp);
				toMap1.setRequiredLocalProps(null);
				
				toMap2.setRequiredGlobalProps(null);
				toMap2.setRequiredLocalProps(reqLp);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(NOT_MET, report);
			}
			
			// some non-fulfilled local properties at step one and some fulfilled global ones at step 2
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(1, 2));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				RequestedGlobalProperties reqGp = new RequestedGlobalProperties();
				reqGp.setAnyPartitioning(new FieldList(1, 2));
				
				RequestedLocalProperties reqLp = new RequestedLocalProperties();
				reqLp.setGroupedFields(new FieldList(2, 1, 3));
				
				toMap1.setRequiredGlobalProps(null);
				toMap1.setRequiredLocalProps(reqLp);
				
				toMap2.setRequiredGlobalProps(reqGp);
				toMap2.setRequiredLocalProps(null);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(NOT_MET, report);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSingleInputOperatorsWithReCreation() {
		try {
			SourcePlanNode target = new SourcePlanNode(getSourceNode(), "Source");
			
			Channel toMap1 = new Channel(target);
			SingleInputPlanNode map1 = new SingleInputPlanNode(getMapNode(), "Mapper 1", toMap1, DriverStrategy.MAP);
			
			Channel toMap2 = new Channel(map1);
			SingleInputPlanNode map2 = new SingleInputPlanNode(getMapNode(), "Mapper 2", toMap2, DriverStrategy.MAP);
			
			// set ship strategy in first channel, so later non matching global properties do not matter
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(1, 2));
				LocalProperties lp = LocalProperties.EMPTY;
				
				RequestedGlobalProperties reqGp = new RequestedGlobalProperties();
				reqGp.setAnyPartitioning(new FieldSet(2, 5));
				
				toMap1.setShipStrategy(ShipStrategyType.PARTITION_HASH, new FieldList(2, 5));
				toMap1.setLocalStrategy(LocalStrategy.NONE);
				
				toMap2.setShipStrategy(ShipStrategyType.FORWARD);
				toMap2.setLocalStrategy(LocalStrategy.NONE);
				
				
				toMap1.setRequiredGlobalProps(null);
				toMap1.setRequiredLocalProps(null);
				
				toMap2.setRequiredGlobalProps(reqGp);
				toMap2.setRequiredLocalProps(null);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(MET, report);
			}
			
			// set ship strategy in second channel, so previous non matching global properties void the match
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(1, 2));
				LocalProperties lp = LocalProperties.EMPTY;
				
				RequestedGlobalProperties reqGp = new RequestedGlobalProperties();
				reqGp.setAnyPartitioning(new FieldSet(2, 5));
				
				toMap1.setShipStrategy(ShipStrategyType.FORWARD);
				toMap1.setLocalStrategy(LocalStrategy.NONE);
				
				toMap2.setShipStrategy(ShipStrategyType.PARTITION_HASH, new FieldList(2, 5));
				toMap2.setLocalStrategy(LocalStrategy.NONE);
				
				
				toMap1.setRequiredGlobalProps(reqGp);
				toMap1.setRequiredLocalProps(null);
				
				toMap2.setRequiredGlobalProps(null);
				toMap2.setRequiredLocalProps(null);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(NOT_MET, report);
			}
			
			// set local strategy in first channel, so later non matching local properties do not matter
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(1, 2));
				LocalProperties lp = LocalProperties.forOrdering(new Ordering(3, null, Order.ASCENDING).appendOrdering(1, null, Order.DESCENDING));
				
				RequestedLocalProperties reqLp = new RequestedLocalProperties();
				reqLp.setGroupedFields(new FieldList(4, 1));
				
				toMap1.setShipStrategy(ShipStrategyType.FORWARD);
				toMap1.setLocalStrategy(LocalStrategy.SORT, new FieldList(5, 7), new boolean[] {false, false});
				
				toMap2.setShipStrategy(ShipStrategyType.FORWARD);
				toMap2.setLocalStrategy(LocalStrategy.NONE);
				
				toMap1.setRequiredGlobalProps(null);
				toMap1.setRequiredLocalProps(null);
				
				toMap2.setRequiredGlobalProps(null);
				toMap2.setRequiredLocalProps(reqLp);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// set local strategy in second channel, so previous non matching local properties void the match
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(1, 2));
				LocalProperties lp = LocalProperties.forOrdering(new Ordering(3, null, Order.ASCENDING).appendOrdering(1, null, Order.DESCENDING));
				
				RequestedLocalProperties reqLp = new RequestedLocalProperties();
				reqLp.setGroupedFields(new FieldList(4, 1));
				
				toMap1.setShipStrategy(ShipStrategyType.FORWARD);
				toMap1.setLocalStrategy(LocalStrategy.NONE);
				
				toMap2.setShipStrategy(ShipStrategyType.FORWARD);
				toMap2.setLocalStrategy(LocalStrategy.SORT, new FieldList(5, 7), new boolean[] {false, false});
				
				
				toMap1.setRequiredGlobalProps(null);
				toMap1.setRequiredLocalProps(reqLp);
				
				toMap2.setRequiredGlobalProps(null);
				toMap2.setRequiredLocalProps(null);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(NOT_MET, report);
			}
			
			// create the properties on the same node as the requirement
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(1, 2));
				LocalProperties lp = LocalProperties.forOrdering(new Ordering(3, null, Order.ASCENDING).appendOrdering(1, null, Order.DESCENDING));
				
				RequestedGlobalProperties reqGp = new RequestedGlobalProperties();
				reqGp.setAnyPartitioning(new FieldSet(5, 7));
				
				RequestedLocalProperties reqLp = new RequestedLocalProperties();
				reqLp.setGroupedFields(new FieldList(5, 7));
				
				toMap1.setShipStrategy(ShipStrategyType.PARTITION_HASH, new FieldList(5, 7));
				toMap1.setLocalStrategy(LocalStrategy.SORT, new FieldList(5, 7), new boolean[] {false, false});
				
				toMap2.setShipStrategy(ShipStrategyType.FORWARD);
				toMap2.setLocalStrategy(LocalStrategy.NONE);
				
				toMap1.setRequiredGlobalProps(reqGp);
				toMap1.setRequiredLocalProps(reqLp);
				
				toMap2.setRequiredGlobalProps(null);
				toMap2.setRequiredLocalProps(null);
				
				FeedbackPropertiesMeetRequirementsReport report = map2.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(MET, report);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSingleInputOperatorsChainOfThree() {
		try {
			SourcePlanNode target = new SourcePlanNode(getSourceNode(), "Source");
			
			Channel toMap1 = new Channel(target);
			SingleInputPlanNode map1 = new SingleInputPlanNode(getMapNode(), "Mapper 1", toMap1, DriverStrategy.MAP);
			
			Channel toMap2 = new Channel(map1);
			SingleInputPlanNode map2 = new SingleInputPlanNode(getMapNode(), "Mapper 2", toMap2, DriverStrategy.MAP);
			
			Channel toMap3 = new Channel(map2);
			SingleInputPlanNode map3 = new SingleInputPlanNode(getMapNode(), "Mapper 3", toMap3, DriverStrategy.MAP);
			
			// set local strategy in first channel, so later non matching local properties do not matter
			{
				GlobalProperties gp = new GlobalProperties();
				LocalProperties lp = LocalProperties.forOrdering(new Ordering(3, null, Order.ASCENDING).appendOrdering(1, null, Order.DESCENDING));
				
				RequestedLocalProperties reqLp = new RequestedLocalProperties();
				reqLp.setGroupedFields(new FieldList(4, 1));
				
				toMap1.setShipStrategy(ShipStrategyType.FORWARD);
				toMap1.setLocalStrategy(LocalStrategy.SORT, new FieldList(5, 7), new boolean[] {false, false});
				
				toMap2.setShipStrategy(ShipStrategyType.FORWARD);
				toMap2.setLocalStrategy(LocalStrategy.NONE);
				
				toMap3.setShipStrategy(ShipStrategyType.FORWARD);
				toMap3.setLocalStrategy(LocalStrategy.NONE);
				
				toMap1.setRequiredGlobalProps(null);
				toMap1.setRequiredLocalProps(null);
				
				toMap2.setRequiredGlobalProps(null);
				toMap2.setRequiredLocalProps(null);
				
				toMap3.setRequiredGlobalProps(null);
				toMap3.setRequiredLocalProps(reqLp);
				
				FeedbackPropertiesMeetRequirementsReport report = map3.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// set global strategy in first channel, so later non matching global properties do not matter
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(5, 3));
				LocalProperties lp = LocalProperties.EMPTY;
				
				RequestedGlobalProperties reqGp = new RequestedGlobalProperties();
				reqGp.setAnyPartitioning(new FieldSet(2, 3));
				
				toMap1.setShipStrategy(ShipStrategyType.PARTITION_HASH, new FieldList(1, 2));
				toMap1.setLocalStrategy(LocalStrategy.NONE);
				
				toMap2.setShipStrategy(ShipStrategyType.FORWARD);
				toMap2.setLocalStrategy(LocalStrategy.NONE);
				
				toMap3.setShipStrategy(ShipStrategyType.FORWARD);
				toMap3.setLocalStrategy(LocalStrategy.NONE);
				
				toMap1.setRequiredGlobalProps(null);
				toMap1.setRequiredLocalProps(null);
				
				toMap2.setRequiredGlobalProps(null);
				toMap2.setRequiredLocalProps(null);
				
				toMap3.setRequiredGlobalProps(reqGp);
				toMap3.setRequiredLocalProps(null);
				
				FeedbackPropertiesMeetRequirementsReport report = map3.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testNoPartialSolutionFoundTwoInputOperator() {
		try {
			SourcePlanNode target = new SourcePlanNode(getSourceNode(), "Partial Solution");

			SourcePlanNode source1 = new SourcePlanNode(getSourceNode(), "Source 1");
			SourcePlanNode source2 = new SourcePlanNode(getSourceNode(), "Source 2");
			
			Channel toMap1 = new Channel(source1);
			toMap1.setShipStrategy(ShipStrategyType.FORWARD);
			toMap1.setLocalStrategy(LocalStrategy.NONE);
			SingleInputPlanNode map1 = new SingleInputPlanNode(getMapNode(), "Mapper 1", toMap1, DriverStrategy.MAP);
			
			Channel toMap2 = new Channel(source2);
			toMap2.setShipStrategy(ShipStrategyType.FORWARD);
			toMap2.setLocalStrategy(LocalStrategy.NONE);
			SingleInputPlanNode map2 = new SingleInputPlanNode(getMapNode(), "Mapper 2", toMap2, DriverStrategy.MAP);
			
			Channel toJoin1 = new Channel(map1);
			Channel toJoin2 = new Channel(map2);
			
			toJoin1.setShipStrategy(ShipStrategyType.FORWARD);
			toJoin1.setLocalStrategy(LocalStrategy.NONE);
			toJoin2.setShipStrategy(ShipStrategyType.FORWARD);
			toJoin2.setLocalStrategy(LocalStrategy.NONE);
			
			DualInputPlanNode join = new DualInputPlanNode(getJoinNode(), "Join", toJoin1, toJoin2, DriverStrategy.HYBRIDHASH_BUILD_FIRST);
			
			FeedbackPropertiesMeetRequirementsReport report = join.checkPartialSolutionPropertiesMet(target, new GlobalProperties(), new LocalProperties());
			assertEquals(NO_PARTIAL_SOLUTION, report);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testTwoOperatorsOneIndependent() {
		try {
			SourcePlanNode target = new SourcePlanNode(getSourceNode(), "Partial Solution");
			SourcePlanNode source = new SourcePlanNode(getSourceNode(), "Other Source");
			
			Channel toMap1 = new Channel(target);
			toMap1.setShipStrategy(ShipStrategyType.FORWARD);
			toMap1.setLocalStrategy(LocalStrategy.NONE);
			SingleInputPlanNode map1 = new SingleInputPlanNode(getMapNode(), "Mapper 1", toMap1, DriverStrategy.MAP);
			
			Channel toMap2 = new Channel(source);
			toMap2.setShipStrategy(ShipStrategyType.FORWARD);
			toMap2.setLocalStrategy(LocalStrategy.NONE);
			SingleInputPlanNode map2 = new SingleInputPlanNode(getMapNode(), "Mapper 2", toMap2, DriverStrategy.MAP);
			
			Channel toJoin1 = new Channel(map1);
			Channel toJoin2 = new Channel(map2);
			
			DualInputPlanNode join = new DualInputPlanNode(getJoinNode(), "Join", toJoin1, toJoin2, DriverStrategy.HYBRIDHASH_BUILD_FIRST);
			
			Channel toAfterJoin = new Channel(join);
			toAfterJoin.setShipStrategy(ShipStrategyType.FORWARD);
			toAfterJoin.setLocalStrategy(LocalStrategy.NONE);
			SingleInputPlanNode afterJoin = new SingleInputPlanNode(getMapNode(), "After Join Mapper", toAfterJoin, DriverStrategy.MAP);
			
			// attach some properties to the non-relevant input
			{
				toMap2.setShipStrategy(ShipStrategyType.BROADCAST);
				toMap2.setLocalStrategy(LocalStrategy.SORT, new FieldList(2, 7), new boolean[] {true, true});
				
				RequestedGlobalProperties joinGp = new RequestedGlobalProperties();
				joinGp.setFullyReplicated();
				
				RequestedLocalProperties joinLp = new RequestedLocalProperties();
				joinLp.setOrdering(new Ordering(2, null, Order.ASCENDING).appendOrdering(7, null, Order.ASCENDING));
				
				toJoin2.setShipStrategy(ShipStrategyType.FORWARD);
				toJoin2.setLocalStrategy(LocalStrategy.NONE);
				toJoin2.setRequiredGlobalProps(joinGp);
				toJoin2.setRequiredLocalProps(joinLp);
			}
			
			// ------------------------------------------------------------------------------------
			
			// no properties from the partial solution, no required properties
			{
				toJoin1.setShipStrategy(ShipStrategyType.FORWARD);
				toJoin1.setLocalStrategy(LocalStrategy.NONE);
				
				GlobalProperties gp = new GlobalProperties();
				LocalProperties lp = LocalProperties.EMPTY;
				
				FeedbackPropertiesMeetRequirementsReport report = join.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// some properties from the partial solution, no required properties
			{
				toJoin1.setShipStrategy(ShipStrategyType.FORWARD);
				toJoin1.setLocalStrategy(LocalStrategy.NONE);
				
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(0));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				FeedbackPropertiesMeetRequirementsReport report = join.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			
			// produced properties match relevant input
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(0));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				RequestedGlobalProperties rgp = new RequestedGlobalProperties();
				rgp.setHashPartitioned(new FieldList(0));
				
				RequestedLocalProperties rlp = new RequestedLocalProperties();
				rlp.setGroupedFields(new FieldList(2));
				
				toJoin1.setRequiredGlobalProps(rgp);
				toJoin1.setRequiredLocalProps(rlp);
				
				toJoin1.setShipStrategy(ShipStrategyType.FORWARD);
				toJoin1.setLocalStrategy(LocalStrategy.NONE);
				
				FeedbackPropertiesMeetRequirementsReport report = join.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// produced properties do not match relevant input
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(0));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				RequestedGlobalProperties rgp = new RequestedGlobalProperties();
				rgp.setHashPartitioned(new FieldList(0));
				
				RequestedLocalProperties rlp = new RequestedLocalProperties();
				rlp.setGroupedFields(new FieldList(1, 2, 3));
				
				toJoin1.setRequiredGlobalProps(rgp);
				toJoin1.setRequiredLocalProps(rlp);
				
				toJoin1.setShipStrategy(ShipStrategyType.FORWARD);
				toJoin1.setLocalStrategy(LocalStrategy.NONE);
				
				FeedbackPropertiesMeetRequirementsReport report = join.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(NOT_MET, report);
			}
			
			// produced properties overridden before join
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(0));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				RequestedGlobalProperties rgp = new RequestedGlobalProperties();
				rgp.setHashPartitioned(new FieldList(0));
				
				RequestedLocalProperties rlp = new RequestedLocalProperties();
				rlp.setGroupedFields(new FieldList(2, 1));
				
				toMap1.setRequiredGlobalProps(rgp);
				toMap1.setRequiredLocalProps(rlp);
				
				toJoin1.setRequiredGlobalProps(null);
				toJoin1.setRequiredLocalProps(null);
				
				toJoin1.setShipStrategy(ShipStrategyType.PARTITION_HASH, new FieldList(2, 1));
				toJoin1.setLocalStrategy(LocalStrategy.SORT, new FieldList(7, 3), new boolean[] {true, false});
				
				FeedbackPropertiesMeetRequirementsReport report = join.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(MET, report);
			}
			
			// produced properties before join match, after join match as well
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(0));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				RequestedGlobalProperties rgp = new RequestedGlobalProperties();
				rgp.setHashPartitioned(new FieldList(0));
				
				RequestedLocalProperties rlp = new RequestedLocalProperties();
				rlp.setGroupedFields(new FieldList(2, 1));
				
				toMap1.setRequiredGlobalProps(null);
				toMap1.setRequiredLocalProps(null);
				
				toJoin1.setShipStrategy(ShipStrategyType.FORWARD);
				toJoin1.setLocalStrategy(LocalStrategy.NONE);
				
				toJoin1.setRequiredGlobalProps(rgp);
				toJoin1.setRequiredLocalProps(rlp);
			
				toAfterJoin.setShipStrategy(ShipStrategyType.FORWARD);
				toAfterJoin.setLocalStrategy(LocalStrategy.NONE);
				
				toAfterJoin.setRequiredGlobalProps(rgp);
				toAfterJoin.setRequiredLocalProps(rlp);
				
				FeedbackPropertiesMeetRequirementsReport report = join.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// produced properties before join match, after join do not match
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(0));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				RequestedGlobalProperties rgp1 = new RequestedGlobalProperties();
				rgp1.setHashPartitioned(new FieldList(0));
				
				RequestedGlobalProperties rgp2 = new RequestedGlobalProperties();
				rgp2.setHashPartitioned(new FieldList(3));
				
				RequestedLocalProperties rlp1 = new RequestedLocalProperties();
				rlp1.setGroupedFields(new FieldList(2, 1));
				
				RequestedLocalProperties rlp2 = new RequestedLocalProperties();
				rlp2.setGroupedFields(new FieldList(3, 4));
				
				toJoin1.setRequiredGlobalProps(rgp1);
				toJoin1.setRequiredLocalProps(rlp1);
			
				toAfterJoin.setShipStrategy(ShipStrategyType.FORWARD);
				toAfterJoin.setLocalStrategy(LocalStrategy.NONE);
				
				toAfterJoin.setRequiredGlobalProps(rgp2);
				toAfterJoin.setRequiredLocalProps(rlp2);
				
				FeedbackPropertiesMeetRequirementsReport report = afterJoin.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(NOT_MET, report);
			}
			
			// produced properties are overridden, does not matter that they do not match
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setAnyPartitioning(new FieldList(0));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				RequestedGlobalProperties rgp = new RequestedGlobalProperties();
				rgp.setHashPartitioned(new FieldList(1));
				
				RequestedLocalProperties rlp = new RequestedLocalProperties();
				rlp.setGroupedFields(new FieldList(1, 2, 3));
				
				toJoin1.setRequiredGlobalProps(null);
				toJoin1.setRequiredLocalProps(null);
				
				toJoin1.setShipStrategy(ShipStrategyType.PARTITION_HASH, new FieldList(2, 1));
				toJoin1.setLocalStrategy(LocalStrategy.SORT, new FieldList(7, 3), new boolean[] {true, false});
				
				toAfterJoin.setRequiredGlobalProps(rgp);
				toAfterJoin.setRequiredLocalProps(rlp);
				
				FeedbackPropertiesMeetRequirementsReport report = afterJoin.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(MET, report);
			}
			
			// local property overridden before join, local property mismatch after join not relevant
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setAnyPartitioning(new FieldList(0));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				RequestedLocalProperties rlp = new RequestedLocalProperties();
				rlp.setGroupedFields(new FieldList(1, 2, 3));
				
				toJoin1.setRequiredGlobalProps(null);
				toJoin1.setRequiredLocalProps(null);
				
				toJoin1.setShipStrategy(ShipStrategyType.FORWARD);
				toJoin1.setLocalStrategy(LocalStrategy.SORT, new FieldList(7, 3), new boolean[] {true, false});
				
				toAfterJoin.setRequiredGlobalProps(null);
				toAfterJoin.setRequiredLocalProps(rlp);
				
				FeedbackPropertiesMeetRequirementsReport report = afterJoin.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// local property overridden before join, global property mismatch after join void the match
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setAnyPartitioning(new FieldList(0));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				RequestedGlobalProperties rgp = new RequestedGlobalProperties();
				rgp.setHashPartitioned(new FieldList(1));
				
				RequestedLocalProperties rlp = new RequestedLocalProperties();
				rlp.setGroupedFields(new FieldList(1, 2, 3));
				
				toJoin1.setRequiredGlobalProps(null);
				toJoin1.setRequiredLocalProps(null);
				
				toJoin1.setShipStrategy(ShipStrategyType.FORWARD);
				toJoin1.setLocalStrategy(LocalStrategy.SORT, new FieldList(7, 3), new boolean[] {true, false});
				
				toAfterJoin.setRequiredGlobalProps(rgp);
				toAfterJoin.setRequiredLocalProps(rlp);
				
				FeedbackPropertiesMeetRequirementsReport report = afterJoin.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(NOT_MET, report);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testTwoOperatorsBothDependent() {
		try {
			SourcePlanNode target = new SourcePlanNode(getSourceNode(), "Partial Solution");
			
			Channel toMap1 = new Channel(target);
			toMap1.setShipStrategy(ShipStrategyType.FORWARD);
			toMap1.setLocalStrategy(LocalStrategy.NONE);
			SingleInputPlanNode map1 = new SingleInputPlanNode(getMapNode(), "Mapper 1", toMap1, DriverStrategy.MAP);
			
			Channel toMap2 = new Channel(target);
			toMap2.setShipStrategy(ShipStrategyType.FORWARD);
			toMap2.setLocalStrategy(LocalStrategy.NONE);
			SingleInputPlanNode map2 = new SingleInputPlanNode(getMapNode(), "Mapper 2", toMap2, DriverStrategy.MAP);
			
			Channel toJoin1 = new Channel(map1);
			toJoin1.setShipStrategy(ShipStrategyType.FORWARD);
			toJoin1.setLocalStrategy(LocalStrategy.NONE);
			
			Channel toJoin2 = new Channel(map2);
			toJoin2.setShipStrategy(ShipStrategyType.FORWARD);
			toJoin2.setLocalStrategy(LocalStrategy.NONE);
			
			DualInputPlanNode join = new DualInputPlanNode(getJoinNode(), "Join", toJoin1, toJoin2, DriverStrategy.HYBRIDHASH_BUILD_FIRST);
			
			Channel toAfterJoin = new Channel(join);
			toAfterJoin.setShipStrategy(ShipStrategyType.FORWARD);
			toAfterJoin.setLocalStrategy(LocalStrategy.NONE);
			SingleInputPlanNode afterJoin = new SingleInputPlanNode(getMapNode(), "After Join Mapper", toAfterJoin, DriverStrategy.MAP);
			
			// no properties from the partial solution, no required properties
			{
				GlobalProperties gp = new GlobalProperties();
				LocalProperties lp = LocalProperties.EMPTY;
				
				FeedbackPropertiesMeetRequirementsReport report = afterJoin.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// some properties from the partial solution, no required properties
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(0));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				FeedbackPropertiesMeetRequirementsReport report = afterJoin.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// test requirements on one input and met
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(0));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				RequestedGlobalProperties rgp = new RequestedGlobalProperties();
				rgp.setHashPartitioned(new FieldList(0));
				
				RequestedLocalProperties rlp = new RequestedLocalProperties();
				rlp.setGroupedFields(new FieldList(2, 1));
				
				toJoin1.setRequiredGlobalProps(rgp);
				toJoin1.setRequiredLocalProps(rlp);
				
				FeedbackPropertiesMeetRequirementsReport report = afterJoin.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// test requirements on both input and met
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(0));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				RequestedGlobalProperties rgp = new RequestedGlobalProperties();
				rgp.setHashPartitioned(new FieldList(0));
				
				RequestedLocalProperties rlp = new RequestedLocalProperties();
				rlp.setGroupedFields(new FieldList(2, 1));
				
				toJoin1.setRequiredGlobalProps(rgp);
				toJoin1.setRequiredLocalProps(rlp);
				
				toJoin2.setRequiredGlobalProps(rgp);
				toJoin2.setRequiredLocalProps(rlp);
				
				FeedbackPropertiesMeetRequirementsReport report = afterJoin.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertTrue(report != null && report != NO_PARTIAL_SOLUTION && report != NOT_MET);
			}
			
			// test requirements on both inputs, one not met
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(0));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				RequestedGlobalProperties rgp1 = new RequestedGlobalProperties();
				rgp1.setHashPartitioned(new FieldList(0));
				
				RequestedLocalProperties rlp1 = new RequestedLocalProperties();
				rlp1.setGroupedFields(new FieldList(2, 1));
				
				RequestedGlobalProperties rgp2 = new RequestedGlobalProperties();
				rgp2.setHashPartitioned(new FieldList(1));
				
				RequestedLocalProperties rlp2 = new RequestedLocalProperties();
				rlp2.setGroupedFields(new FieldList(0, 3));
				
				toJoin1.setRequiredGlobalProps(rgp1);
				toJoin1.setRequiredLocalProps(rlp1);
				
				toJoin2.setRequiredGlobalProps(rgp2);
				toJoin2.setRequiredLocalProps(rlp2);
				
				FeedbackPropertiesMeetRequirementsReport report = afterJoin.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(NOT_MET, report);
			}
			
			// test override on both inputs, later requirement ignored
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(0));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				RequestedGlobalProperties rgp = new RequestedGlobalProperties();
				rgp.setHashPartitioned(new FieldList(1));
				
				RequestedLocalProperties rlp = new RequestedLocalProperties();
				rlp.setGroupedFields(new FieldList(0, 3));
				
				toJoin1.setRequiredGlobalProps(null);
				toJoin1.setRequiredLocalProps(null);
				
				toJoin2.setRequiredGlobalProps(null);
				toJoin2.setRequiredLocalProps(null);
				
				toJoin1.setShipStrategy(ShipStrategyType.PARTITION_HASH, new FieldList(88));
				toJoin2.setShipStrategy(ShipStrategyType.BROADCAST);
				
				toAfterJoin.setRequiredGlobalProps(rgp);
				toAfterJoin.setRequiredLocalProps(rlp);
				
				FeedbackPropertiesMeetRequirementsReport report = afterJoin.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(MET, report);
			}
			
			// test override on one inputs, later requirement met
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(0));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				RequestedGlobalProperties rgp = new RequestedGlobalProperties();
				rgp.setHashPartitioned(new FieldList(0));
				
				RequestedLocalProperties rlp = new RequestedLocalProperties();
				rlp.setGroupedFields(new FieldList(2, 1));
				
				toJoin1.setShipStrategy(ShipStrategyType.PARTITION_HASH, new FieldList(88));
				toJoin2.setShipStrategy(ShipStrategyType.FORWARD);
				
				toAfterJoin.setRequiredGlobalProps(rgp);
				toAfterJoin.setRequiredLocalProps(rlp);
				
				FeedbackPropertiesMeetRequirementsReport report = afterJoin.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(PENDING, report);
			}
			
			// test override on one input, later requirement not met
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(0));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				RequestedGlobalProperties rgp = new RequestedGlobalProperties();
				rgp.setHashPartitioned(new FieldList(3));
				
				RequestedLocalProperties rlp = new RequestedLocalProperties();
				rlp.setGroupedFields(new FieldList(77, 69));
				
				toJoin1.setShipStrategy(ShipStrategyType.PARTITION_HASH, new FieldList(88));
				toJoin2.setShipStrategy(ShipStrategyType.FORWARD);
				
				toAfterJoin.setRequiredGlobalProps(rgp);
				toAfterJoin.setRequiredLocalProps(rlp);
				
				FeedbackPropertiesMeetRequirementsReport report = afterJoin.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(NOT_MET, report);
			}
			
			// test override on one input locally, later global requirement not met
			{
				GlobalProperties gp = new GlobalProperties();
				gp.setHashPartitioned(new FieldList(0));
				LocalProperties lp = LocalProperties.forGrouping(new FieldList(2, 1));
				
				RequestedGlobalProperties rgp = new RequestedGlobalProperties();
				rgp.setHashPartitioned(new FieldList(3));
				
				
				toJoin1.setShipStrategy(ShipStrategyType.FORWARD);
				toJoin1.setLocalStrategy(LocalStrategy.SORT, new FieldList(3), new boolean[] { false });
				
				toJoin2.setShipStrategy(ShipStrategyType.FORWARD);
				toJoin1.setLocalStrategy(LocalStrategy.NONE);
				
				toAfterJoin.setRequiredGlobalProps(rgp);
				toAfterJoin.setRequiredLocalProps(null);
				
				FeedbackPropertiesMeetRequirementsReport report = afterJoin.checkPartialSolutionPropertiesMet(target, gp, lp);
				assertEquals(NOT_MET, report);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static final DataSourceNode getSourceNode() {
		return new DataSourceNode(new GenericDataSourceBase<String, TextInputFormat>(new TextInputFormat(new Path("/")), new OperatorInformation<String>(BasicTypeInfo.STRING_TYPE_INFO)));
	}
	
	private static final MapNode getMapNode() {
		return new MapNode(new MapOperatorBase<String, String, GenericMap<String,String>>(new IdentityMapper<String>(), new UnaryOperatorInformation<String, String>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO), "map op"));
	}
	
	private static final MatchNode getJoinNode() {
		return new MatchNode(new JoinOperatorBase<String, String, String, GenericJoiner<String, String, String>>(new DummyJoinFunction<String>(), new BinaryOperatorInformation<String, String, String>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO), new int[] {1}, new int[] {2}, "join op"));
	}
}
