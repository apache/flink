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

package org.apache.flink.optimizer.dataproperties;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.optimizer.dag.OptimizerNode;
import org.apache.flink.optimizer.dag.SingleInputNode;
import org.apache.flink.optimizer.dag.TwoInputNode;

/**
 * Interesting properties are propagated from parent operators to child operators. They tell the child
 * what data properties would help the parent in operating in a cheaper fashion. A reduce operator, for
 * example, tells its child that partitioned data would help. If the child is a join operator, it can use
 * that knowledge to favor strategies that leave the data in a partitioned form.
 *
 * More on optimization with interesting properties can be found in the works on
 * the volcano- and cascades optimizer framework.
 */
public class InterestingProperties implements Cloneable  {

	private Set<RequestedGlobalProperties> globalProps; // the global properties, i.e. properties across partitions

	private Set<RequestedLocalProperties> localProps; // the local properties, i.e. properties within partitions

	// ------------------------------------------------------------------------

	public InterestingProperties() {
		this.globalProps = new HashSet<RequestedGlobalProperties>();
		this.localProps = new HashSet<RequestedLocalProperties>();
	}

	/**
	 * Private constructor for cloning purposes.
	 * 
	 * @param globalProps  The global properties for this new object.
	 * @param localProps The local properties for this new object.
	 */
	private InterestingProperties(Set<RequestedGlobalProperties> globalProps, Set<RequestedLocalProperties> localProps) {
		this.globalProps = globalProps;
		this.localProps = localProps;
	}

	// ------------------------------------------------------------------------

	public void addGlobalProperties(RequestedGlobalProperties props) {
		this.globalProps.add(props);
	}
	
	public void addLocalProperties(RequestedLocalProperties props) {
		this.localProps.add(props);
	}
	
	public void addInterestingProperties(InterestingProperties other) {
		this.globalProps.addAll(other.globalProps);
		this.localProps.addAll(other.localProps);
	}

	/**
	 * Gets the interesting local properties.
	 * 
	 * @return The interesting local properties.
	 */
	public Set<RequestedLocalProperties> getLocalProperties() {
		return this.localProps;
	}

	/**
	 * Gets the interesting global properties.
	 * 
	 * @return The interesting global properties.
	 */
	public Set<RequestedGlobalProperties> getGlobalProperties() {
		return this.globalProps;
	}

	public InterestingProperties filterByCodeAnnotations(OptimizerNode node, int input) {
		InterestingProperties iProps = new InterestingProperties();
		SemanticProperties props;
		if (node instanceof SingleInputNode || node instanceof TwoInputNode) {
			props = node.getSemanticProperties();
		} else {
			props = new SemanticProperties.EmptySemanticProperties();
		}

		for (RequestedGlobalProperties rgp : this.globalProps) {
			RequestedGlobalProperties filtered = rgp.filterBySemanticProperties(props, input);
			if (filtered != null && !filtered.isTrivial()) {
				iProps.addGlobalProperties(filtered);
			}
		}
		for (RequestedLocalProperties rlp : this.localProps) {
			RequestedLocalProperties filtered = rlp.filterBySemanticProperties(props, input);
			if (filtered != null && !filtered.isTrivial()) {
				iProps.addLocalProperties(filtered);
			}
		}
		return iProps;
	}
	
	public void dropTrivials() {
		for (Iterator<RequestedGlobalProperties> iter = this.globalProps.iterator(); iter.hasNext();) {
			RequestedGlobalProperties gp = iter.next();
			if (gp.isTrivial()) {
				iter.remove();
				break;
			}
		}
		
		for (Iterator<RequestedLocalProperties> iter = this.localProps.iterator(); iter.hasNext();) {
			RequestedLocalProperties lp = iter.next();
			if (lp.isTrivial()) {
				iter.remove();
				break;
			}
		}
	}

	// ------------------------------------------------------------------------
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((globalProps == null) ? 0 : globalProps.hashCode());
		result = prime * result + ((localProps == null) ? 0 : localProps.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof InterestingProperties) {
			InterestingProperties other = (InterestingProperties) obj;
			return this.globalProps.equals(other.globalProps) &&
					this.localProps.equals(other.localProps);
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "InterestingProperties [globalProps=" + this.globalProps + 
				", localProps=" + this.localProps + " ]";
	}

	@Override
	public InterestingProperties clone() {
		HashSet<RequestedGlobalProperties> globalProps = new HashSet<RequestedGlobalProperties>();
		for (RequestedGlobalProperties p : this.globalProps) {
			globalProps.add(p.clone());
		}
		HashSet<RequestedLocalProperties> localProps = new HashSet<RequestedLocalProperties>();
		for (RequestedLocalProperties p : this.localProps) {
			localProps.add(p.clone());
		}
		
		return new InterestingProperties(globalProps, localProps);
	}
}
