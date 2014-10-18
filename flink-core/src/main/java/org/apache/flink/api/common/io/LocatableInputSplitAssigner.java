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

package org.apache.flink.api.common.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.LocatableInputSplit;
import org.apache.flink.util.NetUtils;

/**
 * The locatable input split assigner assigns to each host splits that are local, before assigning
 * splits that are not local. 
 */
public final class LocatableInputSplitAssigner implements InputSplitAssigner {

	private static final Logger LOG = LoggerFactory.getLogger(LocatableInputSplitAssigner.class);


	private final Set<LocatableInputSplit> unassigned = new HashSet<LocatableInputSplit>();
	
	private final ConcurrentHashMap<String, List<LocatableInputSplit>> localPerHost = new ConcurrentHashMap<String, List<LocatableInputSplit>>();
	
	private int localAssignments;		// lock protected by the unassigned set lock
	
	private int remoteAssignments;		// lock protected by the unassigned set lock

	// --------------------------------------------------------------------------------------------
	
	public LocatableInputSplitAssigner(Collection<LocatableInputSplit> splits) {
		this.unassigned.addAll(splits);
	}
	
	public LocatableInputSplitAssigner(LocatableInputSplit[] splits) {
		Collections.addAll(this.unassigned, splits);
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public LocatableInputSplit getNextInputSplit(String host) {
		// for a null host, we return an arbitrary split
		if (host == null) {
			
			synchronized (this.unassigned) {
				Iterator<LocatableInputSplit> iter = this.unassigned.iterator();
				if (iter.hasNext()) {
					LocatableInputSplit next = iter.next();
					iter.remove();
					
					if (LOG.isDebugEnabled()) {
						LOG.debug("Assigning arbitrary split to null host.");
					}
					
					remoteAssignments++;
					return next;
				} else {
					if (LOG.isDebugEnabled()) {
						LOG.debug("No more unassigned input splits remaining.");
					}
					return null;
				}
			}
		}
		
		host = host.toLowerCase(Locale.US);
		
		// for any non-null host, we take the list of non-null splits
		List<LocatableInputSplit> localSplits = this.localPerHost.get(host);
		
		// if we have no list for this host yet, create one
		if (localSplits == null) {
			localSplits = new ArrayList<LocatableInputSplit>(16);
			
			// lock the list, to be sure that others have to wait for that host's local list
			synchronized (localSplits) {
				List<LocatableInputSplit> prior = this.localPerHost.putIfAbsent(host, localSplits);
				
				// if someone else beat us in the case to create this list, then we do not populate this one, but
				// simply work with that other list
				if (prior == null) {
					// we are the first, we populate
					
					// first, copy the remaining splits to release the lock on the set early
					// because that is shared among threads
					LocatableInputSplit[] remaining;
					synchronized (this.unassigned) {
						remaining = (LocatableInputSplit[]) this.unassigned.toArray(new LocatableInputSplit[this.unassigned.size()]);
					}
					
					for (LocatableInputSplit is : remaining) {
						if (isLocal(host, is.getHostnames())) {
							localSplits.add(is);
						}
					}
				}
				else {
					// someone else was faster
					localSplits = prior;
				}
			}
		}
		
		
		// at this point, we have a list of local splits (possibly empty)
		// we need to make sure no one else operates in the current list (that protects against
		// list creation races) and that the unassigned set is consistent
		// NOTE: we need to obtain the locks in this order, strictly!!!
		synchronized (localSplits) {
			int size = localSplits.size();
			if (size > 0) {
				synchronized (this.unassigned) {
					do {
						--size;
						LocatableInputSplit split = localSplits.remove(size);
						if (this.unassigned.remove(split)) {
							
							if (LOG.isDebugEnabled()) {
								LOG.debug("Assigning local split to host " + host);
							}
							
							localAssignments++;
							return split;
						}
					} while (size > 0);
				}
			}
		}
		
		// we did not find a local split, return any
		synchronized (this.unassigned) {
			Iterator<LocatableInputSplit> iter = this.unassigned.iterator();
			if (iter.hasNext()) {
				LocatableInputSplit next = iter.next();
				iter.remove();
				
				if (LOG.isDebugEnabled()) {
					LOG.debug("Assigning remote split to host " + host);
				}
				
				remoteAssignments++;
				return next;
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("No more input splits remaining.");
				}
				return null;
			}
		}
	}
	
	private static final boolean isLocal(String flinkHost, String[] hosts) {
		if (flinkHost == null || hosts == null) {
			return false;
		}
		for (String h : hosts) {
			if (h != null && NetUtils.getHostnameFromFQDN(h.toLowerCase()).equals(flinkHost)) {
				return true;
			}
		}
		
		return false;
	}
	
	public int getNumberOfLocalAssignments() {
		return localAssignments;
	}
	
	public int getNumberOfRemoteAssignments() {
		return remoteAssignments;
	}
}
