/**
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

package org.apache.flink.runtime.jobmanager.scheduler;

import static org.junit.Assert.*;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.flink.runtime.executiongraph.ExecutionVertex2;
import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobgraph.JobID;
import org.junit.Test;

/**
 * This class checks the functionality of the {@link org.apache.flink.runtime.jobmanager.scheduler.DefaultScheduler} class
 */
public class DefaultSchedulerTest {

	private int portNum = 10000;
	
	@Test
	public void testAddAndRemoveInstance() {
		try {
			DefaultScheduler scheduler = new DefaultScheduler();
			
			Instance i1 = getRandomInstance(2);
			Instance i2 = getRandomInstance(2);
			Instance i3 = getRandomInstance(2);
			
			assertEquals(0, scheduler.getNumberOfAvailableInstances());
			scheduler.newInstanceAvailable(i1);
			assertEquals(1, scheduler.getNumberOfAvailableInstances());
			scheduler.newInstanceAvailable(i2);
			assertEquals(2, scheduler.getNumberOfAvailableInstances());
			scheduler.newInstanceAvailable(i3);
			assertEquals(3, scheduler.getNumberOfAvailableInstances());
			
			// cannot add available instance again
			try {
				scheduler.newInstanceAvailable(i2);
				fail("Scheduler accepted instance twice");
			}
			catch (IllegalArgumentException e) {
				// bueno!
			}
			
			// some instances die
			assertEquals(3, scheduler.getNumberOfAvailableInstances());
			scheduler.instanceDied(i2);
			assertEquals(2, scheduler.getNumberOfAvailableInstances());
			
			// try to add a dead instance
			try {
				scheduler.newInstanceAvailable(i2);
				fail("Scheduler accepted dead instance");
			}
			catch (IllegalArgumentException e) {
				// stimmt
				
			}
						
			scheduler.instanceDied(i1);
			assertEquals(1, scheduler.getNumberOfAvailableInstances());
			scheduler.instanceDied(i3);
			assertEquals(0, scheduler.getNumberOfAvailableInstances());
			
			assertFalse(i1.isAlive());
			assertFalse(i2.isAlive());
			assertFalse(i3.isAlive());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	
	@Test
	public void testAssignToSlots() {
		try {
			final JobID jobId = new JobID();
			
			DefaultScheduler scheduler = new DefaultScheduler();

			scheduler.newInstanceAvailable(getRandomInstance(2));
			scheduler.newInstanceAvailable(getRandomInstance(2));
			scheduler.newInstanceAvailable(getRandomInstance(2));
			
			ResourceId id1 = new ResourceId();
			ResourceId id2 = new ResourceId();
			ResourceId id3 = new ResourceId();
			ResourceId id4 = new ResourceId();
			ResourceId id5 = new ResourceId();
			ResourceId id6 = new ResourceId();
			
			AllocatedSlot s1 = scheduler.getResourceToScheduleUnit(new ScheduledUnit(jobId, getDummyVertex(), id1), true);
			AllocatedSlot s2 = scheduler.getResourceToScheduleUnit(new ScheduledUnit(jobId, getDummyVertex(), id2), true);
			AllocatedSlot s3 = scheduler.getResourceToScheduleUnit(new ScheduledUnit(jobId, getDummyVertex(), id3), true);
			AllocatedSlot s4 = scheduler.getResourceToScheduleUnit(new ScheduledUnit(jobId, getDummyVertex(), id4), true);
			AllocatedSlot s5 = scheduler.getResourceToScheduleUnit(new ScheduledUnit(jobId, getDummyVertex(), id5), true);
			AllocatedSlot s6 = scheduler.getResourceToScheduleUnit(new ScheduledUnit(jobId, getDummyVertex(), id6), true);
			
			// no more slots available, the next call should throw an exception
			try {
				scheduler.getResourceToScheduleUnit(new ScheduledUnit(jobId, getDummyVertex(), new ResourceId()), true);
				fail("Scheduler accepted scheduling request without available resource.");
			}
			catch (NoResourceAvailableException e) {
				// expected
			}
			
			// schedule something into the same slots as before
			AllocatedSlot s1s = scheduler.getResourceToScheduleUnit(new ScheduledUnit(jobId, getDummyVertex(), id1), true);
			AllocatedSlot s2s = scheduler.getResourceToScheduleUnit(new ScheduledUnit(jobId, getDummyVertex(), id2), true);
			AllocatedSlot s3s = scheduler.getResourceToScheduleUnit(new ScheduledUnit(jobId, getDummyVertex(), id3), true);
			AllocatedSlot s4s = scheduler.getResourceToScheduleUnit(new ScheduledUnit(jobId, getDummyVertex(), id4), true);
			AllocatedSlot s5s = scheduler.getResourceToScheduleUnit(new ScheduledUnit(jobId, getDummyVertex(), id5), true);
			AllocatedSlot s6s = scheduler.getResourceToScheduleUnit(new ScheduledUnit(jobId, getDummyVertex(), id6), true);
			
			assertEquals(s1, s1s);
			assertEquals(s2, s2s);
			assertEquals(s3, s3s);
			assertEquals(s4, s4s);
			assertEquals(s5, s5s);
			assertEquals(s6, s6s);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	
	
	// --------------------------------------------------------------------------------------------
	
	private Instance getRandomInstance(int numSlots) {
		InetAddress address;
		try {
			address = InetAddress.getByName("127.0.0.1");
		} catch (UnknownHostException e) {
			throw new RuntimeException("Test could not create IP address for localhost loopback.");
		}
		
		int ipcPort = portNum++;
		int dataPort = portNum++;
		
		InstanceConnectionInfo ci = new InstanceConnectionInfo(address, ipcPort, dataPort);
		
		final long GB = 1024L*1024*1024;
		HardwareDescription resources = new HardwareDescription(4, 4*GB, 3*GB, 2*GB);
		
		return new Instance(ci, new InstanceID(), resources, numSlots);
	}
	
	private ExecutionVertex2 getDummyVertex() {
		return new ExecutionVertex2();
	}
}
