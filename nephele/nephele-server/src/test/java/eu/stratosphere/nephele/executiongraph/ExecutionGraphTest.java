/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.executiongraph;

import static org.junit.Assert.*;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.junit.Test;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGraphIterator;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionStage;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.GraphConversionException;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.InstanceListener;
import eu.stratosphere.nephele.instance.local.LocalInstanceManager;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.io.library.FileLineReader;
import eu.stratosphere.nephele.io.library.FileLineWriter;
import eu.stratosphere.nephele.jobgraph.JobFileInputVertex;
import eu.stratosphere.nephele.jobgraph.JobFileOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.template.IllegalConfigurationException;

public class ExecutionGraphTest {

	private static final String PATHNAME = "/tmp/";

	private static final String INPUTFILENAME = "input.txt";

	private static final String OUTPUTFILENAME = "output.txt";

	private static final String OUTPUTDIRNAME = "output.dir";

	private static final String INPUTCONTENT = "a\r\nb\r\nc\r\nd\r\n";

	private static final class MyInstanceListener implements InstanceListener {

		int nrAvailable = 0;

		final Map<JobID, List<AllocatedResource>> resourcesOfJobs = new HashMap<JobID, List<AllocatedResource>>();

		@Override
		public void allocatedResourceDied(JobID jobID, AllocatedResource allocatedResource) {

			--nrAvailable;
			assertTrue(nrAvailable >= 0);
			
			final List<AllocatedResource> resourcesOfJob = this.resourcesOfJobs.get(jobID);
			assertTrue(resourcesOfJob != null);
			assertTrue(resourcesOfJob.contains(allocatedResource));
			resourcesOfJob.remove(allocatedResource);
			if(resourcesOfJob.isEmpty()) {
				this.resourcesOfJobs.remove(jobID);
			}
		}

		@Override
		public void resourceAllocated(JobID jobID, AllocatedResource allocatedResource) {

			assertTrue(nrAvailable >= 0);
			++nrAvailable;
			List<AllocatedResource> resourcesOfJob = this.resourcesOfJobs.get(jobID);
			if(resourcesOfJob == null) {
				resourcesOfJob = new ArrayList<AllocatedResource>();
				this.resourcesOfJobs.put(jobID, resourcesOfJob);
			}
			assertFalse(resourcesOfJob.contains(allocatedResource));
			resourcesOfJob.add(allocatedResource);
		}
	}

	private void createInputFile() {

		File inputFile = new File(PATHNAME + INPUTFILENAME);

		if (inputFile.exists()) {
			inputFile.delete();
		}

		try {
			inputFile.createNewFile();
			FileWriter fw = new FileWriter(inputFile);
			fw.write(INPUTCONTENT);
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void createOutputDir() {

		File outputDir = new File(PATHNAME + OUTPUTDIRNAME);

		if (outputDir.exists()) {
			outputDir.delete();
		}

		outputDir.mkdir();
	}

	private void createJarFile(String className) {

		String jarPath = PATHNAME + className + ".jar";
		File jarFile = new File(jarPath);

		if (jarFile.exists()) {
			jarFile.delete();
		}

		try {
			JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarPath), new Manifest());
			String classPath = ExecutionGraphTest.class.getResource("").getPath() + className + ".class";
			File classFile = new File(classPath);

			String packageName = ExecutionGraphTest.class.getPackage().getName();
			packageName = packageName.replaceAll("\\.", "\\/");
			jos.putNextEntry(new JarEntry("/" + packageName + "/" + className + ".class"));

			FileInputStream fis = new FileInputStream(classFile);
			byte[] buffer = new byte[1024];
			int num = fis.read(buffer);

			while (num != -1) {
				jos.write(buffer, 0, num);
				num = fis.read(buffer);
			}

			fis.close();
			jos.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * input1 -> task1 -> output1
	 * output1 shares instance with input1
	 * input1 shares instance with task1
	 * no subtasks defined
	 * input1 is default, task1 is m1.large, output1 is m1.xlarge
	 * no channel types defined
	 */
	@Test
	public void testConvertJobGraphToExecutionGraph1() {

		// create input and jar files
		createInputFile();
		createJarFile("GrepTask1Input1Output");

		// create job graph
		JobGraph jg = new JobGraph("Job Graph 1");
		JobID jobID = jg.getJobID();

		// input vertex
		JobFileInputVertex i1 = new JobFileInputVertex("Input 1", jg);
		i1.setFileInputClass(FileLineReader.class);
		i1.setFilePath(new Path("file://" + PATHNAME + INPUTFILENAME));

		// task vertex
		JobTaskVertex t1 = new JobTaskVertex("Task 1", jg);
		t1.setTaskClass(GrepTask1Input1Output.class);

		// output vertex
		JobFileOutputVertex o1 = new JobFileOutputVertex("Output 1", jg);
		o1.setFileOutputClass(FileLineWriter.class);
		o1.setFilePath(new Path("file://" + PATHNAME + OUTPUTFILENAME));

		o1.setVertexToShareInstancesWith(i1);
		i1.setVertexToShareInstancesWith(t1);

		// connect vertices
		try {
			i1.connectTo(t1);
			t1.connectTo(o1);
		} catch (JobGraphDefinitionException e) {
			e.printStackTrace();
		}

		// add jar
		jg.addJar(new Path("file://" + PATHNAME + "GrepTask1Input1Output.jar"));
		Path[] paths = { new Path("file://" + PATHNAME + "GrepTask1Input1Output.jar") };

		try {
			LibraryCacheManager.addLibrary(jobID, new Path("file://" + PATHNAME + "GrepTask1Input1Output.jar"),
				new File(PATHNAME + "GrepTask1Input1Output.jar").length(), new DataInputStream(new FileInputStream(
					new File(PATHNAME + "GrepTask1Input1Output.jar"))));
			LibraryCacheManager.register(jobID, paths);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// finish creating the job graph

		// test all methods of JobGraph
		assertEquals(3, jg.getAllJobVertices().length);
		assertEquals(3, jg.getAllReachableJobVertices().length);
		assertTrue(jg.getInputVertices().hasNext());
		assertEquals(1, jg.getJars().length);
		assertEquals(new Path("file://" + PATHNAME + "GrepTask1Input1Output.jar"), jg.getJars()[0]);
		assertNotNull(jg.getJobConfiguration());
		assertEquals("Job Graph 1", jg.getName());
		assertEquals(1, jg.getNumberOfInputVertices());
		assertEquals(1, jg.getNumberOfOutputVertices());
		assertEquals(1, jg.getNumberOfTaskVertices());
		assertEquals(3, jg.getNumberOfVertices());
		assertTrue(jg.getOutputVertices().hasNext());
		assertNotNull(jg.getTaskmanagerConfiguration());
		assertTrue(jg.getTaskVertices().hasNext());
		assertTrue(jg.isAcyclic());
		assertTrue(jg.isWeaklyConnected());

		// test all methods of JobInputVertex input1
		JobInputVertex jiv1 = jg.getInputVertices().next();

		assertNotNull(jiv1.getConfiguration());
		try {
			assertEquals(1, jiv1.getInputSplits().length);
		} catch (IllegalConfigurationException e) {
			e.printStackTrace();
		}
		assertNull(jiv1.getBackwardConnection(0));
		assertNotNull(jiv1.getForwardConnection(0));
		assertNull(jiv1.getForwardConnection(1));
		assertNull(jiv1.getInstanceType());
		assertEquals(FileLineReader.class, jiv1.getInvokableClass());
		assertEquals(jg, jiv1.getJobGraph());
		assertEquals("Input 1", jiv1.getName());
		assertEquals(0, jiv1.getNumberOfBackwardConnections());
		assertEquals(1, jiv1.getNumberOfForwardConnections());
		assertEquals(-1, jiv1.getNumberOfSubtasks());
		assertEquals(-1, jiv1.getNumberOfSubtasksPerInstance());
		assertEquals("Task 1", jiv1.getVertexToShareInstancesWith().getName());

		// test all methods of JobOutputVertex output1
		JobOutputVertex jov1 = jg.getOutputVertices().next();

		assertNotNull(jov1.getConfiguration());
		assertNotNull(jov1.getBackwardConnection(0));
		assertNull(jov1.getBackwardConnection(1));
		assertNull(jov1.getForwardConnection(0));
		// assertEquals("default", jov1.getInstanceType());
		assertEquals(FileLineWriter.class, jov1.getInvokableClass());
		assertEquals(jg, jov1.getJobGraph());
		assertEquals("Output 1", jov1.getName());
		assertEquals(1, jov1.getNumberOfBackwardConnections());
		assertEquals(0, jov1.getNumberOfForwardConnections());
		assertEquals(-1, jov1.getNumberOfSubtasks());
		assertEquals(-1, jov1.getNumberOfSubtasksPerInstance());
		assertEquals("Input 1", jov1.getVertexToShareInstancesWith().getName());

		// JobTaskVertex task1
		JobTaskVertex jtv1 = jg.getTaskVertices().next();

		assertNotNull(jtv1.getConfiguration());
		assertNotNull(jtv1.getBackwardConnection(0));
		assertNull(jtv1.getBackwardConnection(1));
		assertNotNull(jtv1.getForwardConnection(0));
		assertNull(jtv1.getForwardConnection(1));
		// assertEquals("default", jtv1.getInstanceType());
		assertEquals(GrepTask1Input1Output.class, jtv1.getInvokableClass());
		assertEquals(jg, jtv1.getJobGraph());
		assertEquals("Task 1", jtv1.getName());
		assertEquals(1, jtv1.getNumberOfBackwardConnections());
		assertEquals(1, jtv1.getNumberOfForwardConnections());
		assertEquals(-1, jtv1.getNumberOfSubtasks());
		assertEquals(-1, jtv1.getNumberOfSubtasksPerInstance());
		assertEquals(GrepTask1Input1Output.class, jtv1.getTaskClass());
		assertNull(jtv1.getVertexToShareInstancesWith());

		// create the cloud manager
		final String confDir = System.getProperty("user.dir") + "/correct-conf";
		GlobalConfiguration.loadConfiguration(confDir);

		MyInstanceListener myInstanceListener = new MyInstanceListener();
		LocalInstanceManager lim = new LocalInstanceManager(confDir);
		lim.setInstanceListener(myInstanceListener);

		// now convert job graph to execution graph
		try {
			ExecutionGraph eg = new ExecutionGraph(jg, lim);

			// Set all instances to SCHEDULED before conducting the instance test
			final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(eg, true);
			while (it.hasNext()) {
				it.next().setExecutionState(ExecutionState.SCHEDULED);
			}

			// test all methods of ExecutionGraph
			assertEquals(1, eg.getInstanceTypesRequiredForCurrentStage().size());
			assertEquals(1, (int) eg.getInstanceTypesRequiredForCurrentStage()
				.get(lim.getInstanceTypeByName("default")));

			assertEquals(jobID, eg.getJobID());
			assertEquals(0, eg.getIndexOfCurrentExecutionStage());
			assertEquals(1, eg.getNumberOfInputVertices());
			assertEquals(1, eg.getNumberOfOutputVertices());
			assertEquals(1, eg.getNumberOfStages());
			assertNotNull(eg.getInputVertex(0));
			assertNull(eg.getInputVertex(1));
			assertNotNull(eg.getOutputVertex(0));
			assertNull(eg.getOutputVertex(1));
			assertNotNull(eg.getStage(0));
			assertNull(eg.getStage(1));

			// test all methods of ExecutionStage stage0
			ExecutionStage es = eg.getStage(0);

			assertEquals(3, es.getNumberOfStageMembers());
			assertEquals(0, es.getStageNumber());
			assertNotNull(es.getStageMember(0));
			assertNotNull(es.getStageMember(1));
			assertNotNull(es.getStageMember(2));
			assertNull(es.getStageMember(3));

			// test all methods of ExecutionGroupVertex
			ExecutionGroupVertex egv0 = null; // input1
			ExecutionGroupVertex egv1 = null; // output1
			ExecutionGroupVertex egv2 = null; // task1

			if (es.getStageMember(0).getName().equals("Input 1")) {
				egv0 = es.getStageMember(0);
			} else if (es.getStageMember(0).getName().equals("Output 1")) {
				egv1 = es.getStageMember(0);
			} else {
				egv2 = es.getStageMember(0);
			}

			if (es.getStageMember(1).getName().equals("Input 1")) {
				egv0 = es.getStageMember(1);
			} else if (es.getStageMember(1).getName().equals("Output 1")) {
				egv1 = es.getStageMember(1);
			} else {
				egv2 = es.getStageMember(1);
			}

			if (es.getStageMember(2).getName().equals("Input 1")) {
				egv0 = es.getStageMember(2);
			} else if (es.getStageMember(2).getName().equals("Output 1")) {
				egv1 = es.getStageMember(2);
			} else {
				egv2 = es.getStageMember(2);
			}

			// egv0 (input1)
			assertNull(egv0.getBackwardEdge(0));
			assertNotNull(egv0.getConfiguration());
			assertEquals(1, egv0.getCurrentNumberOfGroupMembers());
			assertNotNull(egv0.getExecutionSignature());
			assertEquals(es, egv0.getExecutionStage());
			assertNotNull(egv0.getForwardEdge(0));
			assertNull(egv0.getForwardEdge(1));
			assertNotNull(egv0.getForwardEdges(egv2));
			assertNotNull(egv0.getGroupMember(0));
			assertNull(egv0.getGroupMember(1));
			assertEquals(1, egv0.getInputSplits().length);
			assertEquals(-1, egv0.getMaximumNumberOfGroupMembers());
			assertEquals(1, egv0.getMinimumNumberOfGroupMember());
			assertEquals("Input 1", egv0.getName());
			assertEquals(0, egv0.getNumberOfBackwardLinks());
			assertEquals(1, egv0.getNumberOfForwardLinks());
			assertEquals(1, egv0.getNumberOfSubtasksPerInstance());
			assertEquals(0, egv0.getStageNumber());
			assertEquals(-1, egv0.getUserDefinedNumberOfMembers());
			assertEquals(lim.getDefaultInstanceType(), egv0.getInstanceType());
			assertEquals("Task 1", egv0.getVertexToShareInstancesWith().getName());

			// egv1 (output1)
			assertNotNull(egv1.getBackwardEdge(0));
			assertNull(egv1.getBackwardEdge(1));
			assertNotNull(egv1.getBackwardEdges(egv2));
			assertNotNull(egv1.getConfiguration());
			assertEquals(1, egv1.getCurrentNumberOfGroupMembers());
			assertNotNull(egv1.getExecutionSignature());
			assertEquals(es, egv1.getExecutionStage());
			assertNull(egv1.getForwardEdge(0));
			assertNotNull(egv1.getGroupMember(0));
			assertNull(egv1.getGroupMember(1));
			assertEquals(1, egv1.getMaximumNumberOfGroupMembers());
			assertEquals(1, egv1.getMinimumNumberOfGroupMember());
			assertEquals("Output 1", egv1.getName());
			assertEquals(1, egv1.getNumberOfBackwardLinks());
			assertEquals(0, egv1.getNumberOfForwardLinks());
			assertEquals(1, egv1.getNumberOfSubtasksPerInstance());
			assertEquals(0, egv1.getStageNumber());
			assertEquals(-1, egv1.getUserDefinedNumberOfMembers());
			assertEquals(lim.getInstanceTypeByName("default"), egv1.getInstanceType());
			assertEquals("Input 1", egv1.getVertexToShareInstancesWith().getName());

			// egv2 (task1)
			assertNotNull(egv2.getBackwardEdge(0));
			assertNull(egv2.getBackwardEdge(1));
			assertNotNull(egv2.getBackwardEdges(egv0));
			assertNotNull(egv2.getConfiguration());
			assertEquals(1, egv2.getCurrentNumberOfGroupMembers());
			assertNotNull(egv2.getExecutionSignature());
			assertEquals(es, egv2.getExecutionStage());
			assertNotNull(egv2.getForwardEdge(0));
			assertNull(egv2.getForwardEdge(1));
			assertNotNull(egv2.getForwardEdges(egv1));
			assertNotNull(egv2.getGroupMember(0));
			assertNull(egv2.getGroupMember(1));
			assertEquals(-1, egv2.getMaximumNumberOfGroupMembers());
			assertEquals(1, egv2.getMinimumNumberOfGroupMember());
			assertEquals("Task 1", egv2.getName());
			assertEquals(1, egv2.getNumberOfBackwardLinks());
			assertEquals(1, egv2.getNumberOfForwardLinks());
			assertEquals(1, egv2.getNumberOfSubtasksPerInstance());
			assertEquals(0, egv2.getStageNumber());
			assertEquals(-1, egv2.getUserDefinedNumberOfMembers());
			assertEquals(lim.getInstanceTypeByName("default"), egv2.getInstanceType());
			assertNull(egv2.getVertexToShareInstancesWith());

			// test all methods of ExecutionVertex
			ExecutionVertex ev0 = egv0.getGroupMember(0); // input1
			ExecutionVertex ev1 = egv1.getGroupMember(0); // output1
			ExecutionVertex ev2 = egv2.getGroupMember(0); // task1

			// ev0 (input1)
			assertNotNull(ev0.getEnvironment());
			assertEquals(egv0, ev0.getGroupVertex());
			assertNotNull(ev0.getID());
			assertEquals("Input 1", ev0.getName());
			assertEquals(lim.getInstanceTypeByName("default"), ev0.getAllocatedResource().getInstance().getType());

			// ev1 (output1)
			assertNotNull(ev1.getEnvironment());
			assertEquals(egv1, ev1.getGroupVertex());
			assertNotNull(ev1.getID());
			assertEquals("Output 1", ev1.getName());
			assertEquals(lim.getInstanceTypeByName("default"), ev1.getAllocatedResource().getInstance().getType());

			// ev2 (task1)
			assertNotNull(ev2.getEnvironment());
			assertEquals(egv2, ev2.getGroupVertex());
			assertNotNull(ev2.getID());
			assertEquals("Task 1", ev2.getName());
			assertEquals(lim.getInstanceTypeByName("default"), ev2.getAllocatedResource().getInstance().getType());

			assertEquals(ev0.getAllocatedResource(), ev1.getAllocatedResource());
			assertEquals(ev0.getAllocatedResource(), ev2.getAllocatedResource());

			// test channels
			assertEquals(ChannelType.NETWORK, eg.getChannelType(ev0, ev2));
			assertEquals(ChannelType.NETWORK, eg.getChannelType(ev2, ev1));

		} catch (GraphConversionException e) {
			e.printStackTrace();
		}

		finally {
			// Stop the local instance manager
			lim.shutdown();
		}
	}

	/*
	 * input1 -> task1 -> output1
	 * no subtasks defined
	 * input1 is default, task1 is m1.large, output1 is m1.xlarge
	 * all channels are INMEMORY
	 */
	@Test
	public void testConvertJobGraphToExecutionGraph2() {

		// create input and jar files
		createInputFile();
		createJarFile("GrepTask1Input1Output");

		// create job graph
		JobGraph jg = new JobGraph("Job Graph 1");
		JobID jobID = jg.getJobID();

		// input vertex
		JobFileInputVertex i1 = new JobFileInputVertex("Input 1", jg);
		i1.setFileInputClass(FileLineReader.class);
		i1.setFilePath(new Path("file://" + PATHNAME + INPUTFILENAME));

		// task vertex
		JobTaskVertex t1 = new JobTaskVertex("Task 1", jg);
		t1.setTaskClass(GrepTask1Input1Output.class);

		// output vertex
		JobFileOutputVertex o1 = new JobFileOutputVertex("Output 1", jg);
		o1.setFileOutputClass(FileLineWriter.class);
		o1.setFilePath(new Path("file://" + PATHNAME + OUTPUTFILENAME));

		// connect vertices
		try {
			i1.connectTo(t1, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			t1.connectTo(o1, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
		} catch (JobGraphDefinitionException e) {
			e.printStackTrace();
		}

		// add jar
		jg.addJar(new Path("file://" + PATHNAME + "GrepTask1Input1Output.jar"));
		Path[] paths = { new Path("file://" + PATHNAME + "GrepTask1Input1Output.jar") };

		try {
			LibraryCacheManager.addLibrary(jobID, new Path("file://" + PATHNAME + "GrepTask1Input1Output.jar"),
				new File(PATHNAME + "GrepTask1Input1Output.jar").length(), new DataInputStream(new FileInputStream(
					new File(PATHNAME + "GrepTask1Input1Output.jar"))));
			LibraryCacheManager.register(jobID, paths);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// create the cloud manager
		final String confDir = System.getProperty("user.dir") + "/correct-conf";
		GlobalConfiguration.loadConfiguration(confDir);

		MyInstanceListener myInstanceListener = new MyInstanceListener();
		final LocalInstanceManager lim = new LocalInstanceManager(confDir);
		lim.setInstanceListener(myInstanceListener);

		// now convert job graph to execution graph
		try {
			ExecutionGraph eg = new ExecutionGraph(jg, lim);

			// Set all instances to SCHEDULED before conducting the instance test
			final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(eg, true);
			while (it.hasNext()) {
				it.next().setExecutionState(ExecutionState.SCHEDULED);
			}

			// test instance types in ExecutionGraph
			assertEquals(1, eg.getInstanceTypesRequiredForCurrentStage().size());
			assertEquals(1, (int) eg.getInstanceTypesRequiredForCurrentStage().get(lim.getDefaultInstanceType()));

			// stage0
			ExecutionStage es = eg.getStage(0);

			ExecutionGroupVertex egv0 = null; // input1
			ExecutionGroupVertex egv1 = null; // output1
			ExecutionGroupVertex egv2 = null; // task1

			if (es.getStageMember(0).getName().equals("Input 1")) {
				egv0 = es.getStageMember(0);
			} else if (es.getStageMember(0).getName().equals("Output 1")) {
				egv1 = es.getStageMember(0);
			} else {
				egv2 = es.getStageMember(0);
			}

			if (es.getStageMember(1).getName().equals("Input 1")) {
				egv0 = es.getStageMember(1);
			} else if (es.getStageMember(1).getName().equals("Output 1")) {
				egv1 = es.getStageMember(1);
			} else {
				egv2 = es.getStageMember(1);
			}

			if (es.getStageMember(2).getName().equals("Input 1")) {
				egv0 = es.getStageMember(2);
			} else if (es.getStageMember(2).getName().equals("Output 1")) {
				egv1 = es.getStageMember(2);
			} else {
				egv2 = es.getStageMember(2);
			}

			ExecutionVertex ev0 = egv0.getGroupMember(0); // input1
			ExecutionVertex ev1 = egv1.getGroupMember(0); // output1
			ExecutionVertex ev2 = egv2.getGroupMember(0); // task1

			// ev0 (input1)
			assertEquals(lim.getDefaultInstanceType(), ev0.getAllocatedResource().getInstance().getType());

			// ev1 (output1)
			assertEquals(lim.getDefaultInstanceType(), ev1.getAllocatedResource().getInstance().getType());

			// ev2 (task1)
			assertEquals(lim.getDefaultInstanceType(), ev2.getAllocatedResource().getInstance().getType());

			assertEquals(ev0.getAllocatedResource(), ev1.getAllocatedResource());
			assertEquals(ev0.getAllocatedResource(), ev2.getAllocatedResource());

		} catch (GraphConversionException e) {
			e.printStackTrace();
		}

		finally {
			// Stop the local instance manager
			lim.shutdown();
		}
	}

	/*
	 * input1 -> task1 ->
	 * task3 -> output1
	 * input2 -> task2 ->
	 * each vertex has 2 subtasks
	 * no instance types defined
	 * no channel types defined
	 */
	@Test
	public void testConvertJobGraphToExecutionGraph3() {

		// create input and jar files
		createInputFile();
		createOutputDir();
		createJarFile("GrepTask1Input1Output");
		createJarFile("GrepTask2Inputs1Output");

		// create job graph
		JobGraph jg = new JobGraph("Job Graph 1");
		JobID jobID = jg.getJobID();

		// input vertex
		JobFileInputVertex i1 = new JobFileInputVertex("Input 1", jg);
		i1.setFileInputClass(FileLineReader.class);
		i1.setFilePath(new Path("file://" + PATHNAME + INPUTFILENAME));
		i1.setNumberOfSubtasks(2);

		JobFileInputVertex i2 = new JobFileInputVertex("Input 2", jg);
		i2.setFileInputClass(FileLineReader.class);
		i2.setFilePath(new Path("file://" + PATHNAME + INPUTFILENAME));
		i2.setNumberOfSubtasks(2);

		// task vertex
		JobTaskVertex t1 = new JobTaskVertex("Task 1", jg);
		t1.setTaskClass(GrepTask1Input1Output.class);
		t1.setNumberOfSubtasks(2);

		JobTaskVertex t2 = new JobTaskVertex("Task 2", jg);
		t2.setTaskClass(GrepTask1Input1Output.class);
		t2.setNumberOfSubtasks(2);

		JobTaskVertex t3 = new JobTaskVertex("Task 3", jg);
		t3.setTaskClass(GrepTask2Inputs1Output.class);
		t3.setNumberOfSubtasks(2);

		// output vertex
		JobFileOutputVertex o1 = new JobFileOutputVertex("Output 1", jg);
		o1.setFileOutputClass(FileLineWriter.class);
		o1.setFilePath(new Path("file://" + PATHNAME + OUTPUTDIRNAME));
		o1.setNumberOfSubtasks(2);

		i1.setVertexToShareInstancesWith(t1);
		t1.setVertexToShareInstancesWith(t3);
		i2.setVertexToShareInstancesWith(t2);
		t2.setVertexToShareInstancesWith(t3);
		t3.setVertexToShareInstancesWith(o1);

		// connect vertices
		try {
			i1.connectTo(t1);
			i2.connectTo(t2);
			t1.connectTo(t3);
			t2.connectTo(t3);
			t3.connectTo(o1);
		} catch (JobGraphDefinitionException e) {
			e.printStackTrace();
		}

		// add jar
		jg.addJar(new Path("file://" + PATHNAME + "GrepTask1Input1Output.jar"));
		jg.addJar(new Path("file://" + PATHNAME + "GrepTask2Inputs1Output.jar"));
		Path[] paths = { new Path("file://" + PATHNAME + "GrepTask1Input1Output.jar"),
			new Path("file://" + PATHNAME + "GrepTask2Inputs1Output.jar") };

		try {
			LibraryCacheManager.addLibrary(jobID, new Path("file://" + PATHNAME + "GrepTask1Input1Output.jar"),
				new File(PATHNAME + "GrepTask1Input1Output.jar").length(), new DataInputStream(new FileInputStream(
					new File(PATHNAME + "GrepTask1Input1Output.jar"))));
			LibraryCacheManager.addLibrary(jobID, new Path("file://" + PATHNAME + "GrepTask2Inputs1Output.jar"),
				new File(PATHNAME + "GrepTask2Inputs1Output.jar").length(), new DataInputStream(new FileInputStream(
					new File(PATHNAME + "GrepTask2Inputs1Output.jar"))));
			LibraryCacheManager.register(jobID, paths);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// create the cloud manager
		final String confDir = System.getProperty("user.dir") + "/correct-conf";
		GlobalConfiguration.loadConfiguration(confDir);

		final MyInstanceListener myInstanceListener = new MyInstanceListener();
		final LocalInstanceManager lim = new LocalInstanceManager(confDir);
		lim.setInstanceListener(myInstanceListener);

		// now convert job graph to execution graph
		try {
			ExecutionGraph eg = new ExecutionGraph(jg, lim);

			// Set all instances to SCHEDULED before conducting the instance test
			final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(eg, true);
			while (it.hasNext()) {
				it.next().setExecutionState(ExecutionState.SCHEDULED);
			}

			// test instance types in ExecutionGraph
			assertEquals(1, eg.getInstanceTypesRequiredForCurrentStage().size());
			assertEquals(2, (int) eg.getInstanceTypesRequiredForCurrentStage().get(lim.getDefaultInstanceType()));

			// stage0
			ExecutionStage es = eg.getStage(0);

			ExecutionGroupVertex egv0 = null; // input1
			ExecutionGroupVertex egv1 = null; // input2
			ExecutionGroupVertex egv2 = null; // task1
			ExecutionGroupVertex egv3 = null; // task2
			ExecutionGroupVertex egv4 = null; // task3
			ExecutionGroupVertex egv5 = null; // output1

			if (es.getStageMember(0).getName().equals("Input 1")) {
				egv0 = es.getStageMember(0);
			} else if (es.getStageMember(0).getName().equals("Input 2")) {
				egv1 = es.getStageMember(0);
			} else if (es.getStageMember(0).getName().equals("Task 1")) {
				egv2 = es.getStageMember(0);
			} else if (es.getStageMember(0).getName().equals("Task 2")) {
				egv3 = es.getStageMember(0);
			} else if (es.getStageMember(0).getName().equals("Task 3")) {
				egv4 = es.getStageMember(0);
			} else {
				egv5 = es.getStageMember(0);
			}

			if (es.getStageMember(1).getName().equals("Input 1")) {
				egv0 = es.getStageMember(1);
			} else if (es.getStageMember(1).getName().equals("Input 2")) {
				egv1 = es.getStageMember(1);
			} else if (es.getStageMember(1).getName().equals("Task 1")) {
				egv2 = es.getStageMember(1);
			} else if (es.getStageMember(1).getName().equals("Task 2")) {
				egv3 = es.getStageMember(1);
			} else if (es.getStageMember(1).getName().equals("Task 3")) {
				egv4 = es.getStageMember(1);
			} else {
				egv5 = es.getStageMember(1);
			}

			if (es.getStageMember(2).getName().equals("Input 1")) {
				egv0 = es.getStageMember(2);
			} else if (es.getStageMember(2).getName().equals("Input 2")) {
				egv1 = es.getStageMember(2);
			} else if (es.getStageMember(2).getName().equals("Task 1")) {
				egv2 = es.getStageMember(2);
			} else if (es.getStageMember(2).getName().equals("Task 2")) {
				egv3 = es.getStageMember(2);
			} else if (es.getStageMember(2).getName().equals("Task 3")) {
				egv4 = es.getStageMember(2);
			} else {
				egv5 = es.getStageMember(2);
			}

			if (es.getStageMember(3).getName().equals("Input 1")) {
				egv0 = es.getStageMember(3);
			} else if (es.getStageMember(3).getName().equals("Input 2")) {
				egv1 = es.getStageMember(3);
			} else if (es.getStageMember(3).getName().equals("Task 1")) {
				egv2 = es.getStageMember(3);
			} else if (es.getStageMember(3).getName().equals("Task 2")) {
				egv3 = es.getStageMember(3);
			} else if (es.getStageMember(3).getName().equals("Task 3")) {
				egv4 = es.getStageMember(3);
			} else {
				egv5 = es.getStageMember(3);
			}

			if (es.getStageMember(4).getName().equals("Input 1")) {
				egv0 = es.getStageMember(4);
			} else if (es.getStageMember(4).getName().equals("Input 2")) {
				egv1 = es.getStageMember(4);
			} else if (es.getStageMember(4).getName().equals("Task 1")) {
				egv2 = es.getStageMember(4);
			} else if (es.getStageMember(4).getName().equals("Task 2")) {
				egv3 = es.getStageMember(4);
			} else if (es.getStageMember(4).getName().equals("Task 3")) {
				egv4 = es.getStageMember(4);
			} else {
				egv5 = es.getStageMember(4);
			}

			if (es.getStageMember(5).getName().equals("Input 1")) {
				egv0 = es.getStageMember(5);
			} else if (es.getStageMember(5).getName().equals("Input 2")) {
				egv1 = es.getStageMember(5);
			} else if (es.getStageMember(5).getName().equals("Task 1")) {
				egv2 = es.getStageMember(5);
			} else if (es.getStageMember(5).getName().equals("Task 2")) {
				egv3 = es.getStageMember(5);
			} else if (es.getStageMember(5).getName().equals("Task 3")) {
				egv4 = es.getStageMember(5);
			} else {
				egv5 = es.getStageMember(5);
			}

			ExecutionVertex i1_0 = egv0.getGroupMember(0); // input1
			ExecutionVertex i1_1 = egv0.getGroupMember(1); // input1
			ExecutionVertex i2_0 = egv1.getGroupMember(0); // input2
			ExecutionVertex i2_1 = egv1.getGroupMember(1); // input2
			ExecutionVertex t1_0 = egv2.getGroupMember(0); // task1
			ExecutionVertex t1_1 = egv2.getGroupMember(1); // task1
			ExecutionVertex t2_0 = egv3.getGroupMember(0); // task2
			ExecutionVertex t2_1 = egv3.getGroupMember(1); // task2
			ExecutionVertex t3_0 = egv4.getGroupMember(0); // task3
			ExecutionVertex t3_1 = egv4.getGroupMember(1); // task3
			ExecutionVertex o1_0 = egv5.getGroupMember(0); // output1
			ExecutionVertex o1_1 = egv5.getGroupMember(1); // otuput1

			// instance 1
			assertTrue((t1_0.getAllocatedResource().equals(i1_0.getAllocatedResource()) && !t1_0.getAllocatedResource()
				.equals(i1_1.getAllocatedResource()))
				|| (!t1_0.getAllocatedResource().equals(i1_0.getAllocatedResource()) && t1_0.getAllocatedResource()
					.equals(i1_1.getAllocatedResource())));
			assertTrue((t1_0.getAllocatedResource().equals(i2_0.getAllocatedResource()) && !t1_0.getAllocatedResource()
				.equals(i2_1.getAllocatedResource()))
				|| (!t1_0.getAllocatedResource().equals(i2_0.getAllocatedResource()) && t1_0.getAllocatedResource()
					.equals(i2_1.getAllocatedResource())));
			assertTrue((t1_0.getAllocatedResource().equals(t2_0.getAllocatedResource()) && !t1_0.getAllocatedResource()
				.equals(t2_1.getAllocatedResource()))
				|| (!t1_0.getAllocatedResource().equals(t2_0.getAllocatedResource()) && t1_0.getAllocatedResource()
					.equals(t2_1.getAllocatedResource())));
			assertTrue((t1_0.getAllocatedResource().equals(t3_0.getAllocatedResource()) && !t1_0.getAllocatedResource()
				.equals(t3_1.getAllocatedResource()))
				|| (!t1_0.getAllocatedResource().equals(t3_0.getAllocatedResource()) && t1_0.getAllocatedResource()
					.equals(t3_1.getAllocatedResource())));
			assertTrue((t1_0.getAllocatedResource().equals(o1_0.getAllocatedResource()) && !t1_0.getAllocatedResource()
				.equals(o1_1.getAllocatedResource()))
				|| (!t1_0.getAllocatedResource().equals(o1_0.getAllocatedResource()) && t1_0.getAllocatedResource()
					.equals(o1_1.getAllocatedResource())));

			// instance 2
			assertTrue((t1_1.getAllocatedResource().equals(i1_0.getAllocatedResource()) && !t1_1.getAllocatedResource()
				.equals(i1_1.getAllocatedResource()))
				|| (!t1_1.getAllocatedResource().equals(i1_0.getAllocatedResource()) && t1_1.getAllocatedResource()
					.equals(i1_1.getAllocatedResource())));
			assertTrue((t1_1.getAllocatedResource().equals(i2_0.getAllocatedResource()) && !t1_1.getAllocatedResource()
				.equals(i2_1.getAllocatedResource()))
				|| (!t1_1.getAllocatedResource().equals(i2_0.getAllocatedResource()) && t1_1.getAllocatedResource()
					.equals(i2_1.getAllocatedResource())));
			assertTrue((t1_1.getAllocatedResource().equals(t2_0.getAllocatedResource()) && !t1_1.getAllocatedResource()
				.equals(t2_1.getAllocatedResource()))
				|| (!t1_1.getAllocatedResource().equals(t2_0.getAllocatedResource()) && t1_1.getAllocatedResource()
					.equals(t2_1.getAllocatedResource())));
			assertTrue((t1_1.getAllocatedResource().equals(t3_0.getAllocatedResource()) && !t1_1.getAllocatedResource()
				.equals(t3_1.getAllocatedResource()))
				|| (!t1_1.getAllocatedResource().equals(t3_0.getAllocatedResource()) && t1_1.getAllocatedResource()
					.equals(t3_1.getAllocatedResource())));
			assertTrue((t1_1.getAllocatedResource().equals(o1_0.getAllocatedResource()) && !t1_1.getAllocatedResource()
				.equals(o1_1.getAllocatedResource()))
				|| (!t1_1.getAllocatedResource().equals(o1_0.getAllocatedResource()) && t1_1.getAllocatedResource()
					.equals(o1_1.getAllocatedResource())));

		} catch (GraphConversionException e) {
			e.printStackTrace();
		}

		finally {
			// Stop the local instance manager
			lim.shutdown();
		}
	}

	/*
	 * input1 -> task1 -> output1
	 * -> task3 -> task4
	 * input2 -> task2 -> output2
	 * all subtasks defined
	 * all instance types defined
	 * all channel types defined
	 */
	@Test
	public void testConvertJobGraphToExecutionGraph4() {

		// create input and jar files
		createInputFile();
		createOutputDir();
		createJarFile("GrepTask1Input1Output");
		createJarFile("GrepTask2Inputs1Output");
		createJarFile("GrepTask1Input2Outputs");

		// create job graph
		JobGraph jg = new JobGraph("Job Graph 1");
		JobID jobID = jg.getJobID();

		// input vertex
		JobFileInputVertex i1 = new JobFileInputVertex("Input 1", jg);
		i1.setFileInputClass(FileLineReader.class);
		i1.setFilePath(new Path("file://" + PATHNAME + INPUTFILENAME));
		i1.setNumberOfSubtasks(4);
		i1.setNumberOfSubtasksPerInstance(2);

		JobFileInputVertex i2 = new JobFileInputVertex("Input 2", jg);
		i2.setFileInputClass(FileLineReader.class);
		i2.setFilePath(new Path("file://" + PATHNAME + INPUTFILENAME));
		i2.setNumberOfSubtasks(4);
		i2.setNumberOfSubtasksPerInstance(2);

		// task vertex
		JobTaskVertex t1 = new JobTaskVertex("Task 1", jg);
		t1.setTaskClass(GrepTask1Input1Output.class);
		t1.setNumberOfSubtasks(4);
		t1.setNumberOfSubtasksPerInstance(2);

		JobTaskVertex t2 = new JobTaskVertex("Task 2", jg);
		t2.setTaskClass(GrepTask1Input1Output.class);
		t2.setNumberOfSubtasks(4);
		t2.setNumberOfSubtasksPerInstance(2);

		JobTaskVertex t3 = new JobTaskVertex("Task 3", jg);
		t3.setTaskClass(GrepTask2Inputs1Output.class);
		t3.setNumberOfSubtasks(8);
		t3.setNumberOfSubtasksPerInstance(4);

		JobTaskVertex t4 = new JobTaskVertex("Task 4", jg);
		t4.setTaskClass(GrepTask1Input2Outputs.class);
		t4.setNumberOfSubtasks(8);
		t4.setNumberOfSubtasksPerInstance(4);

		// output vertex
		JobFileOutputVertex o1 = new JobFileOutputVertex("Output 1", jg);
		o1.setFileOutputClass(FileLineWriter.class);
		o1.setFilePath(new Path("file://" + PATHNAME + OUTPUTDIRNAME));
		o1.setNumberOfSubtasks(4);
		o1.setNumberOfSubtasksPerInstance(2);

		JobFileOutputVertex o2 = new JobFileOutputVertex("Output 2", jg);
		o2.setFileOutputClass(FileLineWriter.class);
		o2.setFilePath(new Path("file://" + PATHNAME + OUTPUTDIRNAME));
		o2.setNumberOfSubtasks(4);
		o2.setNumberOfSubtasksPerInstance(2);

		o1.setVertexToShareInstancesWith(o2);

		// connect vertices
		try {
			i1.connectTo(t1, ChannelType.FILE, CompressionLevel.NO_COMPRESSION);
			i2.connectTo(t2, ChannelType.FILE, CompressionLevel.NO_COMPRESSION);
			t1.connectTo(t3, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);
			t2.connectTo(t3, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);
			t3.connectTo(t4, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			t4.connectTo(o1, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);
			t4.connectTo(o2, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);

		} catch (JobGraphDefinitionException e) {
			e.printStackTrace();
		}

		// add jars
		jg.addJar(new Path("file://" + PATHNAME + "GrepTask1Input1Output.jar"));
		jg.addJar(new Path("file://" + PATHNAME + "GrepTask2Inputs1Output.jar"));
		jg.addJar(new Path("file://" + PATHNAME + "GrepTask1Input2Outputs.jar"));
		Path[] paths = { new Path("file://" + PATHNAME + "GrepTask1Input1Output.jar"),
			new Path("file://" + PATHNAME + "GrepTask2Inputs1Output.jar"),
			new Path("file://" + PATHNAME + "GrepTask1Input2Outputs.jar") };

		try {
			LibraryCacheManager.addLibrary(jobID, new Path("file://" + PATHNAME + "GrepTask1Input1Output.jar"),
				new File(PATHNAME + "GrepTask1Input1Output.jar").length(), new DataInputStream(new FileInputStream(
					new File(PATHNAME + "GrepTask1Input1Output.jar"))));
			LibraryCacheManager.addLibrary(jobID, new Path("file://" + PATHNAME + "GrepTask2Inputs1Output.jar"),
				new File(PATHNAME + "GrepTask2Inputs1Output.jar").length(), new DataInputStream(new FileInputStream(
					new File(PATHNAME + "GrepTask2Inputs1Output.jar"))));
			LibraryCacheManager.addLibrary(jobID, new Path("file://" + PATHNAME + "GrepTask1Input2Outputs.jar"),
				new File(PATHNAME + "GrepTask1Input2Outputs.jar").length(), new DataInputStream(new FileInputStream(
					new File(PATHNAME + "GrepTask1Input2Outputs.jar"))));
			LibraryCacheManager.register(jobID, paths);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// create the cloud manager
		final String confDir = System.getProperty("user.dir") + "/correct-conf";
		GlobalConfiguration.loadConfiguration(confDir);

		final MyInstanceListener myInstanceListener = new MyInstanceListener();
		final LocalInstanceManager lim = new LocalInstanceManager(confDir);
		lim.setInstanceListener(myInstanceListener);

		// now convert job graph to execution graph
		try {

			ExecutionGraph eg = new ExecutionGraph(jg, lim);

			// Set all instances to SCHEDULED before conducting the instance test
			Iterator<ExecutionVertex> it = new ExecutionGraphIterator(eg, true);
			while (it.hasNext()) {
				it.next().setExecutionState(ExecutionState.SCHEDULED);
			}

			// test instance types in ExecutionGraph
			assertEquals(1, eg.getInstanceTypesRequiredForCurrentStage().size());
			assertEquals(4, (int) eg.getInstanceTypesRequiredForCurrentStage()
				.get(lim.getInstanceTypeByName("default")));

			// Fake transition to next stage by triggering execution state changes manually
			it = new ExecutionGraphIterator(eg, eg.getIndexOfCurrentExecutionStage(), true, true);
			while (it.hasNext()) {
				final ExecutionVertex ev = it.next();
				ev.setExecutionState(ExecutionState.SCHEDULED);
				ev.setExecutionState(ExecutionState.ASSIGNING);
				ev.setExecutionState(ExecutionState.ASSIGNED);
				ev.setExecutionState(ExecutionState.READY);
				ev.setExecutionState(ExecutionState.RUNNING);
				ev.setExecutionState(ExecutionState.FINISHING);
				ev.setExecutionState(ExecutionState.FINISHED);
			}

			assertEquals(1, eg.getInstanceTypesRequiredForCurrentStage().size());
			assertEquals(8, (int) eg.getInstanceTypesRequiredForCurrentStage()
				.get(lim.getInstanceTypeByName("default")));

		} catch (GraphConversionException e) {
			e.printStackTrace();
		}

		finally {
			// Stop the local instance manager
			lim.shutdown();
		}
	}
}
