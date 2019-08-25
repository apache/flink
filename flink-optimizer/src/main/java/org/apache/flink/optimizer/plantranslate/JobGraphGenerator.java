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

package org.apache.flink.optimizer.plantranslate;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.aggregators.AggregatorRegistry;
import org.apache.flink.api.common.aggregators.AggregatorWithName;
import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.java.io.BlockingShuffleOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.AlgorithmOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.dag.TempMode;
import org.apache.flink.optimizer.plan.BulkIterationPlanNode;
import org.apache.flink.optimizer.plan.BulkPartialSolutionPlanNode;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.IterationPlanNode;
import org.apache.flink.optimizer.plan.NAryUnionPlanNode;
import org.apache.flink.optimizer.plan.NamedChannel;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.plan.SolutionSetPlanNode;
import org.apache.flink.optimizer.plan.SourcePlanNode;
import org.apache.flink.optimizer.plan.WorksetIterationPlanNode;
import org.apache.flink.optimizer.plan.WorksetPlanNode;
import org.apache.flink.optimizer.util.Utils;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.iterative.convergence.WorksetEmptyConvergenceCriterion;
import org.apache.flink.runtime.iterative.task.IterationHeadTask;
import org.apache.flink.runtime.iterative.task.IterationIntermediateTask;
import org.apache.flink.runtime.iterative.task.IterationSynchronizationSinkTask;
import org.apache.flink.runtime.iterative.task.IterationTailTask;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.InputOutputFormatContainer;
import org.apache.flink.runtime.jobgraph.InputOutputFormatVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.operators.CoGroupDriver;
import org.apache.flink.runtime.operators.CoGroupWithSolutionSetFirstDriver;
import org.apache.flink.runtime.operators.CoGroupWithSolutionSetSecondDriver;
import org.apache.flink.runtime.operators.DataSinkTask;
import org.apache.flink.runtime.operators.DataSourceTask;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.JoinDriver;
import org.apache.flink.runtime.operators.JoinWithSolutionSetFirstDriver;
import org.apache.flink.runtime.operators.JoinWithSolutionSetSecondDriver;
import org.apache.flink.runtime.operators.NoOpDriver;
import org.apache.flink.runtime.operators.chaining.ChainedDriver;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.Visitor;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * This component translates the optimizer's resulting {@link org.apache.flink.optimizer.plan.OptimizedPlan}
 * to a {@link org.apache.flink.runtime.jobgraph.JobGraph}. The translation is not strictly a one-to-one,
 * because some nodes from the OptimizedPlan are collapsed into one job vertex.
 *
 * This translation does not make any decisions or assumptions. All degrees-of-freedom in the execution
 * of the job are made by the Optimizer, so that this translation becomes a deterministic mapping.
 *
 * The basic method of operation is a top down traversal over the plan graph. On the way down, job vertices
 * are created for the plan nodes, on the way back up, the nodes connect their predecessors.
 */
public class JobGraphGenerator implements Visitor<PlanNode> {
	
	private static final Logger LOG = LoggerFactory.getLogger(JobGraphGenerator.class);
	
	public static final String MERGE_ITERATION_AUX_TASKS_KEY = "compiler.merge-iteration-aux";
	
	private static final boolean mergeIterationAuxTasks =
		GlobalConfiguration.loadConfiguration().getBoolean(MERGE_ITERATION_AUX_TASKS_KEY, false);

	private static final TaskInChain ALREADY_VISITED_PLACEHOLDER = new TaskInChain(null, null, null, null);

	// ------------------------------------------------------------------------

	private Map<PlanNode, JobVertex> vertices; // a map from optimizer nodes to job vertices
	
	private Map<PlanNode, TaskInChain> chainedTasks; // a map from optimizer nodes to job vertices
	
	private Map<IterationPlanNode, IterationDescriptor> iterations;
	
	private List<TaskInChain> chainedTasksInSequence;
	
	private List<JobVertex> auxVertices; // auxiliary vertices which are added during job graph generation
	
	private final int defaultMaxFan;
	
	private final float defaultSortSpillingThreshold;

	private final boolean useLargeRecordHandler;
	
	private int iterationIdEnumerator = 1;
	
	private IterationPlanNode currentIteration; // the current the enclosing iteration
	
	private List<IterationPlanNode> iterationStack;  // stack of enclosing iterations
	
	private SlotSharingGroup sharingGroup;
	
	// ------------------------------------------------------------------------

	/**
	 * Creates a new job graph generator that uses the default values for its resource configuration.
	 */
	public JobGraphGenerator() {
		this.defaultMaxFan = AlgorithmOptions.SPILLING_MAX_FAN.defaultValue();
		this.defaultSortSpillingThreshold = AlgorithmOptions.SORT_SPILLING_THRESHOLD.defaultValue();
		this.useLargeRecordHandler = ConfigConstants.DEFAULT_USE_LARGE_RECORD_HANDLER;
	}
	
	public JobGraphGenerator(Configuration config) {
		this.defaultMaxFan = config.getInteger(AlgorithmOptions.SPILLING_MAX_FAN);
		this.defaultSortSpillingThreshold = config.getFloat(AlgorithmOptions.SORT_SPILLING_THRESHOLD);
		this.useLargeRecordHandler = config.getBoolean(
				ConfigConstants.USE_LARGE_RECORD_HANDLER_KEY,
				ConfigConstants.DEFAULT_USE_LARGE_RECORD_HANDLER);
	}

	/**
	 * Translates a {@link org.apache.flink.optimizer.plan.OptimizedPlan} into a
	 * {@link org.apache.flink.runtime.jobgraph.JobGraph}.
	 * 
	 * @param program Optimized plan that is translated into a JobGraph.
	 * @return JobGraph generated from the plan.
	 */
	public JobGraph compileJobGraph(OptimizedPlan program) {
		return compileJobGraph(program, null);
	}
	
	public JobGraph compileJobGraph(OptimizedPlan program, JobID jobId) {
		if (program == null) {
			throw new NullPointerException("Program is null, did you called " +
				"ExecutionEnvironment.execute()");
		}
		
		if (jobId == null) {
			jobId = JobID.generate();
		}

		this.vertices = new HashMap<PlanNode, JobVertex>();
		this.chainedTasks = new HashMap<PlanNode, TaskInChain>();
		this.chainedTasksInSequence = new ArrayList<TaskInChain>();
		this.auxVertices = new ArrayList<JobVertex>();
		this.iterations = new HashMap<IterationPlanNode, IterationDescriptor>();
		this.iterationStack = new ArrayList<IterationPlanNode>();
		
		this.sharingGroup = new SlotSharingGroup();
		
		// this starts the traversal that generates the job graph
		program.accept(this);
		
		// sanity check that we are not somehow in an iteration at the end
		if (this.currentIteration != null) {
			throw new CompilerException("The graph translation ended prematurely, leaving an unclosed iteration.");
		}
		
		// finalize the iterations
		for (IterationDescriptor iteration : this.iterations.values()) {
			if (iteration.getIterationNode() instanceof BulkIterationPlanNode) {
				finalizeBulkIteration(iteration);
			} else if (iteration.getIterationNode() instanceof WorksetIterationPlanNode) {
				finalizeWorksetIteration(iteration);
			} else {
				throw new CompilerException();
			}
		}
		
		// now that the traversal is done, we have the chained tasks write their configs into their
		// parents' configurations
		for (TaskInChain tic : this.chainedTasksInSequence) {
			TaskConfig t = new TaskConfig(tic.getContainingVertex().getConfiguration());
			t.addChainedTask(tic.getChainedTask(), tic.getTaskConfig(), tic.getTaskName());
		}
		
		// ----- attach the additional info to the job vertices, for display in the runtime monitor
		
		attachOperatorNamesAndDescriptions();

		// ----------- finalize the job graph -----------

		// create the job graph object
		JobGraph graph = new JobGraph(jobId, program.getJobName());
		try {
			graph.setExecutionConfig(program.getOriginalPlan().getExecutionConfig());
		}
		catch (IOException e) {
			throw new CompilerException("Could not serialize the ExecutionConfig." +
					"This indicates that non-serializable types (like custom serializers) were registered");
		}

		graph.setAllowQueuedScheduling(false);
		graph.setSessionTimeout(program.getOriginalPlan().getSessionTimeout());

		// add vertices to the graph
		for (JobVertex vertex : this.vertices.values()) {
			vertex.setInputDependencyConstraint(program.getOriginalPlan().getExecutionConfig().getDefaultInputDependencyConstraint());
			graph.addVertex(vertex);
		}

		for (JobVertex vertex : this.auxVertices) {
			graph.addVertex(vertex);
			vertex.setSlotSharingGroup(sharingGroup);
		}


		Collection<Tuple2<String, DistributedCache.DistributedCacheEntry>> userArtifacts =
			program.getOriginalPlan().getCachedFiles().stream()
			.map(entry -> Tuple2.of(entry.getKey(), entry.getValue()))
			.collect(Collectors.toList());
		addUserArtifactEntries(userArtifacts, graph);
		
		// release all references again
		this.vertices = null;
		this.chainedTasks = null;
		this.chainedTasksInSequence = null;
		this.auxVertices = null;
		this.iterations = null;
		this.iterationStack = null;

		// return job graph
		return graph;
	}

	public static void addUserArtifactEntries(Collection<Tuple2<String, DistributedCache.DistributedCacheEntry>> userArtifacts, JobGraph jobGraph) {
		if (userArtifacts != null && !userArtifacts.isEmpty()) {
			try {
				java.nio.file.Path tmpDir = Files.createTempDirectory("flink-distributed-cache-" + jobGraph.getJobID());
				for (Tuple2<String, DistributedCache.DistributedCacheEntry> originalEntry : userArtifacts) {
					Path filePath = new Path(originalEntry.f1.filePath);
					boolean isLocalDir = false;
					try {
						FileSystem sourceFs = filePath.getFileSystem();
						isLocalDir = !sourceFs.isDistributedFS() && sourceFs.getFileStatus(filePath).isDir();
					} catch (IOException ioe) {
						LOG.warn("Could not determine whether {} denotes a local path.", filePath, ioe);
					}
					// zip local directories because we only support file uploads
					DistributedCache.DistributedCacheEntry entry;
					if (isLocalDir) {
						Path zip = FileUtils.compressDirectory(filePath, new Path(tmpDir.toString(), filePath.getName() + ".zip"));
						entry = new DistributedCache.DistributedCacheEntry(zip.toString(), originalEntry.f1.isExecutable, true);
					} else {
						entry = new DistributedCache.DistributedCacheEntry(filePath.toString(), originalEntry.f1.isExecutable, false);
					}
					jobGraph.addUserArtifact(originalEntry.f0, entry);
				}
			} catch (IOException ioe) {
				throw new FlinkRuntimeException("Could not compress distributed-cache artifacts.", ioe);
			}
		}
	}

	/**
	 * This methods implements the pre-visiting during a depth-first traversal. It create the job vertex and
	 * sets local strategy.
	 * 
	 * @param node
	 *        The node that is currently processed.
	 * @return True, if the visitor should descend to the node's children, false if not.
	 * @see org.apache.flink.util.Visitor#preVisit(org.apache.flink.util.Visitable)
	 */
	@Override
	public boolean preVisit(PlanNode node) {
		// check if we have visited this node before. in non-tree graphs, this happens
		if (this.vertices.containsKey(node) || this.chainedTasks.containsKey(node) || this.iterations.containsKey(node)) {
			// return false to prevent further descend
			return false;
		}

		// the vertex to be created for the current node
		final JobVertex vertex;
		try {
			if (node instanceof SinkPlanNode) {
				vertex = createDataSinkVertex((SinkPlanNode) node);
			}
			else if (node instanceof SourcePlanNode) {
				vertex = createDataSourceVertex((SourcePlanNode) node);
			}
			else if (node instanceof BulkIterationPlanNode) {
				BulkIterationPlanNode iterationNode = (BulkIterationPlanNode) node;
				// for the bulk iteration, we skip creating anything for now. we create the graph
				// for the step function in the post visit.
				
				// check that the root of the step function has the same parallelism as the iteration.
				// because the tail must have the same parallelism as the head, we can only merge the last
				// operator with the tail, if they have the same parallelism. not merging is currently not
				// implemented
				PlanNode root = iterationNode.getRootOfStepFunction();
				if (root.getParallelism() != node.getParallelism())
				{
					throw new CompilerException("Error: The final operator of the step " +
							"function has a different parallelism than the iteration operator itself.");
				}
				
				IterationDescriptor descr = new IterationDescriptor(iterationNode, this.iterationIdEnumerator++);
				this.iterations.put(iterationNode, descr);
				vertex = null;
			}
			else if (node instanceof WorksetIterationPlanNode) {
				WorksetIterationPlanNode iterationNode = (WorksetIterationPlanNode) node;

				// we have the same constraints as for the bulk iteration
				PlanNode nextWorkSet = iterationNode.getNextWorkSetPlanNode();
				PlanNode solutionSetDelta  = iterationNode.getSolutionSetDeltaPlanNode();
				
				if (nextWorkSet.getParallelism() != node.getParallelism())
				{
					throw new CompilerException("It is currently not supported that the final operator of the step " +
							"function has a different parallelism than the iteration operator itself.");
				}
				if (solutionSetDelta.getParallelism() != node.getParallelism())
				{
					throw new CompilerException("It is currently not supported that the final operator of the step " +
							"function has a different parallelism than the iteration operator itself.");
				}
				
				IterationDescriptor descr = new IterationDescriptor(iterationNode, this.iterationIdEnumerator++);
				this.iterations.put(iterationNode, descr);
				vertex = null;
			}
			else if (node instanceof SingleInputPlanNode) {
				vertex = createSingleInputVertex((SingleInputPlanNode) node);
			}
			else if (node instanceof DualInputPlanNode) {
				vertex = createDualInputVertex((DualInputPlanNode) node);
			}
			else if (node instanceof NAryUnionPlanNode) {
				// skip the union for now
				vertex = null;
			}
			else if (node instanceof BulkPartialSolutionPlanNode) {
				// create a head node (or not, if it is merged into its successor)
				vertex = createBulkIterationHead((BulkPartialSolutionPlanNode) node);
			}
			else if (node instanceof SolutionSetPlanNode) {
				// this represents an access into the solution set index.
				// we do not create a vertex for the solution set here (we create the head at the workset place holder)
				
				// we adjust the joins / cogroups that go into the solution set here
				for (Channel c : node.getOutgoingChannels()) {
					DualInputPlanNode target = (DualInputPlanNode) c.getTarget();
					JobVertex accessingVertex = this.vertices.get(target);
					TaskConfig conf = new TaskConfig(accessingVertex.getConfiguration());
					int inputNum = c == target.getInput1() ? 0 : c == target.getInput2() ? 1 : -1;
					
					// sanity checks
					if (inputNum == -1) {
						throw new CompilerException();
					}
					
					// adjust the driver
					if (conf.getDriver().equals(JoinDriver.class)) {
						conf.setDriver(inputNum == 0 ? JoinWithSolutionSetFirstDriver.class : JoinWithSolutionSetSecondDriver.class);
					}
					else if (conf.getDriver().equals(CoGroupDriver.class)) {
						conf.setDriver(inputNum == 0 ? CoGroupWithSolutionSetFirstDriver.class : CoGroupWithSolutionSetSecondDriver.class);
					}
					else {
						throw new CompilerException("Found join with solution set using incompatible operator (only Join/CoGroup are valid).");
					}
				}
				
				// make sure we do not visit this node again. for that, we add a 'already seen' entry into one of the sets
				this.chainedTasks.put(node, ALREADY_VISITED_PLACEHOLDER);
				
				vertex = null;
			}
			else if (node instanceof WorksetPlanNode) {
				// create the iteration head here
				vertex = createWorksetIterationHead((WorksetPlanNode) node);
			}
			else {
				throw new CompilerException("Unrecognized node type: " + node.getClass().getName());
			}
		}
		catch (Exception e) {
			throw new CompilerException("Error translating node '" + node + "': " + e.getMessage(), e);
		}
		
		// check if a vertex was created, or if it was chained or skipped
		if (vertex != null) {
			// set parallelism
			int pd = node.getParallelism();
			vertex.setParallelism(pd);
			vertex.setMaxParallelism(pd);
			
			vertex.setSlotSharingGroup(sharingGroup);
			
			// check whether this vertex is part of an iteration step function
			if (this.currentIteration != null) {
				// check that the task has the same parallelism as the iteration as such
				PlanNode iterationNode = (PlanNode) this.currentIteration;
				if (iterationNode.getParallelism() < pd) {
					throw new CompilerException("Error: All functions that are part of an iteration must have the same, or a lower, parallelism than the iteration operator.");
				}

				// store the id of the iterations the step functions participate in
				IterationDescriptor descr = this.iterations.get(this.currentIteration);
				new TaskConfig(vertex.getConfiguration()).setIterationId(descr.getId());
			}
	
			// store in the map
			this.vertices.put(node, vertex);
		}

		// returning true causes deeper descend
		return true;
	}

	/**
	 * This method implements the post-visit during the depth-first traversal. When the post visit happens,
	 * all of the descendants have been processed, so this method connects all of the current node's
	 * predecessors to the current node.
	 * 
	 * @param node
	 *        The node currently processed during the post-visit.
	 * @see org.apache.flink.util.Visitor#postVisit(org.apache.flink.util.Visitable) t
	 */
	@Override
	public void postVisit(PlanNode node) {
		try {
			// --------- check special cases for which we handle post visit differently ----------
			
			// skip data source node (they have no inputs)
			// also, do nothing for union nodes, we connect them later when gathering the inputs for a task
			// solution sets have no input. the initial solution set input is connected when the iteration node is in its postVisit
			if (node instanceof SourcePlanNode || node instanceof NAryUnionPlanNode || node instanceof SolutionSetPlanNode) {
				return;
			}

			// if this is a blocking shuffle vertex, we add one IntermediateDataSetID to its predecessor and return
			if (checkAndConfigurePersistentIntermediateResult(node)) {
				return;
			}

			// check if we have an iteration. in that case, translate the step function now
			if (node instanceof IterationPlanNode) {
				// prevent nested iterations
				if (node.isOnDynamicPath()) {
					throw new CompilerException("Nested Iterations are not possible at the moment!");
				}
				
				// if we recursively go into an iteration (because the constant path of one iteration contains
				// another one), we push the current one onto the stack
				if (this.currentIteration != null) {
					this.iterationStack.add(this.currentIteration);
				}
				
				this.currentIteration = (IterationPlanNode) node;
				this.currentIteration.acceptForStepFunction(this);
				
				// pop the current iteration from the stack
				if (this.iterationStack.isEmpty()) {
					this.currentIteration = null;
				} else {
					this.currentIteration = this.iterationStack.remove(this.iterationStack.size() - 1);
				}
				
				// inputs for initial bulk partial solution or initial workset are already connected to the iteration head in the head's post visit.
				// connect the initial solution set now.
				if (node instanceof WorksetIterationPlanNode) {
					// connect the initial solution set
					WorksetIterationPlanNode wsNode = (WorksetIterationPlanNode) node;
					JobVertex headVertex = this.iterations.get(wsNode).getHeadTask();
					TaskConfig headConfig = new TaskConfig(headVertex.getConfiguration());
					int inputIndex = headConfig.getDriverStrategy().getNumInputs();
					headConfig.setIterationHeadSolutionSetInputIndex(inputIndex);
					translateChannel(wsNode.getInitialSolutionSetInput(), inputIndex, headVertex, headConfig, false);
				}
				
				return;
			}
			
			final JobVertex targetVertex = this.vertices.get(node);
			
			
			// --------- Main Path: Translation of channels ----------
			// 
			// There are two paths of translation: One for chained tasks (or merged tasks in general),
			// which do not have their own task vertex. The other for tasks that have their own vertex,
			// or are the primary task in a vertex (to which the others are chained).
			
			// check whether this node has its own task, or is merged with another one
			if (targetVertex == null) {
				// node's task is merged with another task. it is either chained, of a merged head vertex
				// from an iteration
				final TaskInChain chainedTask;
				if ((chainedTask = this.chainedTasks.get(node)) != null) {
					// Chained Task. Sanity check first...
					final Iterator<Channel> inConns = node.getInputs().iterator();
					if (!inConns.hasNext()) {
						throw new CompilerException("Bug: Found chained task with no input.");
					}
					final Channel inConn = inConns.next();
					
					if (inConns.hasNext()) {
						throw new CompilerException("Bug: Found a chained task with more than one input!");
					}
					if (inConn.getLocalStrategy() != null && inConn.getLocalStrategy() != LocalStrategy.NONE) {
						throw new CompilerException("Bug: Found a chained task with an input local strategy.");
					}
					if (inConn.getShipStrategy() != null && inConn.getShipStrategy() != ShipStrategyType.FORWARD) {
						throw new CompilerException("Bug: Found a chained task with an input ship strategy other than FORWARD.");
					}
	
					JobVertex container = chainedTask.getContainingVertex();
					
					if (container == null) {
						final PlanNode sourceNode = inConn.getSource();
						container = this.vertices.get(sourceNode);
						if (container == null) {
							// predecessor is itself chained
							container = this.chainedTasks.get(sourceNode).getContainingVertex();
							if (container == null) {
								throw new IllegalStateException("Bug: Chained task predecessor has not been assigned its containing vertex.");
							}
						} else {
							// predecessor is a proper task job vertex and this is the first chained task. add a forward connection entry.
							new TaskConfig(container.getConfiguration()).addOutputShipStrategy(ShipStrategyType.FORWARD);
						}
						chainedTask.setContainingVertex(container);
					}
					
					// add info about the input serializer type
					chainedTask.getTaskConfig().setInputSerializer(inConn.getSerializer(), 0);
					
					// update name of container task
					String containerTaskName = container.getName();
					if (containerTaskName.startsWith("CHAIN ")) {
						container.setName(containerTaskName + " -> " + chainedTask.getTaskName());
					} else {
						container.setName("CHAIN " + containerTaskName + " -> " + chainedTask.getTaskName());
					}

					//update resource of container task
					container.setResources(container.getMinResources().merge(node.getMinResources()),
							container.getPreferredResources().merge(node.getPreferredResources()));
					
					this.chainedTasksInSequence.add(chainedTask);
					return;
				}
				else if (node instanceof BulkPartialSolutionPlanNode ||
						node instanceof WorksetPlanNode)
				{
					// merged iteration head task. the task that the head is merged with will take care of it
					return;
				} else {
					throw new CompilerException("Bug: Unrecognized merged task vertex.");
				}
			}
			
			// -------- Here, we translate non-chained tasks -------------
			
			
			if (this.currentIteration != null) {
				JobVertex head = this.iterations.get(this.currentIteration).getHeadTask();
				// Exclude static code paths from the co-location constraint, because otherwise
				// their execution determines the deployment slots of the co-location group
				if (node.isOnDynamicPath()) {
					targetVertex.setStrictlyCoLocatedWith(head);
				}
			}
			
			
			// create the config that will contain all the description of the inputs
			final TaskConfig targetVertexConfig = new TaskConfig(targetVertex.getConfiguration());
						
			// get the inputs. if this node is the head of an iteration, we obtain the inputs from the
			// enclosing iteration node, because the inputs are the initial inputs to the iteration.
			final Iterator<Channel> inConns;
			if (node instanceof BulkPartialSolutionPlanNode) {
				inConns = ((BulkPartialSolutionPlanNode) node).getContainingIterationNode().getInputs().iterator();
				// because the partial solution has its own vertex, is has only one (logical) input.
				// note this in the task configuration
				targetVertexConfig.setIterationHeadPartialSolutionOrWorksetInputIndex(0);
			} else if (node instanceof WorksetPlanNode) {
				WorksetPlanNode wspn = (WorksetPlanNode) node;
				// input that is the initial workset
				inConns = Collections.singleton(wspn.getContainingIterationNode().getInput2()).iterator();
				
				// because we have a stand-alone (non-merged) workset iteration head, the initial workset will
				// be input 0 and the solution set will be input 1
				targetVertexConfig.setIterationHeadPartialSolutionOrWorksetInputIndex(0);
				targetVertexConfig.setIterationHeadSolutionSetInputIndex(1);
			} else {
				inConns = node.getInputs().iterator();
			}
			if (!inConns.hasNext()) {
				throw new CompilerException("Bug: Found a non-source task with no input.");
			}
			
			int inputIndex = 0;
			while (inConns.hasNext()) {
				Channel input = inConns.next();
				inputIndex += translateChannel(input, inputIndex, targetVertex, targetVertexConfig, false);
			}
			// broadcast variables
			int broadcastInputIndex = 0;
			for (NamedChannel broadcastInput: node.getBroadcastInputs()) {
				int broadcastInputIndexDelta = translateChannel(broadcastInput, broadcastInputIndex, targetVertex, targetVertexConfig, true);
				targetVertexConfig.setBroadcastInputName(broadcastInput.getName(), broadcastInputIndex);
				targetVertexConfig.setBroadcastInputSerializer(broadcastInput.getSerializer(), broadcastInputIndex);
				broadcastInputIndex += broadcastInputIndexDelta;
			}
		} catch (Exception e) {
			throw new CompilerException(
				"An error occurred while translating the optimized plan to a JobGraph: " + e.getMessage(), e);
		}
	}
	
	private int translateChannel(Channel input, int inputIndex, JobVertex targetVertex,
			TaskConfig targetVertexConfig, boolean isBroadcast) throws Exception
	{
		final PlanNode inputPlanNode = input.getSource();
		final Iterator<Channel> allInChannels;
		
		if (inputPlanNode instanceof NAryUnionPlanNode) {
			allInChannels = ((NAryUnionPlanNode) inputPlanNode).getListOfInputs().iterator();

			// If the union node has a batch data exchange, we have to adopt the exchange mode of
			// the inputs of the union as well, because the optimizer has a separate union
			// node, which does not exist in the JobGraph. Otherwise, this can result in
			// deadlocks when closing a branching flow at runtime.
			for (Channel in : inputPlanNode.getInputs()) {
				if (input.getDataExchangeMode().equals(DataExchangeMode.BATCH)) {
					in.setDataExchangeMode(DataExchangeMode.BATCH);
				}
				if (isBroadcast) {
					in.setShipStrategy(ShipStrategyType.BROADCAST, in.getDataExchangeMode());
				}
			}

			// The outgoing connection of an NAryUnion must be a forward connection.
			if (input.getShipStrategy() != ShipStrategyType.FORWARD && !isBroadcast) {
				throw new CompilerException("Optimized plan contains Union with non-forward outgoing ship strategy.");
			}

		}
		else if (inputPlanNode instanceof BulkPartialSolutionPlanNode) {
			if (this.vertices.get(inputPlanNode) == null) {
				// merged iteration head
				final BulkPartialSolutionPlanNode pspn = (BulkPartialSolutionPlanNode) inputPlanNode;
				final BulkIterationPlanNode iterationNode = pspn.getContainingIterationNode();
				
				// check if the iteration's input is a union
				if (iterationNode.getInput().getSource() instanceof NAryUnionPlanNode) {
					allInChannels = (iterationNode.getInput().getSource()).getInputs().iterator();
				} else {
					allInChannels = Collections.singletonList(iterationNode.getInput()).iterator();
				}
				
				// also, set the index of the gate with the partial solution
				targetVertexConfig.setIterationHeadPartialSolutionOrWorksetInputIndex(inputIndex);
			} else {
				// standalone iteration head
				allInChannels = Collections.singletonList(input).iterator();
			}
		} else if (inputPlanNode instanceof WorksetPlanNode) {
			if (this.vertices.get(inputPlanNode) == null) {
				// merged iteration head
				final WorksetPlanNode wspn = (WorksetPlanNode) inputPlanNode;
				final WorksetIterationPlanNode iterationNode = wspn.getContainingIterationNode();
				
				// check if the iteration's input is a union
				if (iterationNode.getInput2().getSource() instanceof NAryUnionPlanNode) {
					allInChannels = (iterationNode.getInput2().getSource()).getInputs().iterator();
				} else {
					allInChannels = Collections.singletonList(iterationNode.getInput2()).iterator();
				}
				
				// also, set the index of the gate with the partial solution
				targetVertexConfig.setIterationHeadPartialSolutionOrWorksetInputIndex(inputIndex);
			} else {
				// standalone iteration head
				allInChannels = Collections.singletonList(input).iterator();
			}
		} else if (inputPlanNode instanceof SolutionSetPlanNode) {
			// for now, skip connections with the solution set node, as this is a local index access (later to be parameterized here)
			// rather than a vertex connection
			return 0;
		} else {
			allInChannels = Collections.singletonList(input).iterator();
		}
		
		// check that the type serializer is consistent
		TypeSerializerFactory<?> typeSerFact = null;
		
		// accounting for channels on the dynamic path
		int numChannelsTotal = 0;
		int numChannelsDynamicPath = 0;
		int numDynamicSenderTasksTotal = 0;
		

		// expand the channel to all the union channels, in case there is a union operator at its source
		while (allInChannels.hasNext()) {
			final Channel inConn = allInChannels.next();
			
			// sanity check the common serializer
			if (typeSerFact == null) {
				typeSerFact = inConn.getSerializer();
			} else if (!typeSerFact.equals(inConn.getSerializer())) {
				throw new CompilerException("Conflicting types in union operator.");
			}
			
			final PlanNode sourceNode = inConn.getSource();
			JobVertex sourceVertex = this.vertices.get(sourceNode);
			TaskConfig sourceVertexConfig;

			if (sourceVertex == null) {
				// this predecessor is chained to another task or an iteration
				final TaskInChain chainedTask;
				final IterationDescriptor iteration;
				if ((chainedTask = this.chainedTasks.get(sourceNode)) != null) {
					// push chained task
					if (chainedTask.getContainingVertex() == null) {
						throw new IllegalStateException("Bug: Chained task has not been assigned its containing vertex when connecting.");
					}
					sourceVertex = chainedTask.getContainingVertex();
					sourceVertexConfig = chainedTask.getTaskConfig();
				} else if ((iteration = this.iterations.get(sourceNode)) != null) {
					// predecessor is an iteration
					sourceVertex = iteration.getHeadTask();
					sourceVertexConfig = iteration.getHeadFinalResultConfig();
				} else {
					throw new CompilerException("Bug: Could not resolve source node for a channel.");
				}
			} else {
				// predecessor is its own vertex
				sourceVertexConfig = new TaskConfig(sourceVertex.getConfiguration());
			}
			DistributionPattern pattern = connectJobVertices(
				inConn, inputIndex, sourceVertex, sourceVertexConfig, targetVertex, targetVertexConfig, isBroadcast);
			
			// accounting on channels and senders
			numChannelsTotal++;
			if (inConn.isOnDynamicPath()) {
				numChannelsDynamicPath++;
				numDynamicSenderTasksTotal += getNumberOfSendersPerReceiver(pattern,
					sourceVertex.getParallelism(), targetVertex.getParallelism());
			}
		}
		
		// for the iterations, check that the number of dynamic channels is the same as the number
		// of channels for this logical input. this condition is violated at the moment, if there
		// is a union between nodes on the static and nodes on the dynamic path
		if (numChannelsDynamicPath > 0 && numChannelsTotal != numChannelsDynamicPath) {
			throw new CompilerException("Error: It is currently not supported to union between dynamic and static path in an iteration.");
		}
		if (numDynamicSenderTasksTotal > 0) {
			if (isBroadcast) {
				targetVertexConfig.setBroadcastGateIterativeWithNumberOfEventsUntilInterrupt(inputIndex, numDynamicSenderTasksTotal);
			} else {
				targetVertexConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(inputIndex, numDynamicSenderTasksTotal);
			}
		}
		
		// the local strategy is added only once. in non-union case that is the actual edge,
		// in the union case, it is the edge between union and the target node
		addLocalInfoFromChannelToConfig(input, targetVertexConfig, inputIndex, isBroadcast);
		return 1;
	}
	
	private int getNumberOfSendersPerReceiver(DistributionPattern pattern, int numSenders, int numReceivers) {
		if (pattern == DistributionPattern.ALL_TO_ALL) {
			return numSenders;
		} else if (pattern == DistributionPattern.POINTWISE) {
			if (numSenders != numReceivers) {
				if (numReceivers == 1) {
					return numSenders;
				}
				else if (numSenders == 1) {
					return 1;
				}
				else {
					throw new CompilerException("Error: A changing parallelism is currently " +
							"not supported between tasks within an iteration.");
				}
			} else {
				return 1;
			}
		} else {
			throw new CompilerException("Unknown distribution pattern for channels: " + pattern);
		}
	}
	
	// ------------------------------------------------------------------------
	// Methods for creating individual vertices
	// ------------------------------------------------------------------------
	
	private JobVertex createSingleInputVertex(SingleInputPlanNode node) throws CompilerException {
		final String taskName = node.getNodeName();
		final DriverStrategy ds = node.getDriverStrategy();
		
		// check, whether chaining is possible
		boolean chaining;
		{
			Channel inConn = node.getInput();
			PlanNode pred = inConn.getSource();
			chaining = ds.getPushChainDriverClass() != null &&
					!(pred instanceof NAryUnionPlanNode) &&	// first op after union is stand-alone, because union is merged
					!(pred instanceof BulkPartialSolutionPlanNode) &&	// partial solution merges anyways
					!(pred instanceof WorksetPlanNode) &&	// workset merges anyways
					!(pred instanceof IterationPlanNode) && // cannot chain with iteration heads currently
					inConn.getShipStrategy() == ShipStrategyType.FORWARD &&
					inConn.getLocalStrategy() == LocalStrategy.NONE &&
					pred.getOutgoingChannels().size() == 1 &&
					node.getParallelism() == pred.getParallelism() &&
					node.getBroadcastInputs().isEmpty();
			
			// cannot chain the nodes that produce the next workset or the next solution set, if they are not the
			// in a tail 
			if (this.currentIteration instanceof WorksetIterationPlanNode && node.getOutgoingChannels().size() > 0)
			{
				WorksetIterationPlanNode wspn = (WorksetIterationPlanNode) this.currentIteration;
				if (wspn.getSolutionSetDeltaPlanNode() == pred || wspn.getNextWorkSetPlanNode() == pred) {
					chaining = false;
				}
			}
			// cannot chain the nodes that produce the next workset in a bulk iteration if a termination criterion follows
			if (this.currentIteration instanceof BulkIterationPlanNode)
			{
				BulkIterationPlanNode wspn = (BulkIterationPlanNode) this.currentIteration;
				if (node == wspn.getRootOfTerminationCriterion() && wspn.getRootOfStepFunction() == pred){
					chaining = false;
				}else if(node.getOutgoingChannels().size() > 0 &&(wspn.getRootOfStepFunction() == pred ||
						wspn.getRootOfTerminationCriterion() == pred)) {
					chaining = false;
				}
			}
		}
		
		final JobVertex vertex;
		final TaskConfig config;
		
		if (chaining) {
			vertex = null;
			config = new TaskConfig(new Configuration());
			this.chainedTasks.put(node, new TaskInChain(node, ds.getPushChainDriverClass(), config, taskName));
		} else {
			// create task vertex
			vertex = new JobVertex(taskName);
			vertex.setResources(node.getMinResources(), node.getPreferredResources());
			vertex.setInvokableClass((this.currentIteration != null && node.isOnDynamicPath()) ? IterationIntermediateTask.class : BatchTask.class);
			
			config = new TaskConfig(vertex.getConfiguration());
			config.setDriver(ds.getDriverClass());
		}
		
		// set user code
		config.setStubWrapper(node.getProgramOperator().getUserCodeWrapper());
		config.setStubParameters(node.getProgramOperator().getParameters());
		
		// set the driver strategy
		config.setDriverStrategy(ds);
		for (int i = 0; i < ds.getNumRequiredComparators(); i++) {
			config.setDriverComparator(node.getComparator(i), i);
		}
		// assign memory, file-handles, etc.
		assignDriverResources(node, config);
		return vertex;
	}

	private JobVertex createDualInputVertex(DualInputPlanNode node) throws CompilerException {
		final String taskName = node.getNodeName();
		final DriverStrategy ds = node.getDriverStrategy();
		final JobVertex vertex = new JobVertex(taskName);
		final TaskConfig config = new TaskConfig(vertex.getConfiguration());
		vertex.setResources(node.getMinResources(), node.getPreferredResources());
		vertex.setInvokableClass( (this.currentIteration != null && node.isOnDynamicPath()) ? IterationIntermediateTask.class : BatchTask.class);
		
		// set user code
		config.setStubWrapper(node.getProgramOperator().getUserCodeWrapper());
		config.setStubParameters(node.getProgramOperator().getParameters());
		
		// set the driver strategy
		config.setDriver(ds.getDriverClass());
		config.setDriverStrategy(ds);
		if (node.getComparator1() != null) {
			config.setDriverComparator(node.getComparator1(), 0);
		}
		if (node.getComparator2() != null) {
			config.setDriverComparator(node.getComparator2(), 1);
		}
		if (node.getPairComparator() != null) {
			config.setDriverPairComparator(node.getPairComparator());
		}
		
		// assign memory, file-handles, etc.
		assignDriverResources(node, config);
		return vertex;
	}

	private JobVertex createDataSourceVertex(SourcePlanNode node) throws CompilerException {
		final InputOutputFormatVertex vertex = new InputOutputFormatVertex(node.getNodeName());
		final TaskConfig config = new TaskConfig(vertex.getConfiguration());

		final OperatorID operatorID = new OperatorID();

		vertex.setResources(node.getMinResources(), node.getPreferredResources());
		vertex.setInvokableClass(DataSourceTask.class);
		vertex.setFormatDescription(operatorID, getDescriptionForUserCode(node.getProgramOperator().getUserCodeWrapper()));

		// set user code
		new InputOutputFormatContainer(Thread.currentThread().getContextClassLoader())
			.addInputFormat(operatorID, (UserCodeWrapper<? extends InputFormat<?, ?>>) node.getProgramOperator().getUserCodeWrapper())
			.addParameters(operatorID, node.getProgramOperator().getParameters())
			.write(config);

		config.setOutputSerializer(node.getSerializer());
		return vertex;
	}

	private JobVertex createDataSinkVertex(SinkPlanNode node) throws CompilerException {
		final InputOutputFormatVertex vertex = new InputOutputFormatVertex(node.getNodeName());
		final TaskConfig config = new TaskConfig(vertex.getConfiguration());

		final OperatorID operatorID = new OperatorID();

		vertex.setResources(node.getMinResources(), node.getPreferredResources());
		vertex.setInvokableClass(DataSinkTask.class);
		vertex.setFormatDescription(operatorID, getDescriptionForUserCode(node.getProgramOperator().getUserCodeWrapper()));

		// set user code
		new InputOutputFormatContainer(Thread.currentThread().getContextClassLoader())
			.addOutputFormat(operatorID, (UserCodeWrapper<? extends OutputFormat<?>>) node.getProgramOperator().getUserCodeWrapper())
			.addParameters(operatorID, node.getProgramOperator().getParameters())
			.write(config);

		return vertex;
	}
	
	private JobVertex createBulkIterationHead(BulkPartialSolutionPlanNode pspn) {
		// get the bulk iteration that corresponds to this partial solution node
		final BulkIterationPlanNode iteration = pspn.getContainingIterationNode();
		
		// check whether we need an individual vertex for the partial solution, or whether we
		// attach ourselves to the vertex of the parent node. We can combine the head with a node of 
		// the step function, if
		// 1) There is one parent that the partial solution connects to via a forward pattern and no
		//    local strategy
		// 2) parallelism and the number of subtasks per instance does not change
		// 3) That successor is not a union
		// 4) That successor is not itself the last node of the step function
		// 5) There is no local strategy on the edge for the initial partial solution, as
		//    this translates to a local strategy that would only be executed in the first iteration
		
		final boolean merge;
		if (mergeIterationAuxTasks && pspn.getOutgoingChannels().size() == 1) {
			final Channel c = pspn.getOutgoingChannels().get(0);
			final PlanNode successor = c.getTarget();
			merge = c.getShipStrategy() == ShipStrategyType.FORWARD &&
					c.getLocalStrategy() == LocalStrategy.NONE &&
					c.getTempMode() == TempMode.NONE &&
					successor.getParallelism() == pspn.getParallelism() &&
					!(successor instanceof NAryUnionPlanNode) &&
					successor != iteration.getRootOfStepFunction() &&
					iteration.getInput().getLocalStrategy() == LocalStrategy.NONE;
		} else {
			merge = false;
		}
		
		// create or adopt the head vertex
		final JobVertex toReturn;
		final JobVertex headVertex;
		final TaskConfig headConfig;
		if (merge) {
			final PlanNode successor = pspn.getOutgoingChannels().get(0).getTarget();
			headVertex = this.vertices.get(successor);
			
			if (headVertex == null) {
				throw new CompilerException(
					"Bug: Trying to merge solution set with its successor, but successor has not been created.");
			}
			
			// reset the vertex type to iteration head
			headVertex.setInvokableClass(IterationHeadTask.class);
			headConfig = new TaskConfig(headVertex.getConfiguration());
			toReturn = null;
		} else {
			// instantiate the head vertex and give it a no-op driver as the driver strategy.
			// everything else happens in the post visit, after the input (the initial partial solution)
			// is connected.
			headVertex = new JobVertex("PartialSolution ("+iteration.getNodeName()+")");
			headVertex.setResources(iteration.getMinResources(), iteration.getPreferredResources());
			headVertex.setInvokableClass(IterationHeadTask.class);
			headConfig = new TaskConfig(headVertex.getConfiguration());
			headConfig.setDriver(NoOpDriver.class);
			toReturn = headVertex;
		}
		
		// create the iteration descriptor and the iteration to it
		IterationDescriptor descr = this.iterations.get(iteration);
		if (descr == null) {
			throw new CompilerException("Bug: Iteration descriptor was not created at when translating the iteration node.");
		}
		descr.setHeadTask(headVertex, headConfig);
		
		return toReturn;
	}
	
	private JobVertex createWorksetIterationHead(WorksetPlanNode wspn) {
		// get the bulk iteration that corresponds to this partial solution node
		final WorksetIterationPlanNode iteration = wspn.getContainingIterationNode();
		
		// check whether we need an individual vertex for the partial solution, or whether we
		// attach ourselves to the vertex of the parent node. We can combine the head with a node of 
		// the step function, if
		// 1) There is one parent that the partial solution connects to via a forward pattern and no
		//    local strategy
		// 2) parallelism and the number of subtasks per instance does not change
		// 3) That successor is not a union
		// 4) That successor is not itself the last node of the step function
		// 5) There is no local strategy on the edge for the initial workset, as
		//    this translates to a local strategy that would only be executed in the first superstep
		
		final boolean merge;
		if (mergeIterationAuxTasks && wspn.getOutgoingChannels().size() == 1) {
			final Channel c = wspn.getOutgoingChannels().get(0);
			final PlanNode successor = c.getTarget();
			merge = c.getShipStrategy() == ShipStrategyType.FORWARD &&
					c.getLocalStrategy() == LocalStrategy.NONE &&
					c.getTempMode() == TempMode.NONE &&
					successor.getParallelism() == wspn.getParallelism() &&
					!(successor instanceof NAryUnionPlanNode) &&
					successor != iteration.getNextWorkSetPlanNode() &&
					iteration.getInitialWorksetInput().getLocalStrategy() == LocalStrategy.NONE;
		} else {
			merge = false;
		}
		
		// create or adopt the head vertex
		final JobVertex toReturn;
		final JobVertex headVertex;
		final TaskConfig headConfig;
		if (merge) {
			final PlanNode successor = wspn.getOutgoingChannels().get(0).getTarget();
			headVertex = this.vertices.get(successor);
			
			if (headVertex == null) {
				throw new CompilerException(
					"Bug: Trying to merge solution set with its sucessor, but successor has not been created.");
			}
			
			// reset the vertex type to iteration head
			headVertex.setInvokableClass(IterationHeadTask.class);
			headConfig = new TaskConfig(headVertex.getConfiguration());
			toReturn = null;
		} else {
			// instantiate the head vertex and give it a no-op driver as the driver strategy.
			// everything else happens in the post visit, after the input (the initial partial solution)
			// is connected.
			headVertex = new JobVertex("IterationHead("+iteration.getNodeName()+")");
			headVertex.setResources(iteration.getMinResources(), iteration.getPreferredResources());
			headVertex.setInvokableClass(IterationHeadTask.class);
			headConfig = new TaskConfig(headVertex.getConfiguration());
			headConfig.setDriver(NoOpDriver.class);
			toReturn = headVertex;
		}
		
		headConfig.setSolutionSetUnmanaged(iteration.getIterationNode().getIterationContract().isSolutionSetUnManaged());
		
		// create the iteration descriptor and the iteration to it
		IterationDescriptor descr = this.iterations.get(iteration);
		if (descr == null) {
			throw new CompilerException("Bug: Iteration descriptor was not created at when translating the iteration node.");
		}
		descr.setHeadTask(headVertex, headConfig);
		
		return toReturn;
	}
	
	private void assignDriverResources(PlanNode node, TaskConfig config) {
		final double relativeMem = node.getRelativeMemoryPerSubTask();
		if (relativeMem > 0) {
			config.setRelativeMemoryDriver(relativeMem);
			config.setFilehandlesDriver(this.defaultMaxFan);
			config.setSpillingThresholdDriver(this.defaultSortSpillingThreshold);
		}
	}
	
	private void assignLocalStrategyResources(Channel c, TaskConfig config, int inputNum) {
		if (c.getRelativeMemoryLocalStrategy() > 0) {
			config.setRelativeMemoryInput(inputNum, c.getRelativeMemoryLocalStrategy());
			config.setFilehandlesInput(inputNum, this.defaultMaxFan);
			config.setSpillingThresholdInput(inputNum, this.defaultSortSpillingThreshold);
			config.setUseLargeRecordHandler(this.useLargeRecordHandler);
		}
	}

	private boolean checkAndConfigurePersistentIntermediateResult(PlanNode node) {
		if (!(node instanceof SinkPlanNode)) {
			return false;
		}

		final Object userCodeObject = node.getProgramOperator().getUserCodeWrapper().getUserCodeObject();
		if (!(userCodeObject instanceof BlockingShuffleOutputFormat)) {
			return false;
		}

		final Iterator<Channel> inputIterator = node.getInputs().iterator();
		checkState(inputIterator.hasNext(), "SinkPlanNode must have a input.");

		final PlanNode predecessorNode = inputIterator.next().getSource();
		final JobVertex predecessorVertex = (vertices.containsKey(predecessorNode)) ?
			vertices.get(predecessorNode) :
			chainedTasks.get(predecessorNode).getContainingVertex();

		checkState(predecessorVertex != null, "Bug: Chained task has not been assigned its containing vertex when connecting.");

		predecessorVertex.createAndAddResultDataSet(
				// use specified intermediateDataSetID
				new IntermediateDataSetID(((BlockingShuffleOutputFormat) userCodeObject).getIntermediateDataSetId()),
				ResultPartitionType.BLOCKING_PERSISTENT);

		// remove this node so the OutputFormatVertex will not shown in the final JobGraph.
		vertices.remove(node);
		return true;
	}

	// ------------------------------------------------------------------------
	// Connecting Vertices
	// ------------------------------------------------------------------------

	/**
	 * NOTE: The channel for global and local strategies are different if we connect a union. The global strategy
	 * channel is then the channel into the union node, the local strategy channel the one from the union to the
	 * actual target operator.
	 *
	 * @throws CompilerException
	 */
	private DistributionPattern connectJobVertices(Channel channel, int inputNumber,
			final JobVertex sourceVertex, final TaskConfig sourceConfig,
			final JobVertex targetVertex, final TaskConfig targetConfig, boolean isBroadcast)
	throws CompilerException
	{
		// ------------ connect the vertices to the job graph --------------
		final DistributionPattern distributionPattern;

		switch (channel.getShipStrategy()) {
			case FORWARD:
				distributionPattern = DistributionPattern.POINTWISE;
				break;
			case PARTITION_RANDOM:
			case BROADCAST:
			case PARTITION_HASH:
			case PARTITION_CUSTOM:
			case PARTITION_RANGE:
			case PARTITION_FORCED_REBALANCE:
				distributionPattern = DistributionPattern.ALL_TO_ALL;
				break;
			default:
				throw new RuntimeException("Unknown runtime ship strategy: " + channel.getShipStrategy());
		}

		final ResultPartitionType resultType;

		switch (channel.getDataExchangeMode()) {

			case PIPELINED:
				resultType = ResultPartitionType.PIPELINED;
				break;

			case BATCH:
				// BLOCKING results are currently not supported in closed loop iterations
				//
				// See https://issues.apache.org/jira/browse/FLINK-1713 for details
				resultType = channel.getSource().isOnDynamicPath()
						? ResultPartitionType.PIPELINED
						: ResultPartitionType.BLOCKING;
				break;

			case PIPELINE_WITH_BATCH_FALLBACK:
				throw new UnsupportedOperationException("Data exchange mode " +
						channel.getDataExchangeMode() + " currently not supported.");

			default:
				throw new UnsupportedOperationException("Unknown data exchange mode.");

		}

		JobEdge edge = targetVertex.connectNewDataSetAsInput(sourceVertex, distributionPattern, resultType);

		// -------------- configure the source task's ship strategy strategies in task config --------------
		final int outputIndex = sourceConfig.getNumOutputs();
		sourceConfig.addOutputShipStrategy(channel.getShipStrategy());
		if (outputIndex == 0) {
			sourceConfig.setOutputSerializer(channel.getSerializer());
		}
		if (channel.getShipStrategyComparator() != null) {
			sourceConfig.setOutputComparator(channel.getShipStrategyComparator(), outputIndex);
		}
		
		if (channel.getShipStrategy() == ShipStrategyType.PARTITION_RANGE) {
			
			final DataDistribution dataDistribution = channel.getDataDistribution();
			if (dataDistribution != null) {
				sourceConfig.setOutputDataDistribution(dataDistribution, outputIndex);
			} else {
				throw new RuntimeException("Range partitioning requires data distribution.");
			}
		}
		
		if (channel.getShipStrategy() == ShipStrategyType.PARTITION_CUSTOM) {
			if (channel.getPartitioner() != null) {
				sourceConfig.setOutputPartitioner(channel.getPartitioner(), outputIndex);
			} else {
				throw new CompilerException("The ship strategy was set to custom partitioning, but no partitioner was set.");
			}
		}
		
		// ---------------- configure the receiver -------------------
		if (isBroadcast) {
			targetConfig.addBroadcastInputToGroup(inputNumber);
		} else {
			targetConfig.addInputToGroup(inputNumber);
		}
		
		// ---------------- attach the additional infos to the job edge -------------------
		
		String shipStrategy = JsonMapper.getShipStrategyString(channel.getShipStrategy());
		if (channel.getShipStrategyKeys() != null && channel.getShipStrategyKeys().size() > 0) {
			shipStrategy += " on " + (channel.getShipStrategySortOrder() == null ?
					channel.getShipStrategyKeys().toString() :
					Utils.createOrdering(channel.getShipStrategyKeys(), channel.getShipStrategySortOrder()).toString());
		}
		
		String localStrategy;
		if (channel.getLocalStrategy() == null || channel.getLocalStrategy() == LocalStrategy.NONE) {
			localStrategy = null;
		}
		else {
			localStrategy = JsonMapper.getLocalStrategyString(channel.getLocalStrategy());
			if (localStrategy != null && channel.getLocalStrategyKeys() != null && channel.getLocalStrategyKeys().size() > 0) {
				localStrategy += " on " + (channel.getLocalStrategySortOrder() == null ?
						channel.getLocalStrategyKeys().toString() :
						Utils.createOrdering(channel.getLocalStrategyKeys(), channel.getLocalStrategySortOrder()).toString());
			}
		}
		
		String caching = channel.getTempMode() == TempMode.NONE ? null : channel.getTempMode().toString();

		edge.setShipStrategyName(shipStrategy);
		edge.setPreProcessingOperationName(localStrategy);
		edge.setOperatorLevelCachingDescription(caching);

		return distributionPattern;
	}
	
	private void addLocalInfoFromChannelToConfig(Channel channel, TaskConfig config, int inputNum, boolean isBroadcastChannel) {
		// serializer
		if (isBroadcastChannel) {
			config.setBroadcastInputSerializer(channel.getSerializer(), inputNum);
			
			if (channel.getLocalStrategy() != LocalStrategy.NONE || (channel.getTempMode() != null && channel.getTempMode() != TempMode.NONE)) {
				throw new CompilerException("Found local strategy or temp mode on a broadcast variable channel.");
			} else {
				return;
			}
		} else {
			config.setInputSerializer(channel.getSerializer(), inputNum);
		}
		
		// local strategy
		if (channel.getLocalStrategy() != LocalStrategy.NONE) {
			config.setInputLocalStrategy(inputNum, channel.getLocalStrategy());
			if (channel.getLocalStrategyComparator() != null) {
				config.setInputComparator(channel.getLocalStrategyComparator(), inputNum);
			}
		}
		
		assignLocalStrategyResources(channel, config, inputNum);
		
		// materialization / caching
		if (channel.getTempMode() != null) {
			final TempMode tm = channel.getTempMode();

			boolean needsMemory = false;
			// Don't add a pipeline breaker if the data exchange is already blocking, EXCEPT the channel is within an iteration.
			if (tm.breaksPipeline() &&
					(channel.isOnDynamicPath() || channel.getDataExchangeMode() != DataExchangeMode.BATCH) ) {
				config.setInputAsynchronouslyMaterialized(inputNum, true);
				needsMemory = true;
			}
			if (tm.isCached()) {
				config.setInputCached(inputNum, true);
				needsMemory = true;
			}
			
			if (needsMemory) {
				// sanity check
				if (tm == TempMode.NONE || channel.getRelativeTempMemory() <= 0) {
					throw new CompilerException("Bug in compiler: Inconsistent description of input materialization.");
				}
				config.setRelativeInputMaterializationMemory(inputNum, channel.getRelativeTempMemory());
			}
		}
	}
	
	private void finalizeBulkIteration(IterationDescriptor descr) {
		
		final BulkIterationPlanNode bulkNode = (BulkIterationPlanNode) descr.getIterationNode();
		final JobVertex headVertex = descr.getHeadTask();
		final TaskConfig headConfig = new TaskConfig(headVertex.getConfiguration());
		final TaskConfig headFinalOutputConfig = descr.getHeadFinalResultConfig();
		
		// ------------ finalize the head config with the final outputs and the sync gate ------------
		final int numStepFunctionOuts = headConfig.getNumOutputs();
		final int numFinalOuts = headFinalOutputConfig.getNumOutputs();
		
		if (numStepFunctionOuts == 0) {
			throw new CompilerException("The iteration has no operation inside the step function.");
		}
		
		headConfig.setIterationHeadFinalOutputConfig(headFinalOutputConfig);
		headConfig.setIterationHeadIndexOfSyncOutput(numStepFunctionOuts + numFinalOuts);
		final double relativeMemForBackChannel = bulkNode.getRelativeMemoryPerSubTask();
		if (relativeMemForBackChannel <= 0) {
			throw new CompilerException("Bug: No memory has been assigned to the iteration back channel.");
		}
		headConfig.setRelativeBackChannelMemory(relativeMemForBackChannel);
		
		// --------------------------- create the sync task ---------------------------
		final JobVertex sync = new JobVertex("Sync (" + bulkNode.getNodeName() + ")");
		sync.setResources(bulkNode.getMinResources(), bulkNode.getPreferredResources());
		sync.setInvokableClass(IterationSynchronizationSinkTask.class);
		sync.setParallelism(1);
		sync.setMaxParallelism(1);
		this.auxVertices.add(sync);
		
		final TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
		syncConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, headVertex.getParallelism());

		// set the number of iteration / convergence criterion for the sync
		final int maxNumIterations = bulkNode.getIterationNode().getIterationContract().getMaximumNumberOfIterations();
		if (maxNumIterations < 1) {
			throw new CompilerException("Cannot create bulk iteration with unspecified maximum number of iterations.");
		}
		syncConfig.setNumberOfIterations(maxNumIterations);
		
		// connect the sync task
		sync.connectNewDataSetAsInput(headVertex, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		
		// ----------------------------- create the iteration tail ------------------------------
		
		final PlanNode rootOfTerminationCriterion = bulkNode.getRootOfTerminationCriterion();
		final PlanNode rootOfStepFunction = bulkNode.getRootOfStepFunction();
		final TaskConfig tailConfig;
		
		JobVertex rootOfStepFunctionVertex = this.vertices.get(rootOfStepFunction);
		if (rootOfStepFunctionVertex == null) {
			// last op is chained
			final TaskInChain taskInChain = this.chainedTasks.get(rootOfStepFunction);
			if (taskInChain == null) {
				throw new CompilerException("Bug: Tail of step function not found as vertex or chained task.");
			}
			rootOfStepFunctionVertex = taskInChain.getContainingVertex();

			// the fake channel is statically typed to pact record. no data is sent over this channel anyways.
			tailConfig = taskInChain.getTaskConfig();
		} else {
			tailConfig = new TaskConfig(rootOfStepFunctionVertex.getConfiguration());
		}
		
		tailConfig.setIsWorksetUpdate();
		
		// No following termination criterion
		if (rootOfStepFunction.getOutgoingChannels().isEmpty()) {
			
			rootOfStepFunctionVertex.setInvokableClass(IterationTailTask.class);
			
			tailConfig.setOutputSerializer(bulkNode.getSerializerForIterationChannel());
		}
		
		
		// create the fake output task for termination criterion, if needed
		final TaskConfig tailConfigOfTerminationCriterion;
		// If we have a termination criterion and it is not an intermediate node
		if(rootOfTerminationCriterion != null && rootOfTerminationCriterion.getOutgoingChannels().isEmpty()) {
			JobVertex rootOfTerminationCriterionVertex = this.vertices.get(rootOfTerminationCriterion);
			
			
			if (rootOfTerminationCriterionVertex == null) {
				// last op is chained
				final TaskInChain taskInChain = this.chainedTasks.get(rootOfTerminationCriterion);
				if (taskInChain == null) {
					throw new CompilerException("Bug: Tail of termination criterion not found as vertex or chained task.");
				}
				rootOfTerminationCriterionVertex = taskInChain.getContainingVertex();

				// the fake channel is statically typed to pact record. no data is sent over this channel anyways.
				tailConfigOfTerminationCriterion = taskInChain.getTaskConfig();
			} else {
				tailConfigOfTerminationCriterion = new TaskConfig(rootOfTerminationCriterionVertex.getConfiguration());
			}
			
			rootOfTerminationCriterionVertex.setInvokableClass(IterationTailTask.class);
			// Hack
			tailConfigOfTerminationCriterion.setIsSolutionSetUpdate();
			tailConfigOfTerminationCriterion.setOutputSerializer(bulkNode.getSerializerForIterationChannel());
			
			// tell the head that it needs to wait for the solution set updates
			headConfig.setWaitForSolutionSetUpdate();
		}
		
		// ------------------- register the aggregators -------------------
		AggregatorRegistry aggs = bulkNode.getIterationNode().getIterationContract().getAggregators();
		Collection<AggregatorWithName<?>> allAggregators = aggs.getAllRegisteredAggregators();
		
		headConfig.addIterationAggregators(allAggregators);
		syncConfig.addIterationAggregators(allAggregators);
		
		String convAggName = aggs.getConvergenceCriterionAggregatorName();
		ConvergenceCriterion<?> convCriterion = aggs.getConvergenceCriterion();
		
		if (convCriterion != null || convAggName != null) {
			if (convCriterion == null) {
				throw new CompilerException("Error: Convergence criterion aggregator set, but criterion is null.");
			}
			if (convAggName == null) {
				throw new CompilerException("Error: Aggregator convergence criterion set, but aggregator is null.");
			}
			
			syncConfig.setConvergenceCriterion(convAggName, convCriterion);
		}
	}
	
	private void finalizeWorksetIteration(IterationDescriptor descr) {
		final WorksetIterationPlanNode iterNode = (WorksetIterationPlanNode) descr.getIterationNode();
		final JobVertex headVertex = descr.getHeadTask();
		final TaskConfig headConfig = new TaskConfig(headVertex.getConfiguration());
		final TaskConfig headFinalOutputConfig = descr.getHeadFinalResultConfig();
		
		// ------------ finalize the head config with the final outputs and the sync gate ------------
		{
			final int numStepFunctionOuts = headConfig.getNumOutputs();
			final int numFinalOuts = headFinalOutputConfig.getNumOutputs();
			
			if (numStepFunctionOuts == 0) {
				throw new CompilerException("The workset iteration has no operation on the workset inside the step function.");
			}
			
			headConfig.setIterationHeadFinalOutputConfig(headFinalOutputConfig);
			headConfig.setIterationHeadIndexOfSyncOutput(numStepFunctionOuts + numFinalOuts);
			final double relativeMemory = iterNode.getRelativeMemoryPerSubTask();
			if (relativeMemory <= 0) {
				throw new CompilerException("Bug: No memory has been assigned to the workset iteration.");
			}
			
			headConfig.setIsWorksetIteration();
			headConfig.setRelativeBackChannelMemory(relativeMemory / 2);
			headConfig.setRelativeSolutionSetMemory(relativeMemory / 2);
			
			// set the solution set serializer and comparator
			headConfig.setSolutionSetSerializer(iterNode.getSolutionSetSerializer());
			headConfig.setSolutionSetComparator(iterNode.getSolutionSetComparator());
		}
		
		// --------------------------- create the sync task ---------------------------
		final TaskConfig syncConfig;
		{
			final JobVertex sync = new JobVertex("Sync (" + iterNode.getNodeName() + ")");
			sync.setResources(iterNode.getMinResources(), iterNode.getPreferredResources());
			sync.setInvokableClass(IterationSynchronizationSinkTask.class);
			sync.setParallelism(1);
			sync.setMaxParallelism(1);
			this.auxVertices.add(sync);
			
			syncConfig = new TaskConfig(sync.getConfiguration());
			syncConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, headVertex.getParallelism());
	
			// set the number of iteration / convergence criterion for the sync
			final int maxNumIterations = iterNode.getIterationNode().getIterationContract().getMaximumNumberOfIterations();
			if (maxNumIterations < 1) {
				throw new CompilerException("Cannot create workset iteration with unspecified maximum number of iterations.");
			}
			syncConfig.setNumberOfIterations(maxNumIterations);
			
			// connect the sync task
			sync.connectNewDataSetAsInput(headVertex, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		}
		
		// ----------------------------- create the iteration tails -----------------------------
		// ----------------------- for next workset and solution set delta-----------------------

		{
			// we have three possible cases:
			// 1) Two tails, one for workset update, one for solution set update
			// 2) One tail for workset update, solution set update happens in an intermediate task
			// 3) One tail for solution set update, workset update happens in an intermediate task
			
			final PlanNode nextWorksetNode = iterNode.getNextWorkSetPlanNode();
			final PlanNode solutionDeltaNode = iterNode.getSolutionSetDeltaPlanNode();
			
			final boolean hasWorksetTail = nextWorksetNode.getOutgoingChannels().isEmpty();
			final boolean hasSolutionSetTail = (!iterNode.isImmediateSolutionSetUpdate()) || (!hasWorksetTail);
			
			{
				// get the vertex for the workset update
				final TaskConfig worksetTailConfig;
				JobVertex nextWorksetVertex = this.vertices.get(nextWorksetNode);
				if (nextWorksetVertex == null) {
					// nextWorksetVertex is chained
					TaskInChain taskInChain = this.chainedTasks.get(nextWorksetNode);
					if (taskInChain == null) {
						throw new CompilerException("Bug: Next workset node not found as vertex or chained task.");
					}
					nextWorksetVertex = taskInChain.getContainingVertex();
					worksetTailConfig = taskInChain.getTaskConfig();
				} else {
					worksetTailConfig = new TaskConfig(nextWorksetVertex.getConfiguration());
				}
				
				// mark the node to perform workset updates
				worksetTailConfig.setIsWorksetIteration();
				worksetTailConfig.setIsWorksetUpdate();
				
				if (hasWorksetTail) {
					nextWorksetVertex.setInvokableClass(IterationTailTask.class);

					worksetTailConfig.setOutputSerializer(iterNode.getWorksetSerializer());
				}
			}
			{
				final TaskConfig solutionDeltaConfig;
				JobVertex solutionDeltaVertex = this.vertices.get(solutionDeltaNode);
				if (solutionDeltaVertex == null) {
					// last op is chained
					TaskInChain taskInChain = this.chainedTasks.get(solutionDeltaNode);
					if (taskInChain == null) {
						throw new CompilerException("Bug: Solution Set Delta not found as vertex or chained task.");
					}
					solutionDeltaVertex = taskInChain.getContainingVertex();
					solutionDeltaConfig = taskInChain.getTaskConfig();
				} else {
					solutionDeltaConfig = new TaskConfig(solutionDeltaVertex.getConfiguration());
				}
				
				solutionDeltaConfig.setIsWorksetIteration();
				solutionDeltaConfig.setIsSolutionSetUpdate();
				
				if (hasSolutionSetTail) {
					solutionDeltaVertex.setInvokableClass(IterationTailTask.class);
					
					solutionDeltaConfig.setOutputSerializer(iterNode.getSolutionSetSerializer());
					
					// tell the head that it needs to wait for the solution set updates
					headConfig.setWaitForSolutionSetUpdate();
				}
				else {
					// no tail, intermediate update. must be immediate update
					if (!iterNode.isImmediateSolutionSetUpdate()) {
						throw new CompilerException("A solution set update without dedicated tail is not set to perform immediate updates.");
					}
					solutionDeltaConfig.setIsSolutionSetUpdateWithoutReprobe();
				}
			}
		}
		
		// ------------------- register the aggregators -------------------
		AggregatorRegistry aggs = iterNode.getIterationNode().getIterationContract().getAggregators();
		Collection<AggregatorWithName<?>> allAggregators = aggs.getAllRegisteredAggregators();
		
		for (AggregatorWithName<?> agg : allAggregators) {
			if (agg.getName().equals(WorksetEmptyConvergenceCriterion.AGGREGATOR_NAME)) {
				throw new CompilerException("User defined aggregator used the same name as built-in workset " +
						"termination check aggregator: " + WorksetEmptyConvergenceCriterion.AGGREGATOR_NAME);
			}
		}
		
		headConfig.addIterationAggregators(allAggregators);
		syncConfig.addIterationAggregators(allAggregators);
		
		String convAggName = aggs.getConvergenceCriterionAggregatorName();
		ConvergenceCriterion<?> convCriterion = aggs.getConvergenceCriterion();

		if (convCriterion != null || convAggName != null) {
			if (convCriterion == null) {
				throw new CompilerException("Error: Convergence criterion aggregator set, but criterion is null.");
			}
			if (convAggName == null) {
				throw new CompilerException("Error: Aggregator convergence criterion set, but aggregator is null.");
			}

			syncConfig.setConvergenceCriterion(convAggName, convCriterion);
		}
		
		headConfig.addIterationAggregator(WorksetEmptyConvergenceCriterion.AGGREGATOR_NAME, new LongSumAggregator());
		syncConfig.addIterationAggregator(WorksetEmptyConvergenceCriterion.AGGREGATOR_NAME, new LongSumAggregator());
		syncConfig.setImplicitConvergenceCriterion(WorksetEmptyConvergenceCriterion.AGGREGATOR_NAME, new WorksetEmptyConvergenceCriterion());
	}
	
	private String getDescriptionForUserCode(UserCodeWrapper<?> wrapper) {
		try {
			if (wrapper.hasObject()) {
				try {
					return wrapper.getUserCodeObject().toString();
				}
				catch (Throwable t) {
					return wrapper.getUserCodeClass().getName();
				}
			}
			else {
				return wrapper.getUserCodeClass().getName();
			}
		}
		catch (Throwable t) {
			return null;
		}
	}
	
	private void attachOperatorNamesAndDescriptions() {
		JsonFactory jsonFactory = new JsonFactory();

		// we go back to front

		// start with the in chains 
		for (int i = chainedTasksInSequence.size() - 1; i >= 0; i--) {
			TaskInChain next = chainedTasksInSequence.get(i);
			PlanNode planNode = next.getPlanNode();

			JobVertex vertex = next.getContainingVertex();

			// operator
			String opName = planNode.getOptimizerNode().getOperatorName();
			if (vertex.getOperatorName() == null) {
				vertex.setOperatorName(opName);
			}
			else {
				vertex.setOperatorName(opName + " -> " + vertex.getOperatorName());
			}

			// operator description 
			String opDescription = JsonMapper.getOperatorStrategyString(planNode.getDriverStrategy());
			if (vertex.getOperatorDescription() == null) {
				vertex.setOperatorDescription(opDescription);
			}
			else {
				vertex.setOperatorDescription(opDescription + "\n -> " + vertex.getOperatorDescription());
			}

			// pretty name
			String prettyName = StringUtils.showControlCharacters(planNode.getNodeName());
			if (vertex.getOperatorPrettyName() == null) {
				vertex.setOperatorPrettyName(prettyName);
			}
			else {
				vertex.setOperatorPrettyName(prettyName + "\n -> " + vertex.getOperatorPrettyName());
			}

			// optimizer output properties
			if (vertex.getResultOptimizerProperties() == null) {
				// since we go backwards, this must be the last in its chain
				String outputProps =
						JsonMapper.getOptimizerPropertiesJson(jsonFactory, planNode);
				vertex.setResultOptimizerProperties(outputProps);
			}
		}

		// finish back-to-front traversal by going over the head vertices
		for (Map.Entry<PlanNode, JobVertex> entry : vertices.entrySet()) {
			PlanNode node = entry.getKey();
			JobVertex vertex = entry.getValue();

			// get the predecessors

			String input1name = null;
			String input2name = null;
			int num = 0;
			for (Channel c : node.getInputs()) {
				if (num == 0) {
					input1name = c.getSource().getNodeName();
				}
				else if (num == 1) {
					input2name = c.getSource().getNodeName();
				}
				num++;
			}

			// operator
			String opName = node.getOptimizerNode().getOperatorName();
			if (vertex.getOperatorName() == null) {
				vertex.setOperatorName(opName);
			}
			else {
				vertex.setOperatorName(opName + " -> " + vertex.getOperatorName());
			}

			// operator description
			String opStrategy = JsonMapper.getOperatorStrategyString(
					node.getDriverStrategy(),
					input1name != null ? input1name : "(unnamed)",
					input2name != null ? input2name : "(unnamed)");

			if (vertex.getOperatorDescription() == null) {
				vertex.setOperatorDescription(opStrategy);
			}
			else {
				vertex.setOperatorDescription(opStrategy + "\n -> " + vertex.getOperatorDescription());
			}

			// pretty name
			String prettyName = StringUtils.showControlCharacters(node.getNodeName());
			if (vertex.getOperatorPrettyName() == null) {
				vertex.setOperatorPrettyName(prettyName);
			}
			else {
				vertex.setOperatorPrettyName(prettyName + "\n -> " + vertex.getOperatorPrettyName());
			}

			// if there is not yet an output from a chained task, we set this output
			if (vertex.getResultOptimizerProperties() == null) {
				vertex.setResultOptimizerProperties(
						JsonMapper.getOptimizerPropertiesJson(jsonFactory, node));
			}
		}
	}

	// -------------------------------------------------------------------------------------
	// Descriptors for tasks / configurations that are chained or merged with other tasks
	// -------------------------------------------------------------------------------------
	
	/**
	 * Utility class that describes a task in a sequence of chained tasks. Chained tasks are tasks that run
	 * together in one thread.
	 */
	private static final class TaskInChain {
		
		private final Class<? extends ChainedDriver<?, ?>> chainedTask;
		
		private final TaskConfig taskConfig;
		
		private final String taskName;
		
		private final PlanNode planNode;
		
		private JobVertex containingVertex;

		TaskInChain(PlanNode planNode, Class<? extends ChainedDriver<?, ?>> chainedTask,
					TaskConfig taskConfig, String taskName) {
			
			this.planNode = planNode;
			this.chainedTask = chainedTask;
			this.taskConfig = taskConfig;
			this.taskName = taskName;
		}


		public PlanNode getPlanNode() {
			return planNode;
		}

		public Class<? extends ChainedDriver<?, ?>> getChainedTask() {
			return this.chainedTask;
		}
		
		public TaskConfig getTaskConfig() {
			return this.taskConfig;
		}
		
		public String getTaskName() {
			return this.taskName;
		}
		
		public JobVertex getContainingVertex() {
			return this.containingVertex;
		}
		
		public void setContainingVertex(JobVertex containingVertex) {
			this.containingVertex = containingVertex;
		}
	}
	
	private static final class IterationDescriptor {
		
		private final IterationPlanNode iterationNode;
		
		private JobVertex headTask;
		
		private TaskConfig headConfig;
		
		private TaskConfig  headFinalResultConfig;
		
		private final int id;

		public IterationDescriptor(IterationPlanNode iterationNode, int id) {
			this.iterationNode = iterationNode;
			this.id = id;
		}
		
		public IterationPlanNode getIterationNode() {
			return iterationNode;
		}
		
		public void setHeadTask(JobVertex headTask, TaskConfig headConfig) {
			this.headTask = headTask;
			this.headFinalResultConfig = new TaskConfig(new Configuration());
			
			// check if we already had a configuration, for example if the solution set was 
			if (this.headConfig != null) {
				headConfig.getConfiguration().addAll(this.headConfig.getConfiguration());
			}
			
			this.headConfig = headConfig;
		}
		
		public JobVertex getHeadTask() {
			return headTask;
		}
		
		public TaskConfig getHeadFinalResultConfig() {
			return headFinalResultConfig;
		}
		
		public int getId() {
			return this.id;
		}
	}
}
