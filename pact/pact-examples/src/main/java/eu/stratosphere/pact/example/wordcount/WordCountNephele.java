//package eu.stratosphere.pact.example.wordcount;
//
//import java.util.Iterator;
//
//import eu.stratosphere.nephele.client.JobClient;
//import eu.stratosphere.nephele.configuration.ConfigConstants;
//import eu.stratosphere.nephele.configuration.Configuration;
//import eu.stratosphere.nephele.io.channels.ChannelType;
//import eu.stratosphere.nephele.io.compression.CompressionLevel;
//import eu.stratosphere.nephele.jobgraph.JobGraph;
//import eu.stratosphere.nephele.jobgraph.JobInputVertex;
//import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
//import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
//import eu.stratosphere.pact.common.io.DelimitedOutputFormat;
//import eu.stratosphere.pact.common.io.FileInputFormat;
//import eu.stratosphere.pact.common.io.FileOutputFormat;
//import eu.stratosphere.pact.common.io.FixedLengthInputFormat;
//import eu.stratosphere.pact.common.stubs.Collector;
//import eu.stratosphere.pact.common.stubs.MapStub;
//import eu.stratosphere.pact.common.stubs.ReduceStub;
//import eu.stratosphere.pact.common.type.PactRecord;
//import eu.stratosphere.pact.common.type.base.PactInteger;
//import eu.stratosphere.pact.example.io.BinaryIntInputFormat;
//import eu.stratosphere.pact.runtime.task.DataSinkTask;
//import eu.stratosphere.pact.runtime.task.DataSourceTask;
//import eu.stratosphere.pact.runtime.task.MapTask;
//import eu.stratosphere.pact.runtime.task.ReduceTask;
//import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
//import eu.stratosphere.pact.runtime.task.util.TaskConfig;
//import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
//
//
///**
// *
// *
// * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
// */
//public class WordCountNephele
//{
//	private static final int NUM_NODES = 1;
//	
//	private static final long MEMORY_PER_NODE = 350 * 1024 * 1024L;
//
//	public static class FrequencyOutFormat extends DelimitedOutputFormat
//	{
//		private byte[] decodeBuffer = new byte[16];
//
//		/* (non-Javadoc)
//		 * @see eu.stratosphere.pact.common.io.DelimitedOutputFormat#serializeRecord(eu.stratosphere.pact.common.type.PactRecord, byte[])
//		 */
//		@Override
//		public int serializeRecord(PactRecord rec, byte[] target) throws Exception {
//			int key = rec.getField(0, PactInteger.class).getValue();
//			int count = rec.getField(1, PactInteger.class).getValue();
//			
//			int targetpos = 0;
//			int pos = 0;
//			boolean negative = false;
//			
//			// code the key
//			if (key < 0) {
//				negative = true;
//				key = -key;
//			}
//			do {
//				decodeBuffer[pos++] = (byte) ((key % 10) + '0');
//				key /= 10;
//			} while (key > 0);
//			if (negative) {
//				decodeBuffer[pos++] = (byte) '-';
//			}
//			
//			if (target.length < pos) {
//				return -pos * 2;
//			}
//			do {
//				target[targetpos++] = decodeBuffer[--pos];
//			} while (pos > 0);
//			
//			// code the space
//			target[targetpos++] = ' ';
//			
//			// code the count
//			pos = 0;
//			do {
//				decodeBuffer[pos++] = (byte) ((count % 10) + '0');
//				count /= 10;
//			} while (count > 0);
//			if (target.length < targetpos + pos) {
//				return -(targetpos + pos);
//			}
//			do {
//				target[targetpos++] = decodeBuffer[--pos];
//			} while (pos > 0);
//			
//			return targetpos;
//		}
//
//	}
//
//	public static final class Identity extends MapStub
//	{
//		@Override
//		public void map(PactRecord record, Collector out) throws Exception {
//			out.collect(record);
//		}	
//	}
//
//	public static final class CountRecords extends ReduceStub
//	{
//		private final PactInteger integer = new PactInteger();
//		
//		@Override
//		public void reduce(Iterator<PactRecord> records, Collector out) throws Exception
//		{
//			PactRecord rec = null;
//			int cnt = 0;
//			while (records.hasNext()) {
//				rec = records.next();
//				cnt++;
//			}	
//			this.integer.setValue(cnt);
//			rec.setField(1, this.integer);
//			out.collect(rec);
//		}
//	}
//
//
//
//	
//	// ============================================================================================
//	// ============================================================================================
//	
//	@SuppressWarnings("unchecked")
//	public static void main(String[] args) throws Exception
//	{
//		String path = (args.length > 0 ? args[0] : "");
//		int numSendingTasks   = (args.length > 1 ? Integer.parseInt(args[1]) : 1);
//		int numReceivingTasks   = (args.length > 2 ? Integer.parseInt(args[2]) : 1);
//		String outPath    = (args.length > 3 ? args[3] : "");
//		
//		
//		final JobGraph jobGraph = new JobGraph("Word Count");
//		
//		// ----------------------------------------------------------------------------------------
//		//                              Data Source
//		// ----------------------------------------------------------------------------------------
//		
//		JobInputVertex sourceVertex = new JobInputVertex("File Data Source", jobGraph);
//		sourceVertex.setInputClass(DataSourceTask.class);
//		sourceVertex.setNumberOfSubtasks(numSendingTasks);
//		sourceVertex.setNumberOfSubtasksPerInstance(numSendingTasks / NUM_NODES + (numSendingTasks % NUM_NODES == 0 ? 0 : 1));
//		
//		TaskConfig sourceConfig = new TaskConfig(sourceVertex.getConfiguration());
//		sourceConfig.setStubClass(BinaryIntInputFormat.class);
//		sourceConfig.setStubParameter(FileInputFormat.FILE_PARAMETER_KEY, path);
//		sourceConfig.setStubParameter(FixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, String.valueOf(4));
//		sourceConfig.setStubParameter(BinaryIntInputFormat.PAYLOAD_SIZE_PARAMETER_KEY, String.valueOf(4));
//		sourceConfig.setLocalStrategy(LocalStrategy.NONE);
//		sourceConfig.addOutputShipStrategy(ShipStrategy.FORWARD);
//		
//		// ----------------------------------------------------------------------------------------
//		//                                 Map
//		// ----------------------------------------------------------------------------------------
//		
//		JobTaskVertex mapVertex = new JobTaskVertex("Identity Map", jobGraph);
//		mapVertex.setTaskClass(MapTask.class);
//		mapVertex.setNumberOfSubtasks(numSendingTasks);
//		mapVertex.setNumberOfSubtasksPerInstance(numSendingTasks / NUM_NODES + (numSendingTasks % NUM_NODES == 0 ? 0 : 1));
//		TaskConfig mapConfig = new TaskConfig(mapVertex.getConfiguration());
//		mapConfig.setStubClass(Identity.class);
//		mapConfig.setLocalStrategy(LocalStrategy.NONE);
//		mapConfig.addInputShipStrategy(ShipStrategy.FORWARD);
//		mapConfig.addOutputShipStrategy(ShipStrategy.PARTITION_HASH, new int[] {0}, new Class[] {PactInteger.class});
//			
//		// ----------------------------------------------------------------------------------------
//		//                                Reduce
//		// ----------------------------------------------------------------------------------------
//		
//		JobTaskVertex reduceVertex = new JobTaskVertex("Count Records", jobGraph);
//		reduceVertex.setTaskClass(ReduceTask.class);
//		reduceVertex.setNumberOfSubtasks(numReceivingTasks);
//		reduceVertex.setNumberOfSubtasksPerInstance(numReceivingTasks / NUM_NODES + (numReceivingTasks % NUM_NODES == 0 ? 0 : 1));
//		TaskConfig reduceConfig = new TaskConfig(reduceVertex.getConfiguration());
//		reduceConfig.setStubClass(CountRecords.class);
//		reduceConfig.setLocalStrategy(LocalStrategy.SORT);
//		reduceConfig.setLocalStrategyKeyTypes(new Class[] {PactInteger.class});
//		reduceConfig.setLocalStrategyKeyTypes(0, new int[] {0});
//		reduceConfig.setMemorySize(MEMORY_PER_NODE / (numReceivingTasks / NUM_NODES + (numReceivingTasks % NUM_NODES == 0 ? 0 : 1)));
//		reduceConfig.setNumFilehandles(64);
//		reduceConfig.addInputShipStrategy(ShipStrategy.PARTITION_HASH);
//		reduceConfig.addOutputShipStrategy(ShipStrategy.FORWARD);
//		
//		// ----------------------------------------------------------------------------------------
//		//                               Data Sink
//		// ----------------------------------------------------------------------------------------
//		
//		JobOutputVertex sinkVertex = new JobOutputVertex("Frequency Output", jobGraph);
//		sinkVertex.setOutputClass(DataSinkTask.class);
//		sinkVertex.setNumberOfSubtasks(numReceivingTasks);
//		sinkVertex.setNumberOfSubtasksPerInstance(numReceivingTasks / NUM_NODES + (numReceivingTasks % NUM_NODES == 0 ? 0 : 1));
//		sinkVertex.getConfiguration().setInteger(DataSinkTask.DEGREE_OF_PARALLELISM_KEY, numReceivingTasks);
//		TaskConfig sinkConfig = new TaskConfig(sinkVertex.getConfiguration());
//		sinkConfig.setStubClass(FrequencyOutFormat.class);
//		sinkConfig.setLocalStrategy(LocalStrategy.NONE);
//		sinkConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, outPath);
//		sinkConfig.addInputShipStrategy(ShipStrategy.FORWARD);
//		
//		sourceVertex.connectTo(mapVertex, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
//		mapVertex.connectTo(reduceVertex, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);
//		reduceVertex.connectTo(sinkVertex, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
//		
//		if (numReceivingTasks > numSendingTasks) {
//			sourceVertex.setVertexToShareInstancesWith(reduceVertex);
//			mapVertex.setVertexToShareInstancesWith(reduceVertex);
//			sinkVertex.setVertexToShareInstancesWith(reduceVertex);
//		}
//		else {
//			sourceVertex.setVertexToShareInstancesWith(mapVertex);
//			reduceVertex.setVertexToShareInstancesWith(mapVertex);
//			sinkVertex.setVertexToShareInstancesWith(mapVertex);
//		}
//		
//		Configuration cfg = new Configuration();
//		cfg.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
//		cfg.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, 6123);
//		
//		JobClient client = new JobClient(jobGraph, cfg);
//		client.submitJobAndWait();
//	}
//}
