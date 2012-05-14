package eu.stratosphere.pact.iterative.metis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map.Entry;

public class MetisPartitioner {

	private static final int READ_BUFFER_CAPACITY = 64*1024;
	private HashMap<Integer, Integer> hashMapping;
	private HashMap<Integer, Integer> metisMapping;
	private HashMap<Integer, Integer> partitions;
	
	public MetisPartitioner() {
		metisMapping = new HashMap<Integer, Integer>(100000);
		hashMapping = new HashMap<Integer, Integer>(100000);
		partitions = new HashMap<Integer, Integer>(100000);
	}

	private void createHashPartitioningIds(int numPartitions) {
		for (Entry<Integer, Integer> partitionEntry : partitions.entrySet()) {
			int metisID = partitionEntry.getKey();
			int partition = partitionEntry.getValue();
			int graphId = metisMapping.get(metisID);
			
			hashMapping.put(graphId, (graphId * numPartitions) + partition);
		}
	}

	private void convertGraphFile(File graphFile, File outputFile) throws IOException {
		RandomAccessFile file = null;
		FileChannel channel = null;
		
		BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
		
		try {	
			file = new RandomAccessFile(graphFile, "r");
			channel = file.getChannel();

			final ByteBuffer buffer = ByteBuffer.allocateDirect(READ_BUFFER_CAPACITY);
			int current = 0;
			int first = 0;
		
			while (channel.read(buffer) != -1) {
				buffer.flip();
				while (buffer.hasRemaining()) {
					int next = buffer.get();
					if (next == '\n') {
						Integer firstI = hashMapping.get(first);
						Integer secondI = hashMapping.get(current);
						
						writer.write(firstI + "," + secondI + "\n");
						
						current = 0;
					}
					else if (next == ',') {
						first = current;
						current = 0;
					}
					else {
						current *= 10;
						current += (next - '0');
					}
				}
				buffer.clear();
			}
		}
		catch (IOException ioex) {
			System.err.println("Error reading the input into the hashtable: " + ioex.getMessage());
			ioex.printStackTrace(System.err);
			return;
		}
		finally {
			try {
				if (channel != null) {channel.close(); channel = null;}
				if (file != null) {file.close(); file = null;}
			}
			catch (IOException ioex) {
				System.err.println("Error closing the input file: " + ioex.getMessage());
				ioex.printStackTrace(System.err);
				return;
			}
		}
		
		writer.close();
	}

	private void readPartitioningFile(File partitionFile) {
		RandomAccessFile file = null;
		FileChannel channel = null;
		
		try {	
			file = new RandomAccessFile(partitionFile, "r");
			channel = file.getChannel();

			final ByteBuffer buffer = ByteBuffer.allocateDirect(READ_BUFFER_CAPACITY);
			int current = 0;
			int id = 1;
		
			while (channel.read(buffer) != -1) {
				buffer.flip();
				while (buffer.hasRemaining()) {
					int next = buffer.get();
					if (next == '\n') {
						Integer partition = current;
						
						partitions.put(id, partition);
						id++;
						current = 0;
					}
					else {
						current *= 10;
						current += (next - '0');
					}
				}
				buffer.clear();
			}
		}
		catch (IOException ioex) {
			System.err.println("Error reading the input into the hashtable: " + ioex.getMessage());
			ioex.printStackTrace(System.err);
			return;
		}
		finally {
			try {
				if (channel != null) {channel.close(); channel = null;}
				if (file != null) {file.close(); file = null;}
			}
			catch (IOException ioex) {
				System.err.println("Error closing the input file: " + ioex.getMessage());
				ioex.printStackTrace(System.err);
				return;
			}
		}
	}

	private void readIdMappings(File mappingFile) {
		RandomAccessFile file = null;
		FileChannel channel = null;
		
		try {	
			file = new RandomAccessFile(mappingFile, "r");
			channel = file.getChannel();

			final ByteBuffer buffer = ByteBuffer.allocateDirect(READ_BUFFER_CAPACITY);
			int current = 0;
			int first = 0;
		
			while (channel.read(buffer) != -1) {
				buffer.flip();
				while (buffer.hasRemaining()) {
					int next = buffer.get();
					if (next == '\n') {
						Integer firstI = first;
						Integer secondI = current;
						
						metisMapping.put(secondI, firstI);
						current = 0;
					}
					else if (next == ',') {
						first = current;
						current = 0;
					}
					else {
						current *= 10;
						current += (next - '0');
					}
				}
				buffer.clear();
			}
		}
		catch (IOException ioex) {
			System.err.println("Error reading the input into the hashtable: " + ioex.getMessage());
			ioex.printStackTrace(System.err);
			return;
		}
		finally {
			try {
				if (channel != null) {channel.close(); channel = null;}
				if (file != null) {file.close(); file = null;}
			}
			catch (IOException ioex) {
				System.err.println("Error closing the input file: " + ioex.getMessage());
				ioex.printStackTrace(System.err);
				return;
			}
		}
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		int numPartitions = Integer.valueOf(args[0]);
		
		MetisPartitioner partitioner = new MetisPartitioner();
		System.out.println("Reading mapping file for node ids");
		partitioner.readIdMappings(new File(args[1]));
		System.out.println("Finished reading mapping file for node ids");
		System.out.println("Reading mapping file for nodes to partitions");
		partitioner.readPartitioningFile(new File(args[2]));
		System.out.println("Finished reading mapping file for noes to partitions");
		partitioner.createHashPartitioningIds(numPartitions);
		System.out.println("Start converting graph");
		partitioner.convertGraphFile(new File(args[3]), new File(args[4]));
		System.out.println("Finished converting graph");
		// TODO Auto-generated method stub

	}

}

