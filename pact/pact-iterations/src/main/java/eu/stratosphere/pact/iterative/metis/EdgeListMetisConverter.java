package eu.stratosphere.pact.iterative.metis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

public class EdgeListMetisConverter {
	private static final int READ_BUFFER_CAPACITY = 64*1024;
	
	private HashMap<Integer, Set<Integer>> graph;
	private HashMap<Integer, Integer> mapping;
	
	private int idCount = 1;
	private int edgeCount = 0;
	
	public EdgeListMetisConverter() {
		graph = new HashMap<Integer, Set<Integer>>(100000);
		mapping = new HashMap<Integer, Integer>(100000);
	}
	
	public void readEdgeList(File edgeListFile) {
		RandomAccessFile file = null;
		FileChannel channel = null;
		
		try {	
			file = new RandomAccessFile(edgeListFile, "r");
			channel = file.getChannel();

			final ByteBuffer buffer = ByteBuffer.allocateDirect(READ_BUFFER_CAPACITY);
			int current = 0;
			int first = 0;
		
			while (channel.read(buffer) != -1) {
				buffer.flip();
				while (buffer.hasRemaining()) {
					int next = buffer.get();
					if (next == '\n') {
						Integer firstI = getMapping(first);
						Integer secondI = getMapping(current);
						
						Set<Integer> set = graph.get(firstI);
						if(set == null) {
							set = new HashSet<Integer>();
							graph.put(firstI, set);
						}
						if(set.add(secondI)) {
							edgeCount++;
						}
						
						set = graph.get(secondI);
						if(set == null) {
							set = new HashSet<Integer>();
							graph.put(secondI, set);
						}
						set.add(firstI);
						
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
	
	public void writeMetis(File metisPath) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(metisPath));
		
		writer.write(graph.size() + " " + edgeCount + "\n");
		
		for (int i = 1; i < idCount; i++) {
			int nodeId = i;
			Set<Integer> adjacencyList = graph.get(nodeId);
			boolean first = true;
			for (Integer integer : adjacencyList) {
				if(first) {
					first = false;
				} else {
					writer.write(' ');
				}
				writer.write(integer.toString());
			}
			writer.write('\n');
		}
		
		writer.close();
	}
	
	private void writeMapping(File mappingFile) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(mappingFile));
		
		for (Entry<Integer, Integer> mappingEntry : mapping.entrySet()) {
			int oldId = mappingEntry.getKey();
			int metisId = mappingEntry.getValue();
			
			writer.write(oldId+","+metisId+"\n");
		}
		
		writer.close();
	}
	
	private Integer getMapping(int id) {
		Integer mappedId = mapping.get(id);
		if(mappedId == null) {
			mappedId = idCount;
			idCount++;
			mapping.put(id, mappedId);
		}
		
		return mappedId;
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		EdgeListMetisConverter conv = new EdgeListMetisConverter();
		System.out.println("Start reading");
		conv.readEdgeList(new File(args[0]));
		System.out.println("Finished reading");
		System.out.println("Start writing metis file");
		conv.writeMetis(new File(args[1]));
		System.out.println("Finished writing metis file");
		System.out.println("Start writing id mapping file");
		conv.writeMapping(new File(args[2]));
		System.out.println("Finished writing id mapping file");
	}
}
