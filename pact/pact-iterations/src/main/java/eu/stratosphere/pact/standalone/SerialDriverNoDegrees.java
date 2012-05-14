package eu.stratosphere.pact.standalone;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class SerialDriverNoDegrees {
  public static final int READ_BUFFER_CAPACITY = 64*1024;


  public static void main(String[] args)
  {
    final String filePath = args[0];
    final long totalStart = System.nanoTime();

    final HashMap<Integer, SerialTriangleEntryNoDegrees> map = new HashMap<Integer, SerialTriangleEntryNoDegrees>(900000, 0.85f);

    // ========================================================================================
    //                           Phase 1: Read elements into Hash-Table
    // ========================================================================================
 {
      long readAndBuildStart = System.nanoTime();

      RandomAccessFile file = null;
      FileChannel channel = null;

      try {
        file = new RandomAccessFile(filePath, "r");
        channel = file.getChannel();

        final ByteBuffer buffer = ByteBuffer.allocateDirect(READ_BUFFER_CAPACITY);
        int current = 0;
        int first = 0;

        while (channel.read(buffer) != -1) {
          buffer.flip();
          while (buffer.hasRemaining()) {
            int next = buffer.get();
            if (next == '\n') {
              // add an entry in both directions
              Integer fi = Integer.valueOf(first);
              SerialTriangleEntryNoDegrees firstEntry = map.get(fi);
              if (firstEntry == null) {
                firstEntry = new SerialTriangleEntryNoDegrees();
                map.put(fi, firstEntry);
              }
              firstEntry.add(current);

              Integer si = Integer.valueOf(current);
              SerialTriangleEntryNoDegrees secondEntry = map.get(si);
              if (secondEntry == null) {
                secondEntry = new SerialTriangleEntryNoDegrees();
                map.put(si, secondEntry);
              }
              secondEntry.add(first);

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

      long readAndBuildElapsed = System.nanoTime() - readAndBuildStart;
      System.out.println("Reading and Building took: " + (readAndBuildElapsed / 1000000) + "msecs");
    }

    // ========================================================================================
    //                           Phase 2: Sort and uniquify the IDs
    // ========================================================================================
 {
      long sortingStart = System.nanoTime();

      for (Map.Entry<Integer, SerialTriangleEntryNoDegrees> entry : map.entrySet()) {
        SerialTriangleEntryNoDegrees tEntry = entry.getValue();
        tEntry.finalizeListBuilding();
      }

      long sortingElapsed = System.nanoTime() - sortingStart;
      System.out.println("Finalizing took: " + (sortingElapsed / 1000000) + "msecs");
    }

    // ========================================================================================
    //                           Phase 4: Build Triangles
    // ========================================================================================
 {
      long enumerationStart = System.nanoTime();

      long totalTriangles = 0;

      Iterator<Map.Entry<Integer, SerialTriangleEntryNoDegrees>> entryIter = map.entrySet().iterator();
      while (entryIter.hasNext())
      {
        final Map.Entry<Integer, SerialTriangleEntryNoDegrees> kv = entryIter.next();
        final SerialTriangleEntryNoDegrees entry = kv.getValue();
        final int key = kv.getKey().intValue();
        final int degree = entry.size();

        // notify all that have a larger degree of our neighbors with a lower degree than them
        for (int i = 0; i < degree; i++) {
          final int toNotifyId = entry.getId(i);

          // rule out which ones not to notify
          if (toNotifyId < key)
            continue;

          // notify that one of all our neighbors with a smaller id than that one
          final SerialTriangleEntryNoDegrees toNotify = map.get(Integer.valueOf(toNotifyId));
          int numTriangles = toNotify.countTriangles(entry.getAllIds(), entry.getAllTriangleCounts(), i, key);
          entry.setNumTrianglesForEdge(i, numTriangles);

          totalTriangles += numTriangles;
        }
      }

      long enumerationElapsed = System.nanoTime() - enumerationStart;
      System.out.println("Enumeration took: " + (enumerationElapsed / 1000000) + "msecs");

      System.out.println("FOUND " + totalTriangles + " TRIANGLES.");
    }

    long totalElapsed = System.nanoTime() - totalStart;
    System.out.println("TOTAL TIME: " + (totalElapsed / 1000000) + "msecs");
  }
}
