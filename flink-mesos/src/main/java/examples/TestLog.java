package examples; /**
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServer.BasicDataTreeBuilder;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;


// This could be used both in local mode (to include in unit tests)
// and in distributed mode (as a benchmark test) based on the command
// line flags passed to it.
//
// Local: <zk_url> = "local"
//        In this mode a ZooKeeper server and *all* the replicas
//        are spawned locally in process.
//
// Distributed: <zk_url> != "local"
//        In this mode only one replica is spawned locally. The
//        ZooKeeper URL is used to discover other replicas.
//
// If "<load_file>" is provided the replica will continously append
// as many random chunks of data as there are lines in the file. The
// format of the file is one number per line where the number
// represents the size of the chunk in bytes.
public class TestLog {

  private static final Logger LOG = Logger.getLogger(TestLog.class.getName());

  private static ZooKeeperTestServer zkserver = null;

  static class ZooKeeperTestServer {
    private ZooKeeperServer server = null;
    private NIOServerCnxnFactory connection = null;
    private String logdir;

    ZooKeeperTestServer(String dir) {
      logdir = dir;
    }

    private InetSocketAddress start() throws IOException, InterruptedException {
      int port = 0; // Ephemeral port.
      int maxconnections = 1000;

      File dataFile = new File(logdir, "zookeeper_data").getAbsoluteFile();
      File snapFile = new File(logdir, "zookeeper_snap").getAbsoluteFile();

      server = new ZooKeeperServer(
          new FileTxnSnapLog(dataFile, snapFile),
          new BasicDataTreeBuilder());

      connection = new NIOServerCnxnFactory();
      connection.configure(new InetSocketAddress(port), maxconnections);
      connection.startup(server);

      return connection.getLocalAddress();
    }

    private void stop() {
      if (connection != null) {
        connection.shutdown();
      }
    }
  }

  private static void usage() {
    String name = TestLog.class.getName();
    LOG.severe("Usage: " + name + " <zk> <quorum_size> <log_dir> <load_file>");
  }

  private static int system(String command)
      throws IOException, InterruptedException {
    Runtime r = Runtime.getRuntime();
    Process p = r.exec(command);
    p.waitFor();
    return p.exitValue();
  }

  private static void exit(int status) {
    if (zkserver != null) {
      zkserver.stop();
    }
    System.exit(status);
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      usage();
      exit(1);
    }

    // Get command line parameters.
    String zkurl = args[0];
    int quorum = Integer.parseInt(args[1]);
    String logdir = args[2];
    String loadfile = args.length > 3 ? args[3] : null;

    String servers = null;
    String znode = null;
    int local_replicas = 0;
    if (zkurl.equals("local")) {
      LOG.info("Starting a local ZooKeeper server");

      zkserver = new ZooKeeperTestServer(logdir);
      InetSocketAddress address = zkserver.start();
      servers = address.getHostName() + ":" + address.getPort();
      znode = "/log";
      local_replicas = (2*quorum) - 1; // Start all replicas locally.
    } else {
      servers = zkurl.substring(0, zkurl.indexOf("/"));
      znode = zkurl.substring(zkurl.indexOf("/"), zkurl.length());
      local_replicas = 1;

      LOG.info("Connecting to ZooKeeper server " + servers + znode);
    }

    long timeout = 3;
    List<Log> logs = new ArrayList<Log>();
    for (int replica = 1; replica <= local_replicas; replica++) {
      String log = logdir + "/log" + replica;

      String logtool = System.getenv("MESOS_LOG_TOOL");
      if (logtool != null) {
        // Initialize the log.
        // TODO(vinod): Do this via Java API once log tool has one.
        LOG.info("Initializing log " + log + " with " + logtool);
        int status = system(logtool + " initialize --path=" + log);
        if (status != 0) {
          LOG.severe("Error initializing log '" + log + "': " + status);
          exit(1);
        }
      } else {
        // TODO(vinod): Kill this once we don't care about log
        // version < 0.17.0.
        LOG.warning("Not initializing log file");
      }

      logs.add(new Log(quorum, log, servers, timeout, TimeUnit.SECONDS, znode));
    }

    // Write some data.
    if (loadfile != null) {
      LOG.info("Initializing writer");

      // First read the sizes.
      ArrayList<Integer> sizes = new ArrayList<Integer>(10000);
      try {
        BufferedReader in = new BufferedReader(new FileReader(loadfile));
        String str;
        while ((str = in.readLine()) != null) {
          sizes.add(Integer.parseInt(str));
        }
        in.close();
      } catch (IOException e) {
        LOG.severe("Error while reading from file: " + e.getMessage());
        exit(1);
      } catch (NumberFormatException e) {
        LOG.severe("Error while reading size from file: " + e.getMessage());
        exit(1);
      }

      // Now write the data of given sizes.
      int retries = 3;
      Log.Writer writer = new Log.Writer(
        logs.get(0), timeout, TimeUnit.SECONDS, retries);
      try {
        for(int size : sizes) {
          byte[] data = new byte[size];
          new Random().nextBytes(data); // Write random bytes.

          long startTime = System.nanoTime();
          writer.append(data, timeout, TimeUnit.SECONDS);
          long duration = System.nanoTime() - startTime;

          LOG.info("Time: " + System.currentTimeMillis() +
                   " Appended " + size + " bytes in " + duration + " ns");
        }
      } catch (TimeoutException e) {
        LOG.severe("Timed out writing to log: " + e.getMessage());
        exit(1);
      } catch (Log.WriterFailedException e) {
        LOG.severe("Error writing to log: " + e.getMessage());
        exit(1);
      }
    } else if (!zkurl.equals("local")) {
      // We are here if this is a remote replica that is not a writer.
      while(true) { // Wait until interrupted.
        try {
          LOG.info("Sleeping...Press Ctrl+C to exit");
          Thread.sleep(10000);
        } catch (InterruptedException e) {
          exit(1);
        }
      }
    } else {
      LOG.severe("Expecting load file in local mode");
      exit(1);
    }

    exit(0); // Success.
  }
}
