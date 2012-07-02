/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.iterative.concurrent;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.fail;

public class SuperstepBarrierTest {

  @Test
  public void singleRounds() throws InterruptedException {
    Random random = new Random();

    for (int n = 0; n < 10; n++) {
      int numWorkers = random.nextInt(10) + 3;
      int numLocalWorkers = random.nextInt(numWorkers);
      singleRound(numWorkers, numLocalWorkers);
    }
  }

  public void singleRound(int numWorkers, int numLocalWorkers) throws InterruptedException {

    SuperstepBarrier barrier = new SuperstepBarrier(numWorkers);

    Thread signaler = new Thread(new Signaler(barrier, numWorkers));
    Thread[] workers = new Thread[numLocalWorkers];
    for (int n = 0; n < numLocalWorkers; n++) {
      workers[n] = new Thread(new LocalWorker(n, barrier));
    }

    signaler.start();
    for (int n = 0; n < numLocalWorkers; n++) {
      workers[n].start();
    }


    for (int n = 0; n < numLocalWorkers; n++) {
      workers[n].join();
    }
    signaler.join();
  }

  static class Signaler implements Runnable {

    private final SuperstepBarrier barrier;
    private final int numSignals;
    private final Random random;

    Signaler(SuperstepBarrier barrier, int numSignals) {
      this.barrier = barrier;
      this.numSignals = numSignals;
      random = new Random();
    }

    @Override
    public void run() {
      try {
        for (int n = 0; n < numSignals; n++) {
          Thread.sleep(random.nextInt(20) * n);
          System.out.println("Sending signal " + n);
          barrier.signalWorkerDone();
        }
      } catch (InterruptedException e) {
        fail();
      }
    }
  }

  static class LocalWorker implements Runnable {

    private final SuperstepBarrier barrier;
    private final int number;
    private final Random random;

    LocalWorker(int number, SuperstepBarrier barrier) {
      this.number = number;
      this.barrier = barrier;
      random = new Random();
    }

    @Override
    public void run() {
      try {
        Thread.sleep(random.nextInt(20));
        System.out.println("Worker " + number + " waits for others");
        barrier.waitForOthers();
        System.out.println("Worker " + number + " continues");
      } catch (InterruptedException e) {
        fail();
      }
    }
  }

}
