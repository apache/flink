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

package org.apache.flink.examples.java.misc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * Eratosthenes Sieve by   E.I.Sarmas (github.com/dsnz)   2017-01-17
 *
 * This example shows how to implement a classic prime sieve (Eratosthenes).
 * 
 * The implementation is a basic demo, and can/should be optimized very easily
 * to at least skip even numbers and even further to do segmented sieving.
 *
 * Some leftover code is from an attempt to use POJO 'SieveEntry'
 * but DeltaIteration promotes use of Tuple(s).
 *
 * The JoinHint is just an experiment, not mandatory.
 * 
 */

/*
  DeltaIteration works with Tuples only :((
*/
public class Sieve {
  
  public static void main(String[] args) throws Exception {


		final int sieve_size = args.length > 0 ? Integer.parseInt(args[0]) : 100;
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    
    DataSet<Long> numbers = env.generateSequence(1, sieve_size);
    DataSet<Tuple2<Long, Boolean>> sieve = numbers
      .map(new MapFunction<Long, Tuple2<Long, Boolean>>() {
        @Override
        public Tuple2<Long, Boolean> map(Long value) { return new Tuple2<Long, Boolean>(value, true); }
      });
    numbers = null;
    sieve.print();
    
    long max_sieve_number = (long) Math.sqrt(sieve_size);
    // DataSet<Long> workset = env.generateSequence(1, (long) Math.sqrt(sieve_size));
    DataSet<Long> workset = env.fromElements(new Long(2));
    
    /*
    DataSet<SieveEntry> new_sieve = sieve.leftOuterJoin(crosstab).where("number")
      .equalTo(new KeySelector<Long, Long>() {
        @Override
        public Long getKey(Long a) { return a; }
      })
      .with(new JoinFunction<SieveEntry, Long, SieveEntry>(){
        @Override
        public SieveEntry join(SieveEntry entry1, Long cross_number) {
          return new SieveEntry(entry1.number, (entry1.number == cross_number) ? false : true, false);
        }
      });
    new_sieve.print();
    */
    
    DeltaIteration<Tuple2<Long, Boolean>, Long> iteration = sieve
      .iterateDelta(workset, sieve_size, 0);
    
    DataSet<Long> crosstab1 = iteration.getWorkset()
      .flatMap(new FlatMapFunction<Long, Long>() {
        @Override
        public void flatMap(Long value, Collector<Long> out) {
          Long c = value;
          long i = c;
          while (i <= sieve_size) {
            i += c;
            out.collect(i);
          }
        }
      });
    //crosstab1.print();
    
    DataSet<Tuple2<Long, Boolean>> crosstab = env.generateSequence(1, sieve_size)
      .leftOuterJoin(crosstab1, JoinHint.REPARTITION_SORT_MERGE)
      .where(new KeySelector<Long, Long>() {
        @Override
        public Long getKey(Long a) { return a; }
      })
      .equalTo(new KeySelector<Long, Long>() {
        @Override
        public Long getKey(Long a) { return a; }
      })
      .with(new JoinFunction<Long, Long, Tuple2<Long, Boolean>>(){
        @Override
        public Tuple2<Long, Boolean> join(Long entry1, Long cross_number) {
          return new Tuple2<Long, Boolean>(entry1, (entry1 == cross_number) ? true : false);
        }
      });      
    //crosstab.print();
    
    // damn ! only Join and CoGroup for solutionSet !!!
    DataSet<Tuple2<Long, Boolean>> new_sieve =  iteration.getSolutionSet()
      .join(crosstab, JoinHint.REPARTITION_SORT_MERGE)
      .where(0)
      .equalTo(0)
      .with(new JoinFunction<Tuple2<Long, Boolean>, Tuple2<Long, Boolean>, Tuple2<Long, Boolean>>(){
        @Override
        public Tuple2<Long, Boolean> join(Tuple2<Long, Boolean> entry1, Tuple2<Long, Boolean> cross_entry) {
          return new Tuple2<Long, Boolean>(entry1.f0, (cross_entry.f1) ? false : entry1.f1);
        }
      });
    //new_sieve.print();
      
    DataSet<Long> new_workset = iteration.getWorkset()
      .flatMap(new FlatMapFunction<Long, Long>() {
        @Override
        public void flatMap(Long value, Collector<Long> out) {
          long x = value + 1;
          if (x <= max_sieve_number) {
            out.collect(x);
          }
        }
      });
    
    DataSet<Tuple2<Long, Boolean>> final_sieve = iteration.closeWith(new_sieve, new_workset);
    final_sieve.sortPartition(0, Order.ASCENDING).print();
  }

  public static class SieveEntry {

    public Long number;
    public boolean is_prime;
    public boolean last_prime;

    public SieveEntry() {}

    public SieveEntry(Long number_, boolean is_prime_, boolean last_prime_) {
      number = number_;
      is_prime = is_prime_;
      last_prime = last_prime_;
    }

    @Override
    public String toString() {
      return number.toString() + (is_prime ? " Prime" : "");
    }
  }
}
