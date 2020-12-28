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

package org.apache.flink.examples.java.ml.util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.examples.java.ml.LinearRegression.Data;
import org.apache.flink.examples.java.ml.LinearRegression.Params;

import java.util.LinkedList;
import java.util.List;

/**
 * Provides the default data sets used for the Linear Regression example program. The default data
 * sets are used, if no parameters are given to the program.
 */
public class LinearRegressionData {

    // We have the data as object arrays so that we can also generate Scala Data
    // Sources from it.
    public static final Object[][] PARAMS = new Object[][] {new Object[] {0.0, 0.0}};

    public static final Object[][] DATA =
            new Object[][] {
                new Object[] {0.5, 1.0}, new Object[] {1.0, 2.0},
                new Object[] {2.0, 4.0}, new Object[] {3.0, 6.0},
                new Object[] {4.0, 8.0}, new Object[] {5.0, 10.0},
                new Object[] {6.0, 12.0}, new Object[] {7.0, 14.0},
                new Object[] {8.0, 16.0}, new Object[] {9.0, 18.0},
                new Object[] {10.0, 20.0}, new Object[] {-0.08, -0.16},
                new Object[] {0.13, 0.26}, new Object[] {-1.17, -2.35},
                new Object[] {1.72, 3.45}, new Object[] {1.70, 3.41},
                new Object[] {1.20, 2.41}, new Object[] {-0.59, -1.18},
                new Object[] {0.28, 0.57}, new Object[] {1.65, 3.30},
                new Object[] {-0.55, -1.08}
            };

    public static DataSet<Params> getDefaultParamsDataSet(ExecutionEnvironment env) {
        List<Params> paramsList = new LinkedList<>();
        for (Object[] params : PARAMS) {
            paramsList.add(new Params((Double) params[0], (Double) params[1]));
        }
        return env.fromCollection(paramsList);
    }

    public static DataSet<Data> getDefaultDataDataSet(ExecutionEnvironment env) {
        List<Data> dataList = new LinkedList<>();
        for (Object[] data : DATA) {
            dataList.add(new Data((Double) data[0], (Double) data[1]));
        }
        return env.fromCollection(dataList);
    }
}
