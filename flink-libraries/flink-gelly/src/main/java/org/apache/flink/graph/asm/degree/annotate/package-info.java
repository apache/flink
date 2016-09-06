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

/**
 * The degree annotation package provides a collection of edge-centric graph
 * algorithms for counting the vertex degree of directed and undirected graphs.
 *
 * Undirected graphs have the property that for every vertex the in-degree is
 * equivalent to the out-degree.
 *
 * The undirected graph algorithms are:
 *   {@code VertexDegree}      annotates vertices as <v, deg(v)>
 *   {@code EdgeSourceDegree}  annotates edges as <s, t, (EV, deg(s))>
 *   {@code EdgeTargetDegree}  annotates edges as <s, t, (EV, deg(t))>
 *   {@code EdgeDegreePair}    annotates edges as <s, t, (EV, deg(s), deg(t))>
 *
 * The directed graph algorithms are:
 *   {@code VertexDegrees}     annotates vertices as <v, (deg(v), out(v), in(v))>
 *   {@code VertexOutDegree}   annotates vertices as <v, out(v)>
 *   {@code VertexInDegree}    annotates vertices as <v, in(v)>
 *   {@code EdgeSourceDegrees} annotates edges as <s, t, (deg(s), out(s), in(s))>
 *   {@code EdgeTargetDegrees} annotates edges as <s, t, (deg(t), out(t), in(t))>
 *   {@code EdgeDegreesPair}   annotates edges as <s, t, ((deg(s), out(s), in(s)), (deg(t), out(t), in(t)))>
 *
 * where:
 *   EV is the original edge value
 *   deg(x) is the number of vertex neighbors
 *   out(x) is the number of vertex neighbors connected by an out-edge
 *   in(x) is the number of vertex neighbors connected by an in-edge
 *
 * (out(x) + in(x)) / 2 <= deg(x) <= out(x) + in(x)
 */
package org.apache.flink.graph.asm.degree.annotate;
