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
 *   {@code VertexDegree}     annotates vertices as <v, deg(v)>
 *   {@code EdgeSourceDegree} annotates edges as <s, t, deg(s)>
 *   {@code EdgeTargetDegree} annotates edges as <s, t, deg(t)>
 *   {@code EdgeDegreePair}   annotates edges as <s, t, (deg(s), deg(t))>
 *
 * The directed graph algorithms are:
 *   {@code VertexOutDegree}  annotates vertices as <v, out(v)>
 *   {@code VertexInDegree}   annotates vertices as <v, in(v)>
 *   {@code VertexDegreePair} annotates vertices as <v, (out(v), in(v))>
 *
 * A directed graph edge has four possible degrees: source out- and in-degree
 * and target out- and in-degree. This gives 2^4 - 1 = 15 ways to annotate
 * a directed edge.
 */
package org.apache.flink.graph.asm.degree.annotate;
