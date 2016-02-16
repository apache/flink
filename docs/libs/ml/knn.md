---
mathjax: include
htmlTitle: FlinkML - k-nearest neighbors
title: <a href="../ml">FlinkML</a> - knn
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

* This will be replaced by the TOC
{:toc}

## Description
An exact k-nearest neighbors algorithm is given in KNN.scala.  To ease the brute-force computation of computing the distance between every traning point a quadtree is used.  The quadtree scales well in the number of training points, though poorly in the spatial dimension.  

##