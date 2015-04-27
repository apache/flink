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
package org.apache.flink.api.table.trees

/**
 * Base class for a rule that is part of an [[Analyzer]] rule chain. Method `rule` gets a tree
 * and must return a tree. The returned tree can also be the input tree. In an [[Analyzer]]
 * rule chain the result tree of one [[Rule]] is fed into the next [[Rule]] in the chain.
 *
 * A [[Rule]] is repeatedly applied to a tree until the tree does not change between
 * rule applications.
 */
abstract class Rule[A <: TreeNode[A]] {
  def apply(expr: A): A
}
