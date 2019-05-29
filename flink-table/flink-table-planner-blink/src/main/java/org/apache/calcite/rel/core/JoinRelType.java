/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.core;

import org.apache.calcite.linq4j.CorrelateJoinType;

import java.util.Locale;

/**
 * This class is copied from https://github.com/apache/calcite/pull/1157 to supports SEMI/ANTI join.
 * NOTES: This file should be deleted when upgrading to a new calcite version
 * which contains CALCITE-2969.
 */

/**
 * Enumeration of join types.
 */
public enum JoinRelType {
  /**
   * Inner join.
   */
  INNER,

  /**
   * Left-outer join.
   */
  LEFT,

  /**
   * Right-outer join.
   */
  RIGHT,

  /**
   * Full-outer join.
   */
  FULL,

  /**
   * Semi-join.
   *
   * <p>For example, {@code EMP semi-join DEPT} finds all {@code EMP} records
   * that have a corresponding {@code DEPT} record:
   *
   * <blockquote><pre>
   * SELECT * FROM EMP
   * WHERE EXISTS (SELECT 1 FROM DEPT
   *     WHERE DEPT.DEPTNO = EMP.DEPTNO)</pre>
   * </blockquote>
   */
  SEMI,

  /**
   * Anti-join.
   *
   * <p>For example, {@code EMP anti-join DEPT} finds all {@code EMP} records
   * that do not have a corresponding {@code DEPT} record:
   *
   * <blockquote><pre>
   * SELECT * FROM EMP
   * WHERE NOT EXISTS (SELECT 1 FROM DEPT
   *     WHERE DEPT.DEPTNO = EMP.DEPTNO)</pre>
   * </blockquote>
   */
  ANTI;

  /** Lower-case name. */
  public final String lowerName = name().toLowerCase(Locale.ROOT);

  /**
   * Returns whether a join of this type may generate NULL values on the
   * right-hand side.
   */
  public boolean generatesNullsOnRight() {
    return (this == LEFT) || (this == FULL);
  }

  /**
   * Returns whether a join of this type may generate NULL values on the
   * left-hand side.
   */
  public boolean generatesNullsOnLeft() {
    return (this == RIGHT) || (this == FULL);
  }

  /**
   * Swaps left to right, and vice versa.
   */
  public JoinRelType swap() {
    switch (this) {
    case LEFT:
      return RIGHT;
    case RIGHT:
      return LEFT;
    default:
      return this;
    }
  }

  /** Returns whether this join type generates nulls on side #{@code i}. */
  public boolean generatesNullsOn(int i) {
    switch (i) {
    case 0:
      return generatesNullsOnLeft();
    case 1:
      return generatesNullsOnRight();
    default:
      throw new IllegalArgumentException("invalid: " + i);
    }
  }

  /** Returns a join type similar to this but that does not generate nulls on
   * the left. */
  public JoinRelType cancelNullsOnLeft() {
    switch (this) {
    case RIGHT:
      return INNER;
    case FULL:
      return LEFT;
    default:
      return this;
    }
  }

  /** Returns a join type similar to this but that does not generate nulls on
   * the right. */
  public JoinRelType cancelNullsOnRight() {
    switch (this) {
    case LEFT:
      return INNER;
    case FULL:
      return RIGHT;
    default:
      return this;
    }
  }

  /** Transform this JoinRelType to CorrelateJoinType. **/
  public CorrelateJoinType toLinq4j() {
    switch (this) {
    case INNER:
      return CorrelateJoinType.INNER;
    case LEFT:
      return CorrelateJoinType.LEFT;
    case SEMI:
      return CorrelateJoinType.SEMI;
    case ANTI:
      return CorrelateJoinType.ANTI;
    }
    throw new IllegalStateException(
        "Unable to convert " + this + " to CorrelateJoinType");
  }

  public boolean projectsRight() {
    switch (this) {
    case INNER:
    case LEFT:
    case RIGHT:
    case FULL:
      return true;
    case SEMI:
    case ANTI:
      return false;
    }
    throw new IllegalStateException(
        "Unable to convert " + this + " to JoinRelType");
  }
}

// End JoinRelType.java
