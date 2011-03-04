/*
 * Copyright (C) IBM Corp. 2008.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package eu.stratosphere.simple.jaql.rewrite;

import com.ibm.jaql.lang.expr.core.Expr;

/**
 * 
 */
public class Segment
{
  static enum Type
  {
    SEQUENTIAL, 
    MAP, 
    INLINE_MAP,        // included with a parent GROUP or COMBINE
    GROUP, 
    COMBINE, 
    MAPREDUCE,
    MAP_GROUP,         // per group item: no children
    COMBINE_GROUP,     // combine of map group items: exactly one map_group child
    FINAL_GROUP,       // reduce of combines: 1 or more combine_group children
    SEQUENTIAL_GROUP   // run each group holisticly: no children
  };

  Type        type = Type.SEQUENTIAL;
  Expr        root;
  Segment     firstChild;
  Segment     nextSibling;
  public Expr primaryExpr;

  /**
   * @param type
   */
  public Segment(Type type)
  {
    this.type = type;
  }

  /**
   * @param type
   * @param seg
   */
  public Segment(Type type, Segment seg)
  {
    this.type = type;
    addChild(seg);
  }

  /**
   * @param child
   */
  public void addChild(Segment child)
  {
    if (type == Type.SEQUENTIAL && child.type == Type.SEQUENTIAL)
    {
      // merge sequential segments
      child = child.firstChild;
      if (child == null)
      {
        return;
      }
    }
    if (firstChild == null)
    {
      firstChild = child;
    }
    else
    {
      Segment prev = firstChild;
      Segment cur = firstChild.nextSibling;
      while (cur != null)
      {
        prev = cur;
        cur = cur.nextSibling;
      }
      prev.nextSibling = child;
    }
  }

  /**
   * @param s
   * @return
   */
  public static Segment sequential(Segment s)
  {
    if (s.type != Segment.Type.SEQUENTIAL)
    {
      s = new Segment(Segment.Type.SEQUENTIAL, s);
    }
    return s;
  }

  /**
   * 
   */
  public void mergeSequential()
  {
    if (type != Type.SEQUENTIAL)
    {
      return;
    }
    while (firstChild != null && firstChild.type == Segment.Type.SEQUENTIAL)
    {
      firstChild = firstChild.nextSibling;
    }
    Segment prev = firstChild;
    for (Segment s = firstChild; s != null; s = s.nextSibling)
    {
      if (s.type == Segment.Type.SEQUENTIAL) // guaranteed false on first iteration
      {
        prev.nextSibling = s.nextSibling;
      }
      else
      {
        prev = s;
      }
    }
  }
}
