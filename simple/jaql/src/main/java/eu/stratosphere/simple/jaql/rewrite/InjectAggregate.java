/*
 * Copyright (C) IBM Corp. 2009.
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

import com.ibm.jaql.lang.expr.core.AggregateFullExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.GroupByExpr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

// TODO: This rewrite possibly go away with the change in FOR definition to preserve input.

/**
 * e1 -> group ... expand as e2($) 
 * ==> e1 -> group ... expand ($ -> aggregate each $i e3($i))
 * where every use of $ in e2 is:
 *      aggfn( $ )
 *      aggfn( for( $j in $ ) e4 )
 *      aggfn( $ -> transform e4 )
 *      // TODO: should be aggfn( anyIteratingUse($) )
 * becomes in e3:
 *      aggfn( $i )
 *      aggfn( for( $j in $i ) e4 )
 *      aggfn( $i -> transform e4 )
 */
public class InjectAggregate extends Rewrite
{
  /**
   * @param phase
   */
  public InjectAggregate(RewritePhase phase)
  {
    super(phase, GroupByExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    GroupByExpr g = (GroupByExpr)expr;
    if( g.numInputs() != 1 )
    {
      // Don't try this with co-group.  
      // TODO: aggregating co-group needs work. 
      return false;
    }

    Expr aggExpr = AggregateFullExpr.makeIfAggregating(
        engine.env, g.getAsVar(0), null, g.collectExpr(), 
        engine.exprList, engine.aggList, false);
    
    if( aggExpr == null )
    {
      // We couldn't inject AggregateFull.
      return false;
    }
    
    g.setCollectExpr(aggExpr);
    return true;
  }
}
