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

import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.array.UnionFn;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.core.GroupByExpr;
import com.ibm.jaql.lang.expr.core.IfExpr;
import com.ibm.jaql.lang.expr.core.JoinExpr;
import com.ibm.jaql.lang.expr.core.NotExpr;
import com.ibm.jaql.lang.expr.core.OrExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.nil.IsnullExpr;
import com.ibm.jaql.lang.expr.nil.NullElementOnEmptyFn;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * 
 */
public class JoinToCogroup extends Rewrite
{
  /**
   * @param phase
   */
  public JoinToCogroup(RewritePhase phase)
  {
    super(phase, JoinExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    // join preserve? $i in ei1 on ei2($i),
    //      preserve? $j in ej1 on ej2($j),
    //       ...
    // return er($i,$j)
    //
    // group each $ in
    //     ei1 by $tg=ei2($) as $is
    //     ej2 by     ej2($) as $js,
    //     ...
    // expand
    //    for $i in $is | nullElementOnEmpty($is)
    //        $j in $js | nullElementOnEmpty($js),
    //        ...
    //    where not(isnull($tg))
    //    return er($ui, $uj)
    JoinExpr join = (JoinExpr) expr;

    int n = join.numBindings();
    assert n > 1;

    Env env = engine.env;
    Var inVar = env.makeVar("$join_in");
    Var byVar = env.makeVar("$join_on");
    Expr[] inputs = new Expr[n];
    Expr[] bys = new Expr[n];
    Var[] asVars = new Var[n];

    for (int i = 0; i < n; i++)
    {
      BindingExpr b = join.binding(i);
      asVars[i] = env.makeVar("$as_" + i);
      inputs[i] = b.inExpr();
      bys[i] = join.onExpr(i);
      bys[i].replaceVar(b.var, inVar);      
    }
    
    Expr joinCollect = join.collectExpr();

    int numPreserved = 0;
    for (int i = 0; i < n; i++)
    {
      BindingExpr joinBinding = join.binding(i);
      if (joinBinding.preserve)
      {
        numPreserved++;
      }
    }
    
    // The null-key group requires special handling:
    //   We return the union of all the preserved inputs (not the cross)
    //   We drop all the non-preserved inputs (this should be pused below the join/group).
    Expr onNullKey = null;
    if( numPreserved > 0 )
    {
      Expr[] toMerge = new Expr[numPreserved];
      Expr nilExpr = new ConstExpr(null);
      int k = 0;
      for (int i = 0; i < n; i++)
      {
        BindingExpr b = join.binding(i);
        if( b.preserve )
        {
          Var v = env.makeVar(b.var.name());
          Expr e = cloneExpr(joinCollect);
          e.replaceVar(b.var, v);
          for (int j = 0; j < n; j++)
          {
            if( i != j )
            {
              BindingExpr b2 = join.binding(j);
              e = e.replaceVarUses(b2.var, nilExpr, engine.varMap);
            }
          }
          e = new ForExpr(v, new VarExpr(asVars[i]), e);
          toMerge[k++] = e;
        }
      }
      if( numPreserved == 1 )
      {
        onNullKey = toMerge[0];
      }
      else
      {
        onNullKey = new UnionFn(toMerge);
      }
    }
    
    // Expr groupCollect = new ArrayExpr(joinCollect); // <<-- this is if join...expand is not support
    Expr groupCollect = joinCollect;
    Expr preservePlace = groupCollect;
    Expr preserveTest = null;
    for (int i = 0; i < n; i++)
    {
      BindingExpr b = join.binding(i);
      Expr inExpr = new VarExpr(asVars[i]);
      if( numPreserved > 0 ) 
      {
        if( numPreserved > 1 || !b.preserve )
        {
          inExpr = new NullElementOnEmptyFn(inExpr);
        }
        if( numPreserved > 1 && b.preserve )
        {
          Expr test = new NotExpr(new IsnullExpr(new VarExpr(b.var)));
          if( preserveTest == null )
          {
            preserveTest = test;
          }
          else
          {
            preserveTest = new OrExpr(preserveTest, test);
          }
        }
      }

      // groupRet = new ForExpr(i != 0, iterVar, null, inExpr, null, groupRet);
      groupCollect = new ForExpr(b.var, inExpr, groupCollect);
    }

    // drop the non-preserved results // TODO: can this be avoided if we order the preserved inputs first?
    if( numPreserved > 1 )
    {
      new IfExpr(preserveTest, preservePlace.injectAbove());
    }
    
    groupCollect = new IfExpr(new NotExpr(new IsnullExpr(new VarExpr(byVar))),
        groupCollect, onNullKey);

    BindingExpr inBinding = new BindingExpr(BindingExpr.Type.IN, inVar, null, inputs);
    BindingExpr byBinding = new BindingExpr(BindingExpr.Type.EQ, byVar, null, bys);
    Expr using = null; // TODO: take using from join
    Expr groupBy = new GroupByExpr(inBinding, byBinding, asVars, using, join.optionsExpr(), groupCollect);
    // groupBy = new UnnestExpr( groupBy );

    join.replaceInParent(groupBy);

    return true;
  }
}
