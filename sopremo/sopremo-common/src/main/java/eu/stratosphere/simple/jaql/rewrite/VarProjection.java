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

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.RecordExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.path.PathExpr;
import com.ibm.jaql.lang.expr.path.PathFieldValue;
import com.ibm.jaql.lang.expr.path.PathReturn;
import com.ibm.jaql.lang.expr.path.PathStep;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * If we can find the expression that defines a field, make it a common subexpression
 * and eliminate the path step.
 * 
 * ( ...,
 *   $v = {...,field:e,...},
 *   ... $v.field ... )
 * ==> 
 * ( ...,
 *   $field = e,
 *   $v = {...,field:$field,...},
 *   ... $field ... )
 *   
 */
public class VarProjection extends Rewrite
{
  /**
   * @param phase
   */
  public VarProjection(RewritePhase phase)
  {
    super(phase, PathExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr) throws Exception
  {
    // TODO: do the same for indexes?
    // TODO: Do the same for constant records? Probably not needed because we inline the record anyway.
    PathExpr pathExpr = (PathExpr)expr;
    
    // Path starts with a variable
    Expr e = pathExpr.input();
    if( !(e instanceof VarExpr) )
    {
      return false;
    }
    VarExpr varExpr = (VarExpr)e;
    
    // Variable is defined in a DoExpr
    BindingExpr bind = varExpr.findVarDef();
    if( !(bind.parent() instanceof DoExpr) )
    {
      return false;
    }
    
    // Variable is bound to a record constructor
    e = bind.eqExpr();
    if( !(e instanceof RecordExpr) )
    {
      return false;
    }
    RecordExpr rec = (RecordExpr)e;

    // First path step is a constant field access
    PathStep s = pathExpr.firstStep();
    if( !(s instanceof PathFieldValue) )
    {
      return false;
    }
    PathFieldValue pf = (PathFieldValue)s;
    e = pf.nameExpr();
    if( !(e instanceof ConstExpr) )
    {
      return false;
    }
    ConstExpr ce = (ConstExpr)e;
    JsonString field = (JsonString)ce.value;
    
    // Try to find the field's definition
    Expr fieldValue = rec.findStaticFieldValue(field);
    if( fieldValue == null )
    {
      return false;
    }
    
    // We can rewrite!
    Var var = engine.env.makeVar(field.toString());
    fieldValue.replaceInParent(new VarExpr(var));
    BindingExpr bind2 = new BindingExpr(BindingExpr.Type.EQ, var, null, fieldValue);
    int slot = bind.getChildSlot();
    bind.parent().addChildBefore(slot, bind2);
    PathStep nextStep = pf.nextStep();
    if( nextStep instanceof PathReturn )
    {
      pathExpr.replaceInParent(new VarExpr(var));
    }
    else
    {
      pathExpr.setChild(0, new VarExpr(var));
      pathExpr.setChild(1, nextStep);
    }
    return true;
  }
}
