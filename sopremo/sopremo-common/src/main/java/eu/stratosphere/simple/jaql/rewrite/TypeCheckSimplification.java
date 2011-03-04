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

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.TransformExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.schema.CheckFn;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

import static com.ibm.jaql.json.type.JsonType.*;

/**
 * ------------------------------------------------------------------------
 *     check(e1,e2)
 * ==>
 *     e1 -> transform check( elements(e2) )
 * 
 *   when e1 always produces an array and
 *        e2 produces a schema that accepts an array
 *     or e1 always produces an array or null and
 *        e2 produces schema  
 * 
 * ------------------------------------------------------------------------
 *   // TODO:  check(e1,e2) ==> e1 when schemaof(e1) is a subset of e2
 *   
 * ------------------------------------------------------------------------
 *   // TODO: check(check(e1,e2),e3) ==> check(e1, schemaIntersect(e2,e3))
 */
public class TypeCheckSimplification extends Rewrite
{
  /**
   * @param phase
   */
  public TypeCheckSimplification(RewritePhase phase)
  {
    super(phase, CheckFn.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr check)
  {
    Expr schemaExpr = check.child(1);
    if( !( schemaExpr instanceof ConstExpr ) )
    {
      return false;
    }
    
    Expr inExpr = check.child(0);
    Schema inSchema = inExpr.getSchema();
    if( inSchema.is(ARRAY).maybeNot() )
    {
      return false;
    }
    
    JsonSchema jschema = (JsonSchema)((ConstExpr)schemaExpr).value;
    Schema asSchema = jschema.get();
    
    asSchema = SchemaTransformation.restrictToArray(asSchema);
    if( asSchema == null )
    {
      return false;
    }

    // TODO: we could try to deal with a union of array schema, but such schema
    // should probably be transformed
    if( !(asSchema instanceof ArraySchema) )
    {
      return false; 
    }
    
    ArraySchema arraySchema = (ArraySchema)asSchema;
    if( arraySchema.getHeadSchemata().size() != 0 ||
        ! arraySchema.hasRest() )
    {
      return false;
    }
    
    Var var = engine.env.makeVar("$c", inSchema.elements());
    schemaExpr = new ConstExpr(new JsonSchema(asSchema.elements()));
    CheckFn echeck = new CheckFn(new VarExpr(var), schemaExpr);
    TransformExpr te = new TransformExpr(var, inExpr, echeck);
    check.replaceInParent(te);
    return true;
  }
}
