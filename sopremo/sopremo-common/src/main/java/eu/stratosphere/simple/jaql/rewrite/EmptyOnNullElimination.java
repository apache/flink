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

import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.nil.EmptyOnNullFn;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

import static com.ibm.jaql.json.type.JsonType.*;

/**
 * emptyOnNull( e ) ==> e  if e is never null
 */
public class EmptyOnNullElimination extends Rewrite
{
  /**
   * @param phase
   */
  public EmptyOnNullElimination(RewritePhase phase)
  {
    super(phase, EmptyOnNullFn.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    assert expr instanceof EmptyOnNullFn;
    Expr input = expr.child(0);
    if( input.getSchema().is(NULL).never() )
    {
      expr.replaceInParent(input);
      return true;
    }
    // TODO: ask parent if it treats null as empty.
    return false;
  }
}
