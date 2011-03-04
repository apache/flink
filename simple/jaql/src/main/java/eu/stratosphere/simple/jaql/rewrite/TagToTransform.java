/*
 * Copyright (C) IBM Corp. 2010.
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
import com.ibm.jaql.lang.expr.core.TagFn;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * Expand tagSplit a write to a composite file:
 *    e1 -> tag(i)
 * ==>
 *    e1 -> transform [i,$]
 */
public class TagToTransform extends Rewrite
{
  public TagToTransform(RewritePhase phase)
  {
    super(phase, TagFn.class);
  }
  
  @Override
  public boolean rewrite(Expr tag)
  {
    ((TagFn)tag).expand(engine.env);
    return true;
  }
}
