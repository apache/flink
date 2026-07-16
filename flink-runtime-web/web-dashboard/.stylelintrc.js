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
module.exports = {
  plugins: ['stylelint-order'],
  extends: ['stylelint-config-standard', 'stylelint-config-hudochenkov/order', 'stylelint-prettier/recommended'],
  customSyntax: "postcss-less",
  rules: {
    'prettier/prettier': [
      true,
      {
        singleQuote: false
      }
    ],
    'no-empty-source': null,
    'no-descending-specificity': null,
    'no-invalid-position-at-import-rule': null,
    'font-family-no-missing-generic-family-keyword': null,
    'property-no-vendor-prefix': null,
    'alpha-value-notation': null,
    'color-function-notation': null,
    'declaration-property-value-no-unknown': null,
    'declaration-property-value-keyword-no-deprecated': null,
    'import-notation': 'string',
    'color-function-alias-notation': 'with-alpha',
    'selector-pseudo-element-no-unknown': [
      true,
      {
        ignorePseudoElements: ['ng-deep']
      }
    ]
  }
};
