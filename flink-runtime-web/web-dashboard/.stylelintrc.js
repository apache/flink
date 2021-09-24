module.exports = {
  plugins: ['stylelint-order'],
  extends: ['stylelint-config-standard', 'stylelint-config-hudochenkov/order', 'stylelint-prettier/recommended'],
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
    'function-calc-no-invalid': null,
    'font-family-no-missing-generic-family-keyword': null,
    'selector-pseudo-element-no-unknown': [
      true,
      {
        ignorePseudoElements: ['ng-deep']
      }
    ]
  }
};
