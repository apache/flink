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
    'selector-pseudo-element-no-unknown': [
      true,
      {
        ignorePseudoElements: ['ng-deep']
      }
    ]
  }
};
