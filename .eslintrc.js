// 0 - turn the rule off
// 1 - turn the rule on as a warning (doesn't affect exit code)
// 2 - turn the rule on as an error (exit code will be 1)

module.exports = {
    extends: [
        'eslint-config-google',
    ],

    parserOptions: {
        ecmaVersion: 2018,
        sourceType: 'module',
    },

    rules: {
        'camelcase': 2,
        'indent': ['error', 4, {
            // How chaining and related is not consistently formatted in this application. But there are too many
            // instances of incorrectness to fix (2010)
            MemberExpression: 'off',
            // This primarily occurs because of our old style of defining component specs with strings in arrays. Once
            // those are all shifted to template literals, this can be enabled (127 issues)
            ArrayExpression: 'off',
            // Commented out code does not need to follow indenting standards
            ignoreComments: true,
        }],
        'arrow-parens': 0,

        // Disables from the old JSCS file, these may be changed later:
        'max-len': 0,
        'no-trailing-spaces': 0,
        'one-var': 0,
        'no-multi-str': 0, // This only works in ES5 browsers, but those are our lowest targets
        // I recommend these
        'no-return-assign': 0, // Allow arrow functions to return assignments (Applies to Node.js code only)
        'no-negated-condition': 0, // Allow normal negated conditions to be used e.g. a !== b

        // ------
        // IMPORTANT
        'no-warning-comments': 2, // No todo or related comments
        'no-console': 0, // This would help eliminate log verbosity. Needs to be overridden in folders (41 errors)
        'no-unused-vars': 2, // This is a bad to have in code, but causes too many errors right now (261 errors)
        'no-undef': 2, // The API models are put directly into the global namespace which causes this (1295 errors)
        'no-use-before-define': [2, { functions: false }], // Related to 'no-undef' (111 errors)
        'semi': 2, // Can cause confusing errors, but causes too many errors right now (48 errors)
        'no-implicit-coercion': 2, // This is bad, but there are too many errors (40 errors)
        'block-scoped-var': 0, // This can lead to very confusing errors, but fixing could generate app errs (18 errors)
        'eqeqeq': 2, // There is almost no good reason to use == instead of === (160 errors)
        // 'new-cap': 2,// This can cause strange context issues when not applied. Need to see if the instances are now
        // working with the strange behavior (and would error if changed) (8 errors)
        'max-nested-callbacks': [2, 6],
        'no-const-assign': 2,
        'no-dupe-keys': 2,
        'no-var': 0,// Prioritizing let/const is important, but there are still some valid use cases for var

        // MEDIUM
        'new-cap': [2, {
            'capIsNewExceptions': ['Snap']
        }],
        'max-statements-per-line': 2, // Having multiple statements per line is confusing, but tolerable (103 errors)
        'quote-props': 2, // Do not allow quotes for properties, debatable (52 errors)
        'no-unreachable': 2, // Should function declarations always come before returns? debatable (21 errors)
        'no-nested-ternary': 2, // Probably a good idea to disallow, but debatable (2 errors)
        'guard-for-in': 2, // Probably a good idea, but could screw up current code (6 errors)
        'operator-assignment': 0, // Probably a good idea, but it could be argued that not doing this is clearer.
        // Especially, for non-additive operators (6 errors)
        'one-var-declaration-per-line': 0, // This is very confusing, but seems to be a standard in some places. So it
        'comma-dangle': 0, // Allow commas at the end of object literals and arrays
        'handle-callback-err': 2,
        'valid-jsdoc': 0,// Set to 2 to see all the errors across our system
        'prefer-promise-reject-errors': 0, // Promise rejects should only be given Error objects

        // LOW PRIORITY
        // spacing
        'no-useless-escape': 0, // This error only occurs remotely on CodeShip (which is confusing)
        'padded-blocks': 0, // Issues of blocks have things link 2 empty lines in them
        'no-multiple-empty-lines': 0, // Related to the above
        'comma-spacing': 0, // Space before comma issue
        'object-curly-spacing': 0, // Spacing issues around blocks
        'no-multi-spaces': 0, // Multiple spacing issue before blocks
        'keyword-spacing': 0, // If should have spaces after them
        'key-spacing': 0, // If keys in object literals should have spaces after them
        'semi-spacing': 0, // For and while semi-colon spacing
        'space-before-function-paren': [2, {
            "anonymous": "never",
            "named": "never",
            "asyncArrow": "always"
        }],
        'space-before-blocks': 0, // Opening brace space
        'generator-star-spacing': [2, {before: false, after: true}],// function* myFunction() is the only valid generator syntax
        // other
        'no-else-return': 0, // Not conforming to this rule results in verbose code, but not a big deal
        'require-jsdoc': 0, // There is currently no JSDoc commenting standard agreed upon yet
        'spaced-comment': 0, // Allow comments with more than 2 /, e.g. top of gulpfile.js. Related to above (447 errors)
        'no-extra-semi': 0, // Deal with this will make the code easier to read, but it is not a big deal
        'brace-style': 0, // Not allowing one line braces is debateable
        'no-loop-func': 0, // No functions within loops, this is debateable
        'default-case': 0, // I don't see why switch statements need default cases
        'quotes': 0,// Should single/double quotes be consistent?
        'prefer-const': 0,// Prefer const over let which is our policy. But it is inconsitently applied right now.
    },

    globals: {
        'lodash': true,
        //testing keywords
        'afterEach': true,
        'beforeAll': true,
        'afterAll': true,
        'beforeEach': true,
        'describe': true,
        'xdescribe': true,
        'expect': true,
        'fail': true,
        'it': true,
        'xit': true,
        'inject': true,
        'spyOn': true,
        'componentTests': true,
        'zTest': true,
        'jasmine': true,
        'Promise': true,

        'Set': true,
        'Map': true,
        'Uint8Array': true,
    },

    env: {
        'node': true
    },
};
