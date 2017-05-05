#!/usr/local/bin/node

// have a look at the datascript test runner also

var fs = require('fs'),
    vm = require('vm');

global.goog = {};

global.CLOSURE_IMPORT_SCRIPT = function(src) {
  require('./target/none/goog/' + src);
  return true;
};

function nodeGlobalRequire(file) {
  vm.runInThisContext.call(global, fs.readFileSync(file), file);
}


nodeGlobalRequire('./target/none/goog/base.js');
nodeGlobalRequire('./target/none/cljs_deps.js');
goog.require('hitchhiker.konserve_test');


hitchhiker.konserve_test.test_all();

