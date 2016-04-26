(defproject hitchhiker-tree "0.1.0-SNAPSHOT"
  :description "A Hitchhiker Tree Library"
  :url "https://github.com/dgrnbrg/hitchhiker-tree"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.memoize "0.5.8"]
                 [org.clojure/test.check "0.9.0"]]
  :profiles {:profiling
             {:main hitchhiker.bench
              :source-paths ["env/profiling"]
              :dependencies [[criterium "0.4.4"]
                             [org.clojure/tools.cli "0.3.3"]
                             [com.infolace/excel-templates "0.3.3"]]}})
