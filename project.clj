(defproject hitchhiker-tree "0.1.0-SNAPSHOT"
  :description "A Hitchhiker Tree Library"
  :url "https://github.com/dgrnbrg/hitchhiker-tree"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.memoize "0.5.8"]
                 [com.taoensso/carmine "2.12.2"]
                 [org.clojure/core.rrb-vector "0.0.11"]
                 [org.clojure/core.cache "0.6.5"]
                 [org.clojure/java.jdbc "0.6.2-alpha2"]
                 [org.xerial/sqlite-jdbc "3.7.2"]]
  :aliases {"bench" ["with-profile" "profiling" "run" "-m" "hitchhiker.bench"]}
  :jvm-opts ["-server" "-Xmx3700m" "-Xms3700m"]
  :profiles {:test
             {:dependencies [[org.clojure/test.check "0.9.0"]]}
             :profiling
             {:main hitchhiker.bench
              :source-paths ["env/profiling"]
              :dependencies [[criterium "0.4.4"]
                             [org.clojure/tools.cli "0.3.3"]
                             [org.clojure/test.check "0.9.0"]
                             [com.infolace/excel-templates "0.3.3"]]}})
