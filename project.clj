(defproject hitchhiker-tree "0.1.0-SNAPSHOT"
  :description "A Hitchhiker Tree Library"
  :url "https://github.com/dgrnbrg/hitchhiker-tree"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.memoize "0.5.8"]
                 [com.taoensso/carmine "2.12.2"]
                 [org.clojure/core.rrb-vector "0.0.11"]
                 [org.clojure/core.cache "0.6.5"]

                 [io.replikativ/incognito "0.2.2-SNAPSHOT"]
                 [io.replikativ/konserve "0.4.8"]]
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
                             [com.infolace/excel-templates "0.3.3"]]}
             :dev {:dependencies [[binaryage/devtools "0.8.2"]
                                  [figwheel-sidecar "0.5.8"]
                                  [com.cemerick/piggieback "0.2.1"]
                                  [org.clojure/test.check "0.9.0"]]
                   :source-paths ["src" "dev"]
                   ;; need to add dev source path here to get user.clj loaded
                   ;; TODO check whether this still makes sense,
                   ;; workaround for cider cljs REPL
                   #_:figwheel #_{:nrepl-port 7888
                              :nrepl-middleware ["cider.nrepl/cider-middleware"
                                                 "cemerick.piggieback/wrap-cljs-repl"]}
                   :plugins [[lein-figwheel "0.5.8"]
                             #_[cider/cider-nrepl "0.15.0-SNAPSHOT"]]
                   :repl-options {; for nREPL dev you really need to limit output
                                  :init (set! *print-length* 50)
                                  :nrepl-middleware [cemerick.piggieback/wrap-cljs-repl]}}}
  :clean-targets ^{:protect false} ["resources/public/js/compiled" "target"]


  :cljsbuild {:builds
              [{:id "dev"
                :figwheel true
                :source-paths ["src"]
                :compiler {:main hitchhiker.tree.core
                           :asset-path "js/out"
                           :output-to "resources/public/js/core.js"
                           :output-dir "resources/public/js/out" }}]}

  :plugins [[lein-figwheel "0.5.8"]
            [lein-cljsbuild "1.1.4" :exclusions [[org.clojure/clojure]]]])
