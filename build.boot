(def +version+ "0.1.0-SNAPSHOT")

(set-env!
  :source-paths #{"src"}
  :dependencies '[[org.clojure/clojure      "1.8.0"]
                  [org.clojure/core.memoize "0.5.8" ]
                  [com.taoensso/carmine "2.12.2"]
                  [org.clojure/core.rrb-vector "0.0.11"]

                  [criterium "0.4.4"                          :scope "provided"]
                  [org.clojure/tools.cli "0.3.3"              :scope "provided"]
                  [org.clojure/test.check "0.9.0"             :scope "provided"]
                  [com.infolace/excel-templates "0.3.3"       :scope "provided"]

                  ;; boot
                  [boot/core                "2.5.1"           :scope "provided"]
                  [adzerk/bootlaces         "0.1.13"          :scope "test"]
                  [adzerk/boot-test         "1.0.6"           :scope "test"]])

(require '[adzerk.bootlaces :refer :all]
         '[adzerk.boot-test :as bt])

(bootlaces! +version+)

(deftask bench [a args ARGS str "bench arguments"]
  (set-env! :source-paths #(conj % "env/profiling"))
  (require 'hitchhiker.bench)

  (let [bench-it (resolve 'hitchhiker.bench/-main)]
    (apply bench-it (clojure.string/split args #" "))))
  
(task-options!
  pom {:project     'hitchhiker-tree
       :version     +version+
       :description "A Hitchhiker Tree Library"
       :url         "https://github.com/dgrnbrg/hitchhiker-tree"
       :scm         {:url "https://github.com/dgrnbrg/hitchhiker-tree"}
       :license     {"Eclipse Public License"
                     "http://www.eclipse.org/legal/epl-v10.html"}})
