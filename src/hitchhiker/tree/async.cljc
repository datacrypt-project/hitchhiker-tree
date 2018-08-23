(ns hitchhiker.tree.async)

;; rebind this *before* loading any other
;; hh-tree namespace, so it has effect at
;; macro-expansion time
(def ^:dynamic *async-backend* :none)
