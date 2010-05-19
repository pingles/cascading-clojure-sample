(ns cascading-clojure-sample.json
  (:use cascading.clojure.io)
  (:require [cascading.clojure.api :as c]))

(defn transform
  [input]
  [[(.toUpperCase (:name input)) (inc (get-in input [:age-data :age]))]])

(defn run
  []
  (with-log-level :info
    (let [source "./data/input.json" sink "./data/sink"]
      (let [trans (-> (c/pipe "j")
                    (c/map #'transform :< "input" :fn> "output"))
            flow (c/flow
                   {"j" (c/lfs-tap (c/json-line "input") source)}
                   (c/lfs-tap (c/json-line) sink)
                   trans)]
       (c/exec flow)))))
