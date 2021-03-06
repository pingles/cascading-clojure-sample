(ns cascading-clojure-sample.wordcount
  (:import
    cascading.tuple.Fields)
  (:require
    [cascading.clojure.api :as c]
    [cascading.clojure.io :as io]))


(defn tokenize
  [input]
  (reduce (fn [m,i] (conj m [i 1])) [] (.split input " ")))

(def sum (c/agg + 0))

  
(defn plain
  [word cnt]
  (str word ": " cnt))
  
(defn run
  []
  (io/with-log-level :debug
    (let [pipe (->
                  (c/pipe "word-lines")
                  (c/mapcat #'tokenize :< "line" :fn> ["word" "subcount"])
                  (c/group-by "word")
                  (c/aggregate #'sum :< "subcount" :fn> "count" :> ["word" "count"])
                  (c/group-by "count" "count" true)
                  (c/map #'plain :< ["word" "count"] :fn> "output"))
          flow (c/flow
                  {"word-lines" (c/lfs-tap (c/text-line "line") "./data/small-war.txt")}
                  (c/lfs-tap (c/clojure-line) "./data/wordsink")
                  pipe)]
     (c/exec flow))))
