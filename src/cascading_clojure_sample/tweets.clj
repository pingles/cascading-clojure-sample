(ns cascading-clojure-sample.tweets
  (:use clojure.contrib.command-line)
  (:require [cascading.clojure.api :as c]
            [cascading.clojure.io :as io])
  (:gen-class))

(defn transform
  [input]
  [(:text input)])

(defn tagged-json
  [start-tag end-tag & [field-name]]
  (c/tagged-input start-tag
                  end-tag
                  io/decode-json
                  io/encode-json
                  field-name))

(defn run
  [source sink]
  (io/with-log-level :info
    (let [trans (-> (c/pipe "j")
                    (c/map #'transform :< "input" :fn> "output"))
          flow (c/flow {"j" (c/hfs-tap (tagged-json "{\"place\"" "}}" "input") source)}
                       (c/hfs-tap (c/json-line) sink)
                       trans)]
      (c/exec flow))))

(defn -main
  [& args]
  (with-command-line args
    "cascading-clojure twitter demo"
    [[input "Input path"]
     [output "Output directory"]]
    (run input output)))
