(ns cascading-clojure-sample.tweets
  (:use clojure.contrib.command-line)
  (:import [org.codehaus.jackson JsonParseException])
  (:require [cascading.clojure.api :as c]
            [cascading.clojure.io :as io])
  (:gen-class))

(defn transform
  [input]
  [(:text input)])

(defn safe-decode-json
  "Handles cases when records can't be decoded."
  [obj]
  (try (io/decode-json obj)
       (catch JsonParseException e
         {})))

(defn tagged-json
  [start-tag end-tag & [field-name]]
  (c/tagged-input start-tag
                  end-tag
                  safe-decode-json
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
