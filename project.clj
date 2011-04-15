(defproject cascading-clojure-sample "1.0.0-SNAPSHOT"
  :aot [cascading-clojure-sample.json
        cascading-clojure-sample.wordcount
        cascading-clojure-sample.tweets]
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [cascading-clojure "1.0.0-SNAPSHOT"]
                 [http.async.client "0.2.2"]]
  :dev-dependencies [[lein-hadoop "1.0.0"]
                     [swank-clojure "1.3.0"]])
