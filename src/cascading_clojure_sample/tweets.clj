(ns cascading-clojure-sample.tweets
  (:use clojure.contrib.command-line)
  (:import [org.codehaus.jackson JsonParseException])
  (:require [cascading.clojure.api :as c]
            [clojure.walk :as walk]
            [http.async.client :as h]
            [cascading.clojure.io :as io]
            [clj-json.core :as json]
            [clojure.contrib.string :as s])
  (:gen-class))

(defn safe-decode-json
  "Handles cases when records can't be decoded."
  [obj]
  (try (io/decode-json obj)
       (catch JsonParseException e
         (do (println "ERROR Processing" obj)
             {}))))

(defn lookup-spotify
  "Uses the api to look up entity metadata. If a 403 response is returned the request will be retried after 1 second"
  [track]
  (loop [url (format "http://ws.spotify.com/lookup/1/.json?uri=spotify:track:%s"
                     track)
         retries-attempted 0]
    (let [response (-> (h/GET url) h/await)
          status (h/status response)
          status-code (:code status)]
      (cond (or (= 3 retries-attempted)
                (not (= 403 status-code)))
            {:url url
             :status status
             :body (-> response
                       h/string
                       safe-decode-json
                       walk/keywordize-keys)}
            :else (do (println "Rate limited, sleeping for 10 seconds")
                    (Thread/sleep 10000)
                      (recur url (inc retries-attempted)))))))

(def !nil? (comp not nil?))

(defn crc
  [x]
  (.getValue (doto (java.util.zip.CRC32.)
               (.update (.getBytes x)))))

(defn lookup-track
  [track-id]
  (let [t (lookup-spotify track-id)
        track-name (get-in t [:body :track :name])]
    (if (nil? track-name)
      nil
      {:id track-id
       :name track-name
       :crc (crc track-id)})))

(defn extract-track-ids
  [status]
  (filter !nil?
          (map #(second (re-find #"spotify\.com\/track\/([a-zA-Z0-9]+)$" %))
               (filter !nil?
                       (map #(or (:expanded_url %)
                                 (:url %))
                            (get-in status [:entities :urls]))))))

(defn build-record
  [status track-id]
  (let [track (lookup-track track-id)]
    (if (nil? track)
      nil
      [(get-in status [:user :id]) (get-in status [:user :screen_name]) (:id track) (:crc track) (:name track)])))

(defn expand-spotify-tweet
  [status]
  (if (empty? status)
    []
    (filter !nil? (map (partial build-record status)
                       (extract-track-ids status)))))

(defn tagged-json
  [field-name]
  (c/null-text-input safe-decode-json
                     io/encode-json
                     field-name))

(defn to-csv
  [& items]
  (clojure.contrib.string/join "," items))

(defn run
  [source sink]
  (io/with-log-level :info
    (let [trans (-> (c/pipe "j")
                    (c/mapcat #'expand-spotify-tweet :< "tweet" :fn> ["user_id" "screen_name" "track_id" "track_crc" "track_name"])
                    (c/map #'to-csv :< ["user_id" "screen_name" "track_id" "track_crc" "track_name"] :fn> "output"))
          flow (c/flow {"j" (c/hfs-tap (tagged-json "tweet") source)}
                       (c/hfs-tap (c/json-line) sink)
                       trans)]
      (c/exec flow))))

(defn to-mahout
  [line]
  (let [[user_id _ _ track_id _] (s/split #"," line)]
    (str (s/replace-str "\"" "" user_id) "," track_id)))

(defn run-input-for-mahout
  [source sink]
  (io/with-log-level :info
    (let [trans (-> (c/pipe "j")
                    (c/map #'to-mahout :< "line" :fn> "output"))
          flow (c/flow {"j" (c/lfs-tap (c/text-line "line") source)}
                       (c/lfs-tap (c/text-line) sink)
                       trans)]
      (c/exec flow))))

(defn -main
  [& args]
  (with-command-line args
    "cascading-clojure twitter demo"
    [[input "Input path"]
     [output "Output directory"]]
    (run input output)))


