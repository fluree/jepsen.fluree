(ns jepsen.raft-ops
  (:refer-clojure :exclude [swap! reset! get set])
  (:require [clojure.core :as core]
            [clojure.tools.logging :refer :all]
            [clojure.core.reducers :as r]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clj-http.client :as http]
            [clj-http.util :as http.util]
            [cheshire.core :as json]
            [slingshot.slingshot :refer [try+ throw+]]
            [jepsen.control :as c])
  (:import (com.fasterxml.jackson.core JsonParseException)
           (java.io InputStream)
           (clojure.lang MapEntry)))

(def default-timeout "milliseconds" 1000)

(def default-swap-retry-delay
  "How long to wait (approximately) between retrying swap! operations which failed. In ms"
  100)

(defn connect
  "Create new client for given URI. ie.
  (def raft-kv (connect \"http://127.0.0.1:8080\"))

  Options:
:timeout            How long, in milliseconds, to wait for requests.
:swap-retry-delay   Roughly how long to wait between CAS retries in swap!"
  ([server-uri]
   (connect server-uri {}))
  ([server-uri opts]
   (merge {:timeout           default-timeout
           :swap-retry-delay  default-swap-retry-delay
           :endpoint          server-uri}
          opts)))

(defn base-url
  "Constructs the base URL for all raft-kv requests. Example:
  (base-url client) ; => \"http://127.0.0.1:8080/\""
  [client]
  (str (:endpoint client)))


(defn ^String url
  "The URL for a key under a specified root-key.
  (url client) ; => \"http://127.0.0.1:4001/read"
  [client]
  (str "http://" (base-url client) "/read"))

(defn http-opts
  "Given a map of options for a request, constructs a clj-http options map.
  :timeout is used for the socket and connection timeout. Remaining options are
  passed as query params."
  [client opts]
  {:as                    :string
   :throw-exceptions?     true
   :throw-entire-message? true
   :follow-redirects      true
   :force-redirects       true
   :socket-timeout        (or (:timeout opts) (:timeout client))
   :conn-timeout          (or (:timeout opts) (:timeout client))
   :query-params          (dissoc opts :timeout :root-key)})


(defn parse-json
  "Parse an inputstream or string as JSON"
  [str-or-stream]
  (if (instance? InputStream str-or-stream)
    (json/parse-stream (io/reader str-or-stream) true)
    (json/parse-string str-or-stream true)))

(defn parse-resp
  "Takes a clj-http response, extracts the body, and assoc's status and Raft
  X-headers as metadata (:etcd-index, :raft-index, :raft-term) on the
  response's body."
  [response]
  (when-not (:body response)
    (throw+ {:type     ::missing-body
             :response response}))

  (try+
    (let [body (parse-json (:body response))
          h    (:headers response)]
      (with-meta body
                 {:status           (:status response)
                  :leader-peer-url  (core/get h "x-leader-peer-url")
                  :etcd-index       (core/get h "x-etcd-index")
                  :raft-index       (core/get h "x-raft-index")
                  :raft-term        (core/get h "x-raft-term")}))
    (catch JsonParseException e
      (throw+ {:type     ::invalid-json-response
               :response response}))))

(defn remap-keys
  "Given a map, transforms its keys using the (f key). If (f key) is nil,
  preserves the key unchanged.
  (remap-keys inc {1 :a 2 :b})
  ; => {2 :a 3 :b}
  (remap-keys {:a :a'} {:a 1 :b 2})
  ; => {:a' 1 :b 2}"
  [f m]
  (->> m
       (r/map (fn [[k v]]
                [(let [k' (f k)]
                   (if (nil? k') k k'))
                 v]))
       (into {})))

(defn get
  ([client key]
   (get client key {}))
  ([client key opts]
   ;(let [_ (info "CLIENT" client)
   ;      endpoint (url client)
   ;      _ (info "ENDPOINT" endpoint)
   ;      body ["read", key]
   ;      _ (info "BODY " body)
   ;      res  (try (c/exec :curl
   ;                        ;:-d (c/lit (json/generate-string body))
   ;                        (c/lit (str url "/read")))
   ;                (catch Exception e (info "READ failed: " e)))
   ;      _ (info "RES READ: " res)]
   ;  res)
   ;
   (->> opts
        (remap-keys {:recursive?   :recursive
                     :consistent?  :consistent
                     :quorum?      :quorum
                     :sorted?      :sorted
                     :wait?        :wait
                     :wait-index   :waitIndex})
        (http-opts client)
        (http/post (url client) {:body (json/encode ["read" key])})
        :body)

    )
  )

(comment

  (json/generate-string ["write", 1, 2])
  )

(defn set
  "Sets the value of a given key to value. Options:
  :ttl
  :timeout"
  ([client key value]
   (set client key value {}))
  ([client key value opts]
    (let [_ (info "CLIENT" client)
          endpoint (url client)
          _ (info "ENDPOINT" endpoint)
          body ["write", key, value]
          _ (info "BODY " body)
          res  (try (c/exec :curl
                            ;:-d (c/lit (json/generate-string body))
                            (c/lit (str url "/read")))
                    (catch Exception e (info "Write failed: " e)))
          _ (info "RES WRITE: " res)]
      res
      )

   ;(->> (assoc opts :value value)
   ;     (http-opts client)
   ;     (http/post (url client)  {:body (json/encode ["write" key value])})
   ;     :body)

    ))


(defn cas
  "Compare and set based on current value. Options:
  :ttl
  :timeout"
  ([client key value value']
   (cas client key value value' {}))
  ([client key value value' opts]
   ;(->> (assoc opts :value value)
   ;     (http-opts client)
   ;     (http/post (url client) {:body (json/encode ["cas" key value value'])})
   ;     :body)
   (let [_ (info "CLIENT" client)
         endpoint (url client)
         _ (info "ENDPOINT" endpoint)
         res  (try (c/exec :curl
                           ;:-d (str "[\"cas\", \"" key "\", \"" value "\" \"" value' "\"]")
                           (c/lit (str url "/read")))
                   (catch Exception e (info "CAS failed: " e)))
         _ (info "RES CAS: " res)]
     res
     )
    ))
