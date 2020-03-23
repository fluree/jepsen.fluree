(ns jepsen.fluree
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [checker :as checker]
             [cli :as cli]
             [client :as client]
             [control :as c]
             [db :as db]
             [generator :as gen]
             [nemesis :as nemesis]
             [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model]
            [jepsen.raft-ops :as raft-ops]
            [jepsen.os.debian :as debian]))

(def directory "/opt")
(def logfile "/logs/log-2020-03-23.log")

;; has to be /ips.txt in docker
;; But using a sample "ips.txt" here
(def ips (try (slurp "/ips.txt")
              (catch Exception e (slurp "ips.txt"))))

(defn format-ips
  ([ips]
   (format-ips ips nil))
  ([ips port]
   (let [ips'         (-> (subs ips 1 (dec (count ips)))
                          (str/split #","))
         no-control   (filter #(not (str/includes? % "jepsen-control")) ips')
         format-names (map (fn [ip]
                             (let [node-named (str/replace ip "jepsen-n" "")
                                   [name loc] (str/split node-named #"@")
                                   [base mask] (str/split loc #"/")
                                   port       (or port (-> (read-string name) dec (+ 9790)))]
                               (str name "@" base ":" port))) no-control)]
     (str/join "," format-names))))

(defn node-url
  "An HTTP url for connecting to a node on a particular port."
  [node port]
  (str "http://" (name node) ":" port))

(defn peer-url
  "The HTTP url for other peers to talk to a node."
  [node]
  (node-url node 8080))

(defn client-url
  "The HTTP url clients use to talk to a node."
  [node]
  (some #(when (str/includes? % (str node "@"))
           (-> (str/split % #"@") second)) (str/split (format-ips ips 8080) #",")))

(defn r [_ _] {:type :invoke :f :read :value nil})
(defn w [_ _] {:type :invoke :f :write :value (str (rand-int 5))})
(defn cas [_ _] {:type :invoke :f :cas :value [(str (rand-int 5)) (str (rand-int 5))]})

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (info "NODE" node)
    (assoc this :conn (raft-ops/connect (client-url (subs node 1))
                                        {:timeout 5000})))
  (setup! [this test])

  (invoke! [_ test op]
    (info "CONN" conn)
    (case (:f op)
      :read (try (let [res (assoc op :type :ok, :value (raft-ops/get conn "foo"))
                       _   (info "READ RES" res)]
                   res)
                 (catch Exception e (info "Read failed" e)))
      :write (try
               (let [res (assoc op :type :ok :value (raft-ops/set conn "foo" (:value op)))
                     _   (info "WRITE RES" res)]
                 res)
               (catch Exception e (info "Write failed" e)))
      :cas (let [[old new] (:value op)]
             (assoc op :type (if (raft-ops/cas conn old new (:value op))
                               :ok :fail)))))

  (teardown! [this test])

  (close! [_ test]))

(defn db
  "Raft-KV"
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node (str " - Installing Raft-KV"))
      (let [url "https://fluree.s3.amazonaws.com/raft-kv.zip"
            n   (read-string (subs node 1))]
        (cu/install-archive! url directory)
        (info "Installed zip")
        (cu/wget! "https://fluree.s3.amazonaws.com/raft-kv")
        (c/exec :mv "raft-kv" "/etc/init.d/raft-kv")
        (c/su (c/exec :chmod :+x "/etc/init.d/raft-kv"))
        (info "Created service")
        (let [traced-service-start (future (c/su (c/exec :service :raft-kv :start n 5)))
              _                    (info "TRACE" traced-service-start)])
        (info "Started service")
        ;; nohup java -jar /opt/raft-kv/raft.jar n (format-ips ips) &
        ;(info "Started JAR")
        (Thread/sleep 5000)
        (info "After thread sleep in setup!")))
    (teardown! [_ test node]
      (let [n   (read-string (subs node 1))
            ;_   (cu/grepkill! :java)
            url (client-url n)
            _   (try (c/su (c/exec :curl (c/lit (str url "/close"))))
                     (catch Exception e (info "Successfully shut down")))]
        (info "Shutting down " node)
        (Thread/sleep 3000)
        ;(c/su (c/exec :rm :-rf directory "nohup.out"))
        (c/su (c/exec :rm :-rf "nohup.out"))
        ))
    db/LogFiles
    (log-files [_ test node]
      [logfile])))

(defn fluree-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name      "fluree"
          :os        debian/os
          :db        (db "1")
          :client    (Client. nil)
          :nemesis    (nemesis/partition-random-halves)
          :checker   (checker/compose
                       {:perf   (checker/perf)
                        :linear (checker/linearizable {:model     (model/cas-register)
                                                       :algorithm :linear})
                        :timeline (timeline/html)})
          :generator (->> (gen/mix [r w cas])
                          (gen/stagger 1)
                          (gen/nemesis
                            (gen/seq (cycle [(gen/sleep 5)
                                             {:type :info :f :start}
                                             (gen/sleep 5)
                                             {:type :info :f :stop}])))
                          (gen/time-limit 30))}))

(defn -main
  "Handles command line arguments. Can either run a test, or a webserver for browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn fluree-test})
                   (cli/serve-cmd))
            args))