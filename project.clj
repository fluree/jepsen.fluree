(defproject jepsen.fluree "0.1.0-SNAPSHOT"
  :description "A Jepsen test for Fluree"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main fluree.jepsen
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [jepsen "0.1.13"]
                 [cheshire "5.6.3"]
                 [clj-http "2.2.0"]])
