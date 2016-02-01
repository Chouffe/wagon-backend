(ns data-stream.utils
  (:require [clojure.java.shell :as shell]
            [clojure.java.io :refer [reader writer]]))

(defn generator
  [n]
  (->> n str (shell/sh "./generator") :out))

(def prompt ">")

(defn test-client [in out]
  (binding [*in* (reader in)
            *out* (writer out)]
           (print prompt) (flush)
           (loop [input (read-line)]
             (when input
               (println (str "OUT:" input))
               (print prompt) (flush)
               (if (not= input "exit\n") (recur (read-line)) )))))

(def client-stream (java.io.PipedWriter.))
(def r (java.io.BufferedReader. (java.io.PipedReader. client-stream)))

(defn run-client
  []
  (future (test-client r *out*)))
; (.write client-stream "test\n")

(defn- log-range-aux
  [start step end acc]
  (if (> start end)
    acc
    (recur (* start step) step end (conj acc start))))

(defn log-range
  [start step end]
  (log-range-aux start step end []))

(assert (= (log-range 1 10 1000)   [1 10 100 1000]))
(assert (= (log-range 1 10 100000) [1 10 100 1000 10000 100000]))

(defn test-perfs
  [f n-start n-end]
  (doseq [n (log-range n-start 10 n-end)]
    (println "n: " n)
    (time (f n))
    (println "-----------")))
