(ns data-stream.core
  (:require [clojure.java.shell :as shell]
            [clojure.string :as s]
            [data-stream.parser :as parser]
            [data-stream.stats :as stats]
            [data-stream.utils :refer :all]))

(defn run-stats
  [fns xs]
  (->> fns
       (map (fn [[kw f]] [kw (stats/calc (apply stats/online-calc f xs))]))
       (into {})))

;; Move to test dir
(assert (= (run-stats [[:min stats/minimum]
                       [:avg stats/average]
                       [:count stats/not-null-count]
                       [:null-count stats/null-count]
                       [:max stats/maximum]] [1 2 3 nil])
           {:min        1
            :max        3
            :avg        2
            :null-count 1
            :count      3}))

(defn run-stats-on
  [rows header-index header]
  (let [{:keys [cell-type cell-name]} header
        xs (mapv #(:value (get % header-index)) rows)
        fns (stats/cell-type->stat-fns cell-type)]
    (run-stats fns xs)))

(defn batched-data-stream-computation [n]
  (let [{:keys [headers rows]} (parser/parse-input (generator n))]
    ;; run in parallel (pmap: parallel map)
    (pmap (partial run-stats-on rows) (range) headers)))


(test-perfs batched-data-stream-computation 10 1000000)

;Output=> O (n) in time; constant space
;n:  10
;"Elapsed time: 13.077821 msecs"
;-----------
;n:  100
;"Elapsed time: 13.061911 msecs"
;-----------
;n:  1000
;"Elapsed time: 37.469522 msecs"
;-----------
;n:  10000
;"Elapsed time: 261.222542 msecs"
;-----------
;n:  100000
;"Elapsed time: 2727.67692 msecs"
;-----------
;n:  1000000
;"Elapsed time: 33962.860368 msecs"
;-----------
