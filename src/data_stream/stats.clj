(ns data-stream.stats
  (:require [data-stream.utils :refer :all]
            [clojure.data.priority-map :as prio]
            [incanter.stats :as istats :exclude [update]]))

(defn make-stat-fn
  "Creates any stat-fn that can work with data stream.

   - a reducer that is applied on each new point
   - a computer which computes the stat-fn value from the reducer-state
   - an initial reducer state (default: 0)
   - a pred function to filter out values (eg nil? and (complement nil?))"
  ([reducer computer] (make-stat-fn reducer computer 0))
  ([reducer computer reducer-state] (make-stat-fn reducer computer reducer-state (constantly true)))
  ([reducer computer reducer-state pred]
   {:reducer       reducer
    :reducer-state reducer-state
    :pred          pred
    :computer      computer}))

(defn online-calc
  "Online calculation for a new data point.
   Input: stat-fn (created with make-stat-fn for instance)
          a sequence of points xs to perform the online-calc on
   Output: stat-fn with an updated reducer-state"
  [{:keys [reducer reducer-state computer pred] :as stat-fn} & xs]
  (let [xs (filter pred xs)]
    (if-not (seq xs)
      stat-fn
      (assoc stat-fn :reducer-state (reduce reducer reducer-state xs)))))

(defn calc
  "Input: a stat-fn
   Output: the value of the stat-fn on the current reducer-state

   The rationale is to make the calc function very performant in terms of space/time complexity (constant time and space)"
  [{:keys [reducer-state computer] :as stat-fn}]
  (computer reducer-state))

;; ----------------------
;; Reducers and Computers
;; ----------------------

(defn avg-reducer
  [[cnt sum] x]
  [(inc cnt) (+ x sum)])

(defn avg-computer
  [[cnt sum :as state]]
  (/ sum cnt))

(defn avg-length-reducer
  [[cnt sum] txt]
  [(inc cnt) (+ (count txt) sum)])

(def avg-length-computer avg-computer)

(defn value-count-reducer-fn
  [comparator-fn]
  (fn [[size-map current-size :as state] x]
    (let [size (count x)]
      (cond
        (and (= size current-size) (get size-map x))       [(update size-map x inc) current-size]
        (and (= size current-size) (not (get size-map x))) [(assoc size-map x 1) current-size]
        (comparator-fn size current-size)                  [(hash-map x 1) size]
        :else                                              state))))

(defn value-count-computer
  [[size-map size :as state]]
  [size (->> size-map
             (remove (comp (partial > (apply max (vals size-map))) second))
             (map first)
             sort)])

(defn median-reducer
  "Keeps a max and min heap in memory to be able to return in constant time the median element."
  [[max-heap min-heap] x]
  (let [[_ maxi :as max-heap?] (peek max-heap)
        [_ mini :as min-heap?] (peek min-heap)]
    (cond
      (and (not max-heap?) (not min-heap?))
      [(assoc max-heap (gensym x) x) min-heap]

      (and (= (count max-heap) (count min-heap)) (>= x mini))
      [max-heap (assoc min-heap (gensym x) x)]

      (and (= (count max-heap) (count min-heap)) (< x mini))
      [(assoc max-heap (gensym x) x) min-heap]

      (> (count max-heap) (count min-heap))
      (if (<= x maxi)
        [(assoc (pop max-heap) (gensym x) x) (assoc min-heap (gensym maxi) maxi)]
        [max-heap (assoc min-heap (gensym x) x)])

      (< (count max-heap) (count min-heap))
      (if (>= x mini)
        [(assoc max-heap (gensym mini) mini) (assoc (pop min-heap) (gensym x) x)]
        [(assoc max-heap (gensym x) x) min-heap]))))

(defn median-computer
  [[max-heap min-heap]]
  (cond
    (> (count max-heap) (count min-heap)) (second (peek max-heap))
    (< (count max-heap) (count min-heap)) (second (peek min-heap))
    :else (float (/ (+ (second (peek max-heap)) (second (peek min-heap))) 2))))

;; --------------
;; Stat functions
;; --------------

(def average (make-stat-fn avg-reducer
                           avg-computer
                           [0 0] ;; [count sum]
                           (complement nil?) ))
(def minimum (make-stat-fn min
                           identity
                           (Integer/MAX_VALUE) ;; current min
                           (complement nil?)))
(def maximum (make-stat-fn max
                           identity
                           (Integer/MIN_VALUE) ;; current max
                           (complement nil?)))
(def shortest-value (make-stat-fn (value-count-reducer-fn <)
                                  value-count-computer
                                  [{} (Integer/MAX_VALUE)]
                                  (complement nil?)))
(def longest-value (make-stat-fn (value-count-reducer-fn >)
                                 value-count-computer
                                 [{} (Integer/MIN_VALUE)]
                                 (complement nil?)))
(def average-length (make-stat-fn avg-length-reducer
                                  avg-length-computer
                                  [0 0] ;; [count sum]
                                  (complement nil?)))
(def null-count (make-stat-fn (fn [acc _] (inc acc))
                              identity
                              0 ; current counr
                              nil?))
(def not-null-count (make-stat-fn (fn [acc _] (inc acc))
                                  identity
                                  0
                                  (complement nil?)))
(def median (make-stat-fn median-reducer
                          median-computer
                          [(prio/priority-map-by >) (prio/priority-map-by <)]
                          ;; Empty Max heap and Empty Min heap
                          (complement nil?)))

(def stat-fns
  "This table maps from a cell-type to a collection of stat-fns that should be computed on that cell-type"
  {:number  {:minimum              minimum
             :maximum              maximum
             :average              average}
   :text    {:shortest-value-count shortest-value
             :longest-value-count  longest-value
             :average-length       average-length}
   :default {:null-count           null-count
             :count                not-null-count}})

(defn cell-type->stat-fns
  [cell-type]
  (merge (get stat-fns :default) (get stat-fns cell-type)))
