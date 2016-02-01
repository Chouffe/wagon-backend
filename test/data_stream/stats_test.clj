(ns data-stream.stats-test
  (:require [clojure.test :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [incanter.stats :as istats :exclude [update]]
            [data-stream.stats :refer :all]))

;; -----
;; Utils
;; -----

(def int-or-nil (gen/one-of [gen/int (gen/return nil)]))

(defn relative-error
  [epsilon x y]
  (<= (Math/abs (- x y)) epsilon))

(defn test-properties
  [number-tests & properties]
  (map (partial tc/quick-check number-tests) properties))

;; -----
;; Props
;; -----

(def average-prop
  (prop/for-all [xs (gen/such-that not-empty (gen/vector gen/int))]
    (relative-error 0.001
                    (calc (apply online-calc average xs))
                    (istats/mean xs))))

(def median-prop
  (prop/for-all [xs (gen/such-that not-empty (gen/vector gen/int))]
    (relative-error 0.001
                    (calc (apply online-calc median xs))
                    (istats/median xs))))

(def null-count-prop
  (prop/for-all [xs (gen/such-that not-empty (gen/vector int-or-nil))]
    (= (count (filter nil? xs)) (calc (apply online-calc null-count xs)))))

;; Runs 1000 random tests on the properties
(test-properties 1000 median-prop average-prop null-count-prop)

; ----------
; REPL tests
; ----------
; (calc (online-calc average nil 1 nil 3 #_(range 1000)))
; (calc (online-calc median nil 1 nil 3 3.2 4 5 #_(range 1000)))
; (calc (redux minimum nil 1 nil 3 #_(range 1000)))
; (calc (redux shortest-value nil "Query" "hellooooooo" nil))
; (calc (redux average-length nil "Query" "hellooooooo" nil))
; (calc (apply redux minimum (shuffle (range 1000))))
; (calc (apply redux maximum (shuffle (range 1000))))
; (calc (redux null-count 1 ))
; (calc (redux not-null-count 3 2 1 nil nil 1 nil))
