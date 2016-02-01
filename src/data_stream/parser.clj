(ns data-stream.parser
  (:require [clojure.string :as s]))

(def cell-separator-regex #",")
(def line-separator-regex #"\r\n")
(def cell-header-regex #"\"(.*)\s+\((.*)\)\"")
(def cell-type->coerce-fn {:number #(Float. %) :text identity})

(defn parse-header-cell
  [s]
  (let [[_ name type] (re-matches cell-header-regex s)]
    {:cell-name name :cell-type (keyword type)}))

(defn parse-header
  [s]
  (mapv parse-header-cell (s/split s cell-separator-regex)))

(defn parse-row-fn
  [headers]
  (let [coercers (map (comp cell-type->coerce-fn :cell-type) headers)]
    (fn [s]
      (mapv (fn [x coercer] {:value (when-not (empty? x) (coercer x))})
           (s/split s cell-separator-regex) coercers))))

(defn parse-input
  [input]
  (let [[raw-header & raw-rows] (s/split input line-separator-regex)
        headers (parse-header raw-header)
        row-parser (parse-row-fn headers)]
  {:headers headers
   :rows (mapv row-parser raw-rows)}))
