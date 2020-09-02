(ns mbchallenge.sql
  (:require [mbchallenge.types]
            [mbchallenge.dialect :refer [quote-identifier]])
  (:import (mbchallenge.types SQLNumber SQLNull SQLString SQLGreaterThan SQLLessThan SQLAnd SQLOr SQLNotEqualTo SQLEqualTo Field SQLIsEmpty SQLNotEmpty)))

(defprotocol AsSQL
  (as-sql [this ctx]))

(declare parse-clause)

(defn binary-op-sql [op args ctx & {:keys [parens?]
                                    :or {parens? false}}]
  (let [nested-ctx (assoc ctx :nested? true)
        clause-sql (concat
                     (as-sql (first args) nested-ctx)
                     [op]
                     (as-sql (last args) nested-ctx))]
    (if (and parens? (:nested? ctx))
      (concat ["("] clause-sql [")"])
      clause-sql)))

(defn equality-op-sql [binary-op null-op n-ary-op args ctx]
  (let [first-arg (first args)
        last-arg (last args)]
    (if (= 2 (count args))
      (if (= (class last-arg) SQLNull)
        (concat (as-sql first-arg ctx) [null-op])
        (binary-op-sql binary-op args ctx))
      (concat
        (as-sql first-arg ctx)
        [n-ary-op "("]
        (interpose "," (mapcat #(as-sql % ctx) (rest args)))
        [")"]))))

(def is-null "IS NULL")
(def is-not-null "IS NOT NULL")

(extend-protocol AsSQL
  SQLNull
  (as-sql [this ctx] ["NULL"])
  SQLNumber
  (as-sql [this ctx] [(-> this (.-value) str)])
  SQLString
  (as-sql [this ctx] [(str "'" (.-value this) "'")])
  SQLGreaterThan
  (as-sql [this ctx]
    (binary-op-sql ">" (.-args this) ctx))
  SQLLessThan
  (as-sql [this ctx]
    (binary-op-sql "<" (.-args this) ctx))
  SQLAnd
  (as-sql [this ctx]
    (binary-op-sql "AND" (.-args this) ctx :parens? true))
  SQLOr
  (as-sql [this ctx]
    (binary-op-sql "OR" (.-args this) ctx :parens? true))
  SQLNotEqualTo
  (as-sql [this ctx]
    (equality-op-sql "<>" is-not-null "NOT IN" (.-args this) ctx))
  SQLEqualTo
  (as-sql [this ctx]
    (equality-op-sql "=" is-null "IN" (.-args this) ctx))
  SQLIsEmpty
  (as-sql [this ctx] (conj (as-sql (.-value this) ctx) is-null))
  SQLNotEmpty
  (as-sql [this ctx] (conj (as-sql (.-value this) ctx) is-not-null))
  Field
  (as-sql [this ctx] [(->> this .-value name (quote-identifier (:dialect ctx)))]))
