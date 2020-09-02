(ns mbchallenge.core
  (:require [clojure.string :as string]
            [mbchallenge.dialect :refer [get-dialect with-limit]]
            [mbchallenge.sql :refer [as-sql]]
            [mbchallenge.types :refer [get-literal get-clause]]))

(def select-statement ["SELECT" "*" "FROM" "data"])

(defn maybe-with-limit [{dialect :dialect} limit sql]
  (if limit
    (with-limit dialect limit sql)
    sql))

(def is-clause? sequential?)

(defmulti parse-node (fn [node ctx] (is-clause? node)))
(defmethod parse-node true [node ctx]
  (let [clause-id (first node)
        clause-args (map #(parse-node % ctx) (rest node))
        clause (get-clause clause-id clause-args ctx)]
    clause))
(defmethod parse-node false [node ctx]
  (get-literal node ctx))

(defn maybe-with-where [ctx where sql]
  (if where
    (concat
     sql
     ["WHERE"]
     (->
      where
      (parse-node ctx)
      (as-sql ctx)))
    sql))

(defn generate-sql [dialect fields {where :where limit :limit}]
  (let [sql-dialect (get-dialect dialect)
        ctx {:dialect sql-dialect :fields fields}]
    (->>
     select-statement
     (maybe-with-where ctx where)
     (maybe-with-limit ctx limit)
     (string/join " "))))

