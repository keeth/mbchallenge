(ns mbchallenge.core
  (:require [clojure.string :as string]))

(def select ["SELECT" "*" "FROM" "data"])

(defprotocol Dialect
  (with-limit [_ limit sql]))

(def base-dialect
  {:with-limit (fn [_ limit sql] (concat sql ["LIMIT" limit]))})

(defrecord PostgresDialect [])

(defrecord MySQLDialect [])

(defrecord SQLServerDialect [])

(extend PostgresDialect
  Dialect
  base-dialect)

(extend MySQLDialect
  Dialect
  base-dialect)

(extend SQLServerDialect
  Dialect
  (merge base-dialect
         {:with-limit (fn [_ limit [select & xs]]
                        (concat [select "TOP" limit] xs))}))

(defmulti get-dialect identity)

(defmethod get-dialect :postgres [_] (PostgresDialect.))

(defmethod get-dialect :mysql [_] (MySQLDialect.))

(defmethod get-dialect :sqlserver [_] (SQLServerDialect.))

(defprotocol AsSQL
  (as-sql [this ctx]))

; Literals
(defrecord SQLNull [])
(defrecord SQLNumber [value])
(defrecord SQLString [value])

; Clauses
(defrecord SQLEqualTo [args])
(defrecord SQLNotEqualTo [args])
(defrecord SQLGreaterThan [args])
(defrecord SQLLessThan [args])
(defrecord Field [value])

(declare parse-clause)

(defn binary-op-sql [op args ctx]
  (concat
    (as-sql (first args) ctx)
    [op]
    (as-sql (last args) ctx)))

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
    (binary-op-sql ">" (.-args this) ctx))
  SQLNotEqualTo
  (as-sql [this ctx]
    (let [args (.-args this)
          first-arg (first args)
          last-arg (last args)]
      (if (= 2 (count args))
        (if (= (class last-arg) SQLNull)
          (concat (as-sql first-arg ctx) ["IS NOT NULL"])
          (binary-op-sql "!=" args ctx))
        (concat
          (as-sql first-arg ctx)
          ["NOT IN" "("]
          (interpose "," (mapcat #(as-sql % ctx) (rest args)))
          [")"]))))
  SQLEqualTo
  (as-sql [this ctx]
    (let [args (.-args this)
          first-arg (first args)
          last-arg (last args)]
      (if (= 2 (count args))
        (if (= (class last-arg) SQLNull)
          (concat (as-sql first-arg ctx) ["IS NULL"])
          (binary-op-sql "=" args ctx))
        (concat
          (as-sql first-arg ctx)
          ["IN" "("]
          (interpose "," (mapcat #(as-sql % ctx) (rest args)))
          [")"]))))
  Field
  (as-sql [this ctx] [(-> this (.-value) (name))]))

(defmulti get-literal (fn [node _] (class node)))
(defmethod get-literal nil [_ ctx] (SQLNull.))
(defmethod get-literal Long [node ctx] (SQLNumber. node))

(defmulti get-clause (fn [clause-id _ _] clause-id))
(defmethod get-clause := [_ args ctx] (SQLEqualTo. args))
(defmethod get-clause :< [_ args ctx] (SQLLessThan. args))
(defmethod get-clause :> [_ args ctx] (SQLGreaterThan. args))
(defmethod get-clause :!= [_ args ctx] (SQLNotEqualTo. args))
(defmethod get-clause :field [_ args ctx]
  (let [field-id (-> (first args) (.-value))]
    (Field. (-> ctx :fields (get field-id)))))

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
    (concat sql ["WHERE"] (as-sql (parse-node where ctx) ctx))
    sql))

(defn generate-sql [dialect fields {where :where limit :limit}]
  (let [sql-dialect (get-dialect dialect)
        ctx {:dialect sql-dialect :fields fields}]
    (->>
      select
      (maybe-with-where ctx where)
      (maybe-with-limit ctx limit)
      (string/join " "))))

