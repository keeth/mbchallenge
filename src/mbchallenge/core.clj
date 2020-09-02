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
  (as-sql [this]))

; Literals
(defrecord SQLNull [ctx])
(defrecord SQLNumber [ctx value])

; Clauses
(defrecord SQLEquals [ctx args])
(defrecord Field [ctx args])

(declare parse-clause)

(extend-protocol AsSQL
  SQLNull
  (as-sql [this] ["IS NULL"])
  SQLNumber
  (as-sql [this] [(-> (.-value this) str)])
  SQLEquals
  (as-sql [this] (concat
                   (-> (.-args this) (first) (as-sql))
                   ["="]
                   (-> (.-args this) (last) (as-sql))))
  Field
  (as-sql [this] ["foo"]))

(defmulti get-literal (fn [node _] (class node)))
(defmethod get-literal nil [_ ctx] (SQLNull. ctx))
(defmethod get-literal Long [node ctx] (SQLNumber. ctx node))

(defmulti get-clause (fn [clause-id _ _] clause-id))
(defmethod get-clause := [_ args ctx] (SQLEquals. ctx args))
(defmethod get-clause :field [_ args ctx] (Field. ctx args))

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
    (when-not clause
      (throw (IllegalArgumentException. (str "Unknown clause " node))))
    clause))
(defmethod parse-node false [node ctx]
  (if-let [literal (get-literal node ctx)]
    literal
    (throw (IllegalArgumentException. (str "Unknown literal " node)))))

(defn maybe-with-where [ctx where sql]
  (if where
    (concat sql ["WHERE"] (as-sql (parse-node where ctx)))
    sql))

(defn generate-sql [dialect fields {where :where limit :limit}]
  (let [sql-dialect (get-dialect dialect)
        ctx {:dialect sql-dialect :fields fields}]
    (->>
      select
      (maybe-with-where ctx where)
      (maybe-with-limit ctx limit)
      (string/join " "))))

