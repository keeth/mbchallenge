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
         {:with-limit (fn [limit [select & xs]]
                        (concat [select "TOP" limit] xs))}))

(defmulti get-dialect identity)

(defmethod get-dialect :postgres [_] (PostgresDialect.))

(defmethod get-dialect :mysql [_] (MySQLDialect.))

(defmethod get-dialect :sqlserver [_] (SQLServerDialect.))

(defn generate-sql [dialect fields {where :where limit :limit}]
  (let [sql-dialect (get-dialect dialect)]
    (->>
      select
      (with-limit sql-dialect limit)
      (string/join " "))))

