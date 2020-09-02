(ns mbchallenge.dialect)

(defprotocol Dialect
  (with-limit [_ limit sql])
  (quote-identifier [_ identifier]))

(def base-dialect
  {:with-limit (fn [_ limit sql]
                 (concat sql ["LIMIT" limit]))
   :quote-identifier (fn [_ identifier]
                       (str "\"" identifier "\""))})

(defrecord PostgresDialect [])

(defrecord MySQLDialect [])

(defrecord SQLServerDialect [])

(extend PostgresDialect
  Dialect
  base-dialect)

(extend MySQLDialect
  Dialect
  (merge base-dialect
         {:quote-identifier (fn [_ identifier]
                              (str "`" identifier "`"))}))

(extend SQLServerDialect
  Dialect
  (merge base-dialect
         {:with-limit (fn [_ limit [select & xs]]
                        (concat [select "TOP" limit] xs))}))

(defmulti get-dialect identity)

(defmethod get-dialect :postgres [_] (PostgresDialect.))

(defmethod get-dialect :mysql [_] (MySQLDialect.))

(defmethod get-dialect :sqlserver [_] (SQLServerDialect.))
