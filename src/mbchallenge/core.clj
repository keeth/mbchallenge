(ns mbchallenge.core
  (:require [clojure.string :as string]
            [mbchallenge.dialect :refer [get-dialect add-limit]]
            [mbchallenge.sql :refer [as-sql]]
            [mbchallenge.types :refer [get-literal get-clause]]))

(def select-statement ["SELECT" "*" "FROM" "data"])

(defn maybe-add-limit [{dialect :dialect} limit sql]
  (if limit
    (add-limit dialect limit sql)
    sql))

(def is-clause? sequential?)
(defn is-macro? [clause] (-> clause first (= :macro)))
(defn node-type [node]
  (if (is-clause? node)
    (if (is-macro? node)
      :macro
      :clause)
    :literal))

(defmulti parse-node (fn [node ctx] (node-type node)))
(defmethod parse-node :clause [node ctx]
  (let [clause-id (first node)
        clause-args (map #(parse-node % ctx) (rest node))
        clause (get-clause clause-id clause-args ctx)]
    clause))
(defmethod parse-node :literal [node ctx]
  (get-literal node ctx))
(defmethod parse-node :macro [node ctx]
  (let [macro-id (last node)
        macro (-> ctx :macros (get macro-id))]
    (parse-node macro ctx)))

(defn maybe-add-where [ctx where sql]
  (if where
    (concat
     sql
     ["WHERE"]
     (->
      where
      (parse-node ctx)
      (as-sql ctx)))
    sql))

(defn generate-sql [dialect fields {where :where limit :limit} & {:keys [macros] :or {macros {}}}]
  (let [ctx {:dialect (get-dialect dialect) :fields fields :macros macros}]
    (->>
     select-statement
     (maybe-add-where ctx where)
     (maybe-add-limit ctx limit)
     (string/join " "))))

