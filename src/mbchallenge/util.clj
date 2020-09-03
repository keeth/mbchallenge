(ns mbchallenge.util)

(def is-clause? sequential?)

(defn is-macro? [clause] (-> clause first (= :macro)))

(defn node-type [node]
  (if (is-clause? node)
    (if (is-macro? node)
      :macro
      :clause)
    :literal))

(defmulti circular-macro-check* (fn [node seen macros] (node-type node)))

(defmethod circular-macro-check* :clause [node seen macros]
  (doseq [child (rest node)]
    (circular-macro-check* child seen macros)))

(defmethod circular-macro-check* :macro [node seen macros]
  (let [macro-id (last node)
        macro (-> macros (get macro-id))]
    (when (seen macro-id)
      (throw (IllegalArgumentException. (str "Circular macro \"" macro-id "\" detected!"))))
    (circular-macro-check* macro (conj seen macro-id) macros)))

(defmethod circular-macro-check* :literal [node seen macros])

(defn circular-macro-check! [macros]
  (doseq [keyval macros]
    (circular-macro-check* (val keyval) (set [(key keyval)]) macros)))
