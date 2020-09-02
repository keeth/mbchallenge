(ns mbchallenge.types)

; Literals
(defrecord SQLNull [])
(defrecord SQLNumber [value])
(defrecord SQLString [value])

; Clauses
(defrecord SQLEqualTo [args])
(defrecord SQLNotEqualTo [args])
(defrecord SQLGreaterThan [args])
(defrecord SQLLessThan [args])
(defrecord SQLAnd [args])
(defrecord SQLOr [args])
(defrecord SQLIsEmpty [value])
(defrecord SQLNotEmpty [value])
(defrecord Field [value])

(defmulti get-literal (fn [node _] (class node)))
(defmethod get-literal nil [_ ctx] (SQLNull.))
(defmethod get-literal Long [node ctx] (SQLNumber. node))
(defmethod get-literal String [node ctx] (SQLString. node))

(defmulti get-clause (fn [clause-id _ _] (keyword clause-id)))
(defmethod get-clause := [_ args ctx] (SQLEqualTo. args))
(defmethod get-clause :< [_ args ctx] (SQLLessThan. args))
(defmethod get-clause :> [_ args ctx] (SQLGreaterThan. args))
(defmethod get-clause :!= [_ args ctx] (SQLNotEqualTo. args))
(defmethod get-clause :and [_ args ctx] (SQLAnd. args))
(defmethod get-clause :or [_ args ctx] (SQLOr. args))
(defmethod get-clause :is-empty [_ args ctx] (SQLIsEmpty. (first args)))
(defmethod get-clause :not-empty [_ args ctx] (SQLNotEmpty. (first args)))
(defmethod get-clause :field [_ args ctx]
  (let [field-id (-> (first args) (.-value))]
    (Field. (-> ctx :fields (get field-id)))))
