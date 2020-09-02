(ns mbchallenge.core-test
  (:require [clojure.test :refer :all]
            [mbchallenge.core :refer :all]))

(def fields {1 :id
             2 :name
             3 :date_joined
             4 :age})

(deftest generate-sql-tests
  (testing "limit"
    (is (=
          "SELECT * FROM data LIMIT 5"
          (generate-sql :postgres fields {:limit 5})))
    (is (=
          "SELECT * FROM data LIMIT 5"
          (generate-sql :mysql fields {:limit 5})))
    (is (=
          "SELECT TOP 5 * FROM data"
          (generate-sql :sqlserver fields {:limit 5}))))
  (testing "no limit"
    (is (=
          "SELECT * FROM data"
          (generate-sql :postgres fields {}))))
  (testing "basics"
    (is (=
          "SELECT * FROM data WHERE date_joined IS NULL"
          (generate-sql :postgres fields {:where [:= [:field 3] nil]})))))
