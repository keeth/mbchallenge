(ns mbchallenge.core-test
  (:require [clojure.test :refer :all]
            [mbchallenge.core :refer :all]))

(def fields {1 :id
             2 :name
             3 :date_joined
             4 :age})

(def macros {"is_joe" [:= [:field 2] "joe"]
             "is_old" [">" [:field 4] 18]
             "is_old_joe" [:and [:macro "is_old"] [:macro "is_joe"]]
             })

(def circular-macros {"is_good" [:and [:macro "is_decent"] [:> [:field 4] 18]]
                      "is_decent" [:and [:macro "is_good"] [:< [:field 5] 5]]})

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
         "SELECT * FROM data WHERE \"date_joined\" IS NULL"
         (generate-sql :postgres fields {:where [:= [:field 3] nil]}))))

  (testing "main requirements"
    (is (= "SELECT * FROM data WHERE \"age\" > 35"
           (generate-sql :postgres fields {:where [:> [:field 4] 35]})))
    (is (= "SELECT * FROM data WHERE \"id\" < 5 AND \"name\" = 'joe'"
           (generate-sql :postgres fields {:where [:and [:< [:field 1] 5] [:= [:field 2] "joe"]]})))
    (is (= "SELECT * FROM data WHERE \"date_joined\" <> '2015-11-01' OR \"id\" = 456"
           (generate-sql :postgres fields {:where [:or [:!= [:field 3] "2015-11-01"] [:= [:field 1] 456]]})))
    (is (= "SELECT * FROM data WHERE \"date_joined\" IS NOT NULL AND ( \"age\" > 25 OR \"name\" = 'Jerry' )"
           (generate-sql :postgres fields {:where [:and [:!= [:field 3] nil] [:or [">" [:field 4] 25] [:= [:field 2] "Jerry"]]]})))
    (is (= "SELECT * FROM data WHERE \"age\" IN ( 25 , 26 , 27 )"
           (generate-sql :postgres fields {:where [:= [:field 4] 25 26 27]})))
    (is (= "SELECT * FROM data WHERE \"name\" = 'cam'"
           (generate-sql :postgres fields {:where [:= [:field 2] "cam"]})))
    (is (= "SELECT * FROM data WHERE `name` = 'cam' LIMIT 10"
           (generate-sql :mysql fields {:where [:= [:field 2] "cam"], :limit 10})))
    (is (= "SELECT * FROM data LIMIT 20"
           (generate-sql :postgres fields {:limit 20})))
    (is (= "SELECT TOP 20 * FROM data"
           (generate-sql :sqlserver fields {:limit 20}))))

  (testing "is-empty and not-empty"
    (is (= "SELECT * FROM data WHERE \"age\" IS NULL"
           (generate-sql :postgres fields {:where [:is-empty [:field 4]]})))
    (is (= "SELECT * FROM data WHERE \"age\" IS NOT NULL"
           (generate-sql :postgres fields {:where [:not-empty [:field 4]]}))))

  (testing "macros"
    (is (= "SELECT * FROM data WHERE \"id\" < 5 AND \"name\" = 'joe'"
           (generate-sql :postgres fields {:where [:and [:< [:field 1] 5] [:macro "is_joe"]]} :macros macros))))
    (is (= "SELECT * FROM data WHERE \"id\" < 5 AND ( \"age\" > 18 AND \"name\" = 'joe' )"
           (generate-sql :postgres fields {:where [:and [:< [:field 1] 5] [:macro "is_old_joe"]]} :macros macros)))
    (is (thrown? IllegalArgumentException
                 (generate-sql :postgres fields {} :macros circular-macros))))

