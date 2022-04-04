(ns git-server.sql (:import (java.sql Connection)))

(defn execute
  ([db string]
   (let [stmt (.createStatement db)]
     (.executeUpdate stmt string)
     (.close stmt)))
  ([db string params]
   (let [stmt (.prepareStatement db string)]
     (dotimes [i (.length params)]
       (.setObject stmt (+ i 1) (nth params i)))
     (.execute stmt)
     (.close stmt))))

(defn query-single
  ([db string]
   (let [stmt (.createStatement db)
         res (.executeQuery stmt string)
         obj (if (.next res) (.getObject res 1))]
     (.close stmt)
     obj))
  ([db string params]
   (let [stmt (.prepareStatement db string)]
     (dotimes [i (.length params)]
       (.setObject stmt (+ i 1) (nth params i)))
     (let [res (.executeQuery stmt)
           obj (if (.next res) (.getObject res 1))]
       (.close stmt)
       obj))))

(defn generate-insert-statement [table columns n]
  (let [red (fn [x y] (str x ", " y))
        s
        (str "INSERT INTO " table " (" (reduce red columns) ") VALUES"
             (reduce (fn [x y] (str x "," y))
                     (map (fn [x] (str " (" x ")")) (repeat n (reduce red (repeat (.length columns) "?")))))
             " ON CONFLICT DO NOTHING;")]
    s))

(set! *warn-on-reflection* true)
(defn insert [^Connection db ^String table-name ^clojure.lang.PersistentVector column-names ^clojure.lang.PersistentVector rows]
  (let [n (quot 999 (.length column-names))
        rs (.length rows)
        cs (.length column-names)]
    (let [stmt (.prepareStatement db (generate-insert-statement table-name column-names n))]
      (dotimes [i (quot rs n)]
        (dotimes [j (* n cs)]
          (.setObject stmt (+ j 1) (nth (nth rows (+ (* i n) (quot j cs)))
                                        (mod j cs))))
        (.addBatch stmt))
      (.executeBatch stmt)
      (.close stmt))
    (if (not (= (mod rs n) 0))
      (let [stmt (.prepareStatement db (generate-insert-statement table-name column-names (mod rs n)))
            base (- rs (mod rs n))]
        (dotimes [j (* cs (mod rs n))]
          (.setObject stmt (+ j 1) (nth (nth rows (+ base (quot j cs)))
                                      (mod j cs))))
        (.executeUpdate stmt)
        (.close stmt)))))
(set! *warn-on-reflection* false)

(defn process-result-set [res]
  (let [cs (.getColumnCount (.getMetaData res))]
    (loop [ret []]
      (if (.next res)
        (recur (conj ret (mapv (fn [i]
                                 (.getObject res (+ i 1)))
                               (range cs))))
        ret))))

(defn query
  ([db query]
   (let [stmt (.createStatement db)
         ret (process-result-set (.executeQuery stmt query))]
     (.close stmt)
     ret))
  ([db query params]
   (let [stmt (.prepareStatement db query)]
     (dotimes [i (.length params)]
       (.setObject stmt (+ i 1) (nth params i)))
     (let [ret (process-result-set (.executeQuery stmt))]
       (.close stmt)
       ret))))

(defn explain-execute
  ([db string]
   (doseq [line (query db (str "EXPLAIN ANALYZE " string))]
     (println "EXPLAIN" (first line))))
  ([db string params]
   (doseq [line (query db (str "EXPLAIN ANALYZE " string) params)]
     (println "EXPLAIN" (first line)))))
