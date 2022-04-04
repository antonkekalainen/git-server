(ns git-server.handler
  (:require [compojure.core :refer :all]
            [compojure.route :as route]
            [ring.middleware.defaults :refer [wrap-defaults site-defaults]]
            [ring.util.io :refer [piped-input-stream]]
            [clojure.string :refer [index-of]]
            [clojure.core.match :refer :all]
            [clojure.set :refer [union]]
            [git-server.sql :refer :all]))

(defn read-config [file-path]
  (let [props (java.util.Properties.)]
    (with-open [file (java.io.FileInputStream. file-path)]
      (.load props file))
    props))

(let [f (java.io.File. "./pack")]
  (.mkdir f))

(def config (read-config "server.properties"))

(def db-path (.getProperty config "jdbc_path" "jdbc:postgresql://localhost/git?user=git&password=git_password"))
(def deflate-level 1)

(def inflate-buffer-size 8192)
(def deflate-buffer-size 1024)

;; How many objects should we store at once?
(def objects-to-store 10000)
(defrecord receive-state [db unexpanded-deltas total-bytes ready-objects ready-map])

(defn write [out n])

(def hex-chars "0123456789abcdef")

(defn hex [x len]
  (if (= len 0)
    (if (or (= x 0) (= x -1))
      ""
      (throw (Exception. (str "hex: overflow (" x ")"))))
    (str (hex (bit-shift-right x 4) (- len 1))
         (nth hex-chars (bit-and x 15)))))

(defn from-hex [^String s]
  (doseq [c s]
    (if (= nil (index-of hex-chars c))
      (throw (Exception. (str "Invalid hex value: " s)))))
  (loop [i (- (.length s) 1)
         x 1
         y 0]
    (if (>= i 0)
      (recur (- i 1) (* x 16)
             (+ y (* x (index-of hex-chars (nth s i)))))
      y)))

(defn lines [lines]
  (reduce
   str
   (map (fn [line]
          (case line
            :flush "0000"
            :delimit "0001"
            :end "0002"
            (str (hex (+ (.length line) 5) 4)
                 line
                 "\n")))
        lines)))

(defn byte-array= [^"[B" a ^"[B" b]
  (if (not (= (alength a) (alength b)))
    false
    (loop [i 0]
      (if (= i (alength a))
        true
        (if (= (aget a i) (aget b i))
          (recur (+ i 1))
          false)))))

(defn compare-bytes [^"[B" a ^"[B" b]
  (if (not (= (alength a) (alength b)))
    (throw (Exception. "bytestrings are not of equal length in compare-bytes"))
    (loop [i 0]
      (if (= i (alength a))
        0
        (if (= (aget a i) (aget b i))
          (recur (+ i 1))
          (- (aget a i) (aget b i)))))))

(defn bytes-to-hex [^"[B" hash]
  (let [len (alength hash)]
    (loop [i 0
           output ""]
      (if (= i len)
        output
        (let [byte (aget hash i)]
          (recur (+ i 1)
                 (str output
                      (nth hex-chars (bit-and 15 (bit-shift-right byte 4)))
                      (nth hex-chars (bit-and 15 byte)))))))))

(defn write-string [out str]
  (let [bs (.getBytes str)]
    (write out bs 0 (alength bs))))

(defn read-varint [in]
  (let [byte (.read in)
        value (bit-and byte 0x7F)
        cont-bit (bit-shift-right byte 7)]
    (if (= cont-bit 0)
      value
      (bit-or value (bit-shift-left (read-varint in) 7)))))

(defn pkt-len [n]
  (case n
    :flush "0000"
    :delimit "0001"
    :end "0002"
    (hex (+ n 4) 4)))

(defn write-pkt-header [out n]
  (write-string out (pkt-len n)))

(deftype sideband-stream [out])
(defn write
  ([out bytes offset length]
   (if (instance? sideband-stream out)
     (do (println length)
         (write-pkt-header (.out out) (+ length 1))
         (.write (.out out) 1)
         (.write (.out out) bytes offset length))
     (.write out bytes offset length)))
  ([out bytes]
   (write out bytes 0 (alength bytes))))

(defn finish-sideband-stream [out]
  (write-string "0000" (.out out)))

(defn read-pkt-header [in]
  (let [bytes (byte-array 4)]
    (.read in bytes 0 4)
    (let [val (from-hex (String. bytes))]
      (cond
        (= val 0)
        :flush
        (= val 1)
        :delim
        (= val 2)
        :end
        (> val 4)
        (- val 4)
        true
        (throw (Exception. (str "Unexpected PKT-LEN: " (String. bytes))))))))

(defn write-pkt-line [out line]
  (let [bytes (if (string? line) (.getBytes line) line)]
    (write-pkt-header out (alength bytes))
    (write out bytes)))

(defn sign-byte [unsigned-byte]
  (if (> unsigned-byte 127)
    (- unsigned-byte 256)
    unsigned-byte))

(defn bytes-from-hex [^String hash]
  (let [bytes (byte-array (quot (.length hash) 2))]
    (doseq [i (range (quot (.length hash) 2))]
      (aset-byte bytes i (sign-byte (from-hex (.substring hash (* i 2) (* (+ i 1) 2))))))
    bytes))

(defn add-hash-to-object [object]
  (let [^java.security.MessageDigest digest (java.security.MessageDigest/getInstance "SHA-1")]
    (.update digest
             (.getBytes
              (str (case (:type object)
                     :blob "blob"
                     :tree "tree"
                     :commit "commit")
                   " "
                   (alength (:data object)))))
    (.update digest (byte-array 1))
    (.update digest (:data object))
    (assoc object :hash (.digest digest))))

(def upload-capabilities "side-band-64k object-format=sha1 agent=test-server")
(def receive-capabilities "thin-pack no-done push-options report-status side-band-64k delete-refs quiet atomic object-format=sha1 agent=test-server")

(defn send-ref-advertisements [out capabilities lines]
  (doseq [l lines]
    (println "Advertising ref:" (String. l)))
  (let [fst (if (empty? lines)
              (.getBytes "0000000000000000000000000000000000000000 capabilities^{}")
              (first lines))
        cap-bytes (.getBytes capabilities)]
    (write-pkt-header out (+ 2 (alength fst) (alength cap-bytes)))
    (.write out fst)
    (.write out 0)
    (.write out cap-bytes)
    ;; Newline
    (.write out 0x0A)
    (doseq [line (next lines)]
      (write-pkt-header out (+ (alength line) 1))
      (.write out line)
      (.write out 0x0A))
    (write-string out "0000")))

(defn advertise-refs [request]
  (let [out (java.io.ByteArrayOutputStream.)
        service (:service (:params request))]
    (write-pkt-line out (str "# service=" service "\n"))
    (write-pkt-header out :flush)
    (send-ref-advertisements
     out
     (case service
       "git-upload-pack" upload-capabilities
       "git-receive-pack" receive-capabilities)
     (with-open [db (java.sql.DriverManager/getConnection db-path)]
       (let [head (query-single db "SELECT hash FROM refs WHERE repo = ? AND name = 'refs/heads/master';" [(:repo (:route-params request))])
             all (mapv (fn [ref]
                        (.getBytes (str (bytes-to-hex (nth ref 0)) " " (nth ref 1))))
                      (query db "SELECT hash, name FROM refs WHERE repo = ? ORDER BY name;" [(:repo (:route-params request))]))]
         (if head
           (cons (.getBytes (str (bytes-to-hex head) " HEAD")) all)
           all))))
    {:status 200
     :body (java.io.ByteArrayInputStream. (.toByteArray out))
     :headers {"Content-Type" (str "application/x-" service "-advertisement")}}))

(defn read-line-bytes [input]
  (let [len-bytes (byte-array 4)
        count (.read input len-bytes 0 4)]
    (case count
      -1 nil
      4 (let [code (from-hex (String. len-bytes))]
          (case code
            0 :flush
            1 :delimit
            2 :end
            (let [len (- code 4)
                  buf (byte-array len)]
              (loop [read 0]
                (if (< read len)
                  (let [n (.read input buf read (- len read))]
                    (if (= n -1)
                      (throw (Exception. "Partial Git line"))
                      (recur (+ read n))))
                  buf))))))))

(defn read-line [input]
  (let [r (read-line-bytes input)]
    (case r
      nil nil
      :flush :flush
      :delimit :delimit
      :end :end
      (String. r))))

(defn read-section
  ([input delimiter]
   (loop [lines []]
     (let [line (read-line input)]
       (cond
         (nil? line) (throw (Exception. "Partial section"))
         (= line delimiter) lines
         true (recur (conj lines line))))))
  ([input] (read-section input :delimit)))

(defn read-lines
  ([input lines]
   (let [len-bytes (byte-array 4)
         count (.read input len-bytes 0 4)]
     (cond
       (= count -1) lines
       (= count 0) lines
       (not (= count 4)) (throw (Exception. (str "Partial length in Git line (" count "/4)")))
       true
       (let [line-len (- (from-hex (String. len-bytes)) 4)]
         (cond
           (= line-len -4) (read-lines input (conj lines :flush))
           (= line-len -3) (read-lines input (conj lines :delimit))
           (= line-len -2) (conj lines :end)
           true
           (let [line-bytes (byte-array line-len)
                 line-count (.read input line-bytes 0 line-len)]
             (if (not (= line-count line-len))
               (throw (Exception. (str "Partial Git line " line-count "/" line-len))))
             (println "line=" (String. line-bytes))
             (read-lines input (conj lines (String. line-bytes)))))))))
  ([input] (read-lines input [])))

(defn parse-argument [str]
  (match (re-find #"^(\S+)(( \S+)*)$" str)
         [_ name params _]
         (loop [m (re-matcher #"^ (\S+)" params)
                results [(keyword name)]]
           (match (re-find m)
             [_ param] (recur m (conj results param))
             nil results))))

(defn parse-commit-parents [commit]
  (let [m (re-matcher #"((\S+) (.*)\n)|\n" commit)]
    (loop [parents []]
      (let [f (re-find m)]
        (cond
          (= f nil)
          (throw (Exception. "No match"))
          (= (nth f 0) "\n")
          parents
          (or (= (nth f 2) "parent")
              (= (nth f 2) "tree"))
          (recur (conj parents (bytes-from-hex (nth f 3))))
          true
          (recur parents))))))

;; It's a bit strange to refer to the items in a tree as being its parents
;; It would be a bit more appropriate to refer to them as dependencies instead
(defn parse-tree-parents [tree]
  (let [b (java.io.ByteArrayInputStream. tree)]
    (loop [hashes []]
      (if (> (.available b) 0)
        (do
          (loop []
            ;; Read until space (file mode)
            (when (not (= (.read b) 0x20))
              (recur)))
          (loop []
            ;; Read until NUL terminator (file name)
            (when (not (= (.read b) 0))
              (recur)))
          (let [hash (byte-array 20)]
            (.read b hash 0 20)
            (recur (conj hashes hash))))
        hashes))))

(defn concat-strict [head tail]
  (loop [h head
         t tail]
    (if (empty? t)
      h
      (recur (conj h (first t)) (next t)))))

(defn compress [bytes]
  (let [def (java.util.zip.Deflater.)
        bs (byte-array deflate-buffer-size)
        out (java.io.ByteArrayOutputStream.)]
    (.setLevel def deflate-level)
    (.setInput def bytes)
    (.finish def)
    (loop []
      (if (.finished def)
        (.toByteArray out)
        (let [n (.deflate def bs)]
          (.write out bs 0 n)
          (recur))))))

(defn uncompress [compressed size]
  (let [inf (java.util.zip.Inflater.)
        out (byte-array size)]
    (.setInput inf compressed)
    (.inflate inf out)
    (if (not (= (.getRemaining inf) 0))
      (throw (Exception. "uncompress data left")))
    (if (not (.finished inf))
      (throw (Exception. "partial uncompress")))
    out))

(defn get-compressed-object [obj]
  (if (:compressed obj)
    (:compressed obj)
    (compress (:data obj))))

(defn write-pack-index [db pack-name direct delta]
  (insert db "objects" ["hash" "pack" "type" "compressed_size" "size" "location"] (mapv (fn [x] [(:hash x) pack-name (name (:type x)) (:compressed-size x) (:size x) (:offset x)]) direct))
  (insert db "deltas" ["hash" "pack" "base" "compressed_size" "size" "location"] (mapv (fn [x] [(:hash x) pack-name (:base x) (:compressed-size x) (:size x) (:offset x)]) delta)))

(defn write-internal-pack [db objects]
  (let [pack-id (hex (int (rand 0x7FFFFFFF)) 8)
        out (java.io.ByteArrayOutputStream.)]
      (loop [objs objects
             bytes-written 0
             direct []
             delta []]
        (if (empty? objs)
          (do (with-open [writer (java.io.FileOutputStream. (str "packs/" pack-id))]
                (.write writer (.toByteArray out)))
              (write-pack-index db pack-id direct delta))
          (let [obj (first objs)
                next-objs (next objs)]
            (if (= 0 (query-single db "SELECT COUNT(*) FROM objects WHERE hash = ?;" [(:hash obj)])
                     (query-single db "SELECT COUNT(*) FROM deltas WHERE hash = ?;" [(:hash obj)]))
              (if (:base obj)
                (do (.write out (:delta obj))
                    (recur next-objs (+ bytes-written (alength (:delta obj)))
                           direct
                           (conj delta {:offset bytes-written
                                        :compressed-size (alength (:delta obj))
                                        :size (alength (:delta obj))
                                        :hash (:hash obj)
                                        :base (:hash (:base obj))})))
                (let [c (compress (:data obj))]
                  (.write out c)
                  (recur next-objs (+ bytes-written (alength c))
                         (conj direct {:offset bytes-written
                                       :compressed-size (alength c)
                                       :size (alength (:data obj))
                                       :hash (:hash obj)
                                       :type (:type obj)})
                         delta)))))))))

(defn store-objects [db objs]
  (println "Storing" (.length objs) "objects")
  (loop [os objs
         parents []]
    (if (empty? os)
      (do
          (println "Writing relationships to disk...")
          (insert db "parents" ["child" "parents"] parents)
          (println "Writing pack")
          (write-internal-pack db objs)
          (println "Committing...")
          (.commit db)
          (println "Done!"))
      (let [obj (first os)]
        (cond
          (= (:type obj) :commit)
          (recur (next os)
                 (conj parents [(:hash obj) (into-array (class (byte-array 0)) (parse-commit-parents (String. (:data obj))))]))
          (= (:type obj) :tree)
          (recur (next os)
                 (conj parents [(:hash obj) (into-array (class (byte-array 0)) (parse-tree-parents (:data obj)))]))
          true
          (recur (next os) parents))))))

(defn decode-delta-instruction [^java.io.InputStream in ^"[B" base ^"[B" buf ^Integer bytes-written]
  (let [op (.read in)]
    ;; The MSB for the first byte determines whether this is a copy or add instruction
    (if (= (bit-shift-right op 7) 1)
      ;; Copy
      (loop [offset 0
             n 0
             bits op]
        (if (= n 4)
          ;; Once the four bytes of offset have been read, we read the size
          (loop [size 0
                 n 0
                 bits bits]
            (if (= n 3)
              ;; Now that we have the entire offset and size
              ;; we can perform the copy
              (let [real-size (if (= size 0)
                                0x10000
                                size)]
                (System/arraycopy
                 base offset
                 buf bytes-written
                 real-size)
                (+ bytes-written real-size))
              (recur
               ;; Is size byte #n included?
               (if (= (bit-and bits 1) 1)
                 (+ size
                    (bit-shift-left (.read in)
                                    (* n 8)))
                 size)
               (+ n 1)
               (bit-shift-right bits 1))))
          (recur
           ;; Is offset byte #n included?
           (if (= (bit-and bits 1) 1)
             ;; Yes
             (+ offset
                (bit-shift-left (.read in)
                                (* n 8)))
             ;; No, skip this byte
             offset)
           (+ n 1)
           (bit-shift-right bits 1))))
      ;; Add
      (let [size (bit-and 0x7F op)]
        (if (= size 0)
          (throw (Exception. "Reserved instruction in delta")))
        (if (not (= (.read in buf bytes-written size) size))
          (throw (Exception. "Read too small")))
        (+ bytes-written size)))))

(defn expand-delta [delta base]
  (let [in (java.io.ByteArrayInputStream. delta)
        base-size (read-varint in)
        new-size (read-varint in)
        new (byte-array new-size)]
    (if (not (= base-size (alength (:data base))))
      (throw (Exception. (str "Size doesn't match for base object in delta (" (alength (:data base)) "/" base-size ")"))))
    (loop [bytes-written 0]
      (if (= bytes-written new-size)
        (add-hash-to-object {:type (:type base) :data new})
        (recur (try (decode-delta-instruction in (:data base) new bytes-written)
                    (catch Exception e
                      (println (str "Exception when performing delta expansion, wrote " bytes-written "/" new-size " bytes, " (.available in) " bytes left in instruction stream"))
                      (throw e))))))))

(defn delta? [x]
  (or (= (:type x) :ref-delta)
      (= (:type x) :ofs-delta)))

(defn receive-object [state object])

(defn receive-objects [state objects]
  (loop [st state
         objs objects]
    (if (empty? objs)
      st
      (recur (receive-object st (first objs)) (next objs)))))

(defn add-hashes [state hashes]
  ;; Using a loop here prevents a stack overflow
  (loop [state state
         hashes hashes]
    (if (empty? hashes)
      state
      (let [hash (first hashes)
            objects (find (:unexpanded-deltas state) hash)]
        (if objects
          (recur (receive-objects (update state :unexpanded-deltas #(dissoc % hash)) (nth objects 1)) (next hashes))
          (recur state (next hashes)))))))

(defn flush-objects [state]
  (let [hashes (mapv (fn [x] (:hash x)) (:ready-objects state))]
    (println "Storing" (.length (:ready-objects state)) "totaling" (:total-bytes state) "bytes")
    (when (not (empty? (:ready-objects state)))
      (store-objects (:db state) (:ready-objects state)))
    (assoc (assoc (assoc state :ready-objects []) :total-bytes 0) :ready-map (sorted-map-by compare-bytes))))

;; Flushing objects might resolve new deltas, so if we actually want to flush everything to disk we'll have to retry
(defn flush-all-objects [state]
  (loop [st state]
    (if (empty? (:ready-objects st))
      st
      (recur (flush-objects st)))))

(defn get-single-object [db hash]
  (let [b (first (query db "SELECT pack, compressed_size, size, location, type FROM objects WHERE hash = ?;" [hash]))]
    (if b
      (let [file (java.io.FileInputStream. (str "packs/" (nth b 0)))
            buf (byte-array (nth b 1))]
        (.skip file (nth b 3))
        (.read file buf 0 (nth b 1))
        (.close file)
        {:type (keyword (nth b 4))
         :hash (into-array Byte/TYPE hash)
         :data (into-array Byte/TYPE (uncompress buf (nth b 2)))})
      (let [d (first (query db "SELECT pack, compressed_size, size, location, base FROM deltas WHERE hash = ?;" [hash]))]
        (if d
          (let [b (get-single-object db (nth d 4))
                buf (byte-array (nth d 1))
                file (java.io.FileInputStream. (str "packs/" (nth d 0)))]
            (.skip file (nth d 3))
            (.read file buf 0 (nth d 1))
            (.close file)
            (merge (expand-delta buf b)
                   {:base b
                    :delta (into-array Byte/TYPE buf)})))))))

(defn get-refs [db want have]
  (execute db "CREATE TEMP TABLE hashes (hash BYTEA PRIMARY KEY);")
  (execute db "INSERT INTO hashes WITH RECURSIVE ancestors(hash) AS (SELECT unnest(?) UNION SELECT unnest(parents) FROM parents INNER JOIN ancestors ON hash = child) SELECT hash FROM ancestors;" [(into-array (class (byte-array 0)) want)])
  (let [r (cons (query-single db "SELECT COUNT(*) FROM hashes;")
                (filter (fn [x] x)
                        (map (fn [x] (get-single-object db (nth x 0)))
                             (query db "SELECT hash FROM hashes;"))))]
    (execute db "DROP TABLE hashes;")
    r))

(defn receive-object [state object]
  (if (delta? object)
    (let [b (find (:ready-map state) (:base-hash object))]
      (if b
        (let [bb (nth b 1)]
          (receive-object state (merge (expand-delta (:data object) bb)
                                       {:base bb
                                        :delta (:data object)})))
        (let [base (get-single-object (:db state) (:base-hash object))]
          (if base
            (receive-object
             state
             (merge (expand-delta (:data object) base)
                    {:base base
                     :delta (:data object)}))
            (update state :unexpanded-deltas #(update % (:base-hash object) (fn [x] (conj x (:data object)))))))))
    (if (>= (.length (:ready-objects state)) objects-to-store)
      (receive-object (flush-objects state) object)
      (let [unresolved (find (:unexpanded-deltas state) (:hash object))
            next (update (update (update state :ready-objects #(conj % object))
                                 :total-bytes #(+ % (alength (:data object))))
                         :ready-map #(assoc % (:hash object) object))]
        (if unresolved
          (do (println "Unresolved!") (receive-objects (update next :unexpanded-deltas #(dissoc % (:hash object)))
                           (mapv (fn [delta] (expand-delta delta object)) (nth unresolved 1))))
          next)))))

(defn write-varint [out int]
  (if (> int 0x7F)
    (do
      (.write out (bit-or 0x80 (bit-and int 0x7F)))
      (write-varint out (bit-shift-right int 7)))
    (.write out int)))

(defn write-type-and-size [out type size]
  (let [ty (case (keyword type)
             :commit 1
             :tree 2
             :blob 3
             :tag 4
             :ofs-delta 6
             :ref-delta 7)
        byte (bit-or (bit-shift-left ty 4)
                     (bit-and size 0x0F))]
  (if (> size 0x0F)
    (do (.write out (bit-or byte 0x80))
        (write-varint out (bit-shift-right size 4)))
    (.write out byte))))

(defn create-packfile [out count objects]
  (let [bs (byte-array 4)
        buf (java.nio.ByteBuffer/wrap bs)
        o (java.io.ByteArrayOutputStream. 8)]
    (.write o (.getBytes "PACK") 0 4)
    ;; Version
    (.putInt buf 0 2)
    (.write o bs 0 4)
    (println count)
    (.putInt buf 0 count)
    (.write o bs 0 4)
    (let [digest (java.security.MessageDigest/getInstance "SHA-1")]
      (.update digest (.toByteArray o))
      (write out (.toByteArray o))
      (.reset o)
      (doseq [obj objects]
        (write-type-and-size o (:type obj) (alength (:data obj)))
        (.update digest (.toByteArray o))
        (write out (.toByteArray o))
        (.reset o)
        (let [c (get-compressed-object obj)]
          (write out c)
          (.update digest c)))
      (let [digest-bytes (.digest digest)]
        (write out digest-bytes 0 (alength digest-bytes)))))
  (finish-sideband-stream out))

(defn send-packfile [out db want have]
  (let [r (get-refs db want have)]
    (create-packfile out (first r) (next r))))
    
(defn read-int [in]
  (let [bytes (byte-array 4)
        buf (java.nio.ByteBuffer/wrap bytes)]
    (.read in bytes 0 4)
    (.getInt buf)))

(defn read-type-and-size [in]
  (let [byte (.read in)
        size (bit-and 15 byte)
        type-id (bit-and 7 (bit-shift-right byte 4))
        cont-bit (bit-shift-right byte 7)]
    [(case type-id
       1 :commit
       2 :tree
       3 :blob
       4 :tag
       6 :ofs-delta
       7 :ref-delta)
    (if (= cont-bit 0)
      size
      (bit-or size (bit-shift-left (read-varint in) 4)))]))

(defn read-compressed-object [in size]
  (let [inflater (java.util.zip.Inflater. false)
        object (byte-array size)
        buf (byte-array inflate-buffer-size)
        compressed-buffer (java.io.ByteArrayOutputStream. inflate-buffer-size)]
    (loop [read 0
           buf-size 0]
      (if (.finished inflater)
        (if (= read size)
          (do
            (.unread in buf (- buf-size (.getRemaining inflater)) (.getRemaining inflater))
            {:data object :compressed (.toByteArray compressed-buffer)})
          (throw (Exception. "Compressed object is of wrong size")))
        (do
          (let [n (.read in buf)]
            (.setInput inflater buf 0 n)
            (.write compressed-buffer buf 0 n)
            (recur (+ read (.inflate inflater object read (- (alength object) read)))
                   n)))))))

(defn read-pack-file [in]
  (let [header-bytes (byte-array 4)]
    (.read in header-bytes 0 4)
    (when (not (= (String. header-bytes) "PACK"))
      (throw (Exception. "Invalid packfile")))
    (let [version (read-int in)
          num-entries (read-int in)]
      (when (not (or (= version 2)
                     (= version 3)))
        (throw (Exception. "Invalid packfile version")))
      (println num-entries "objects in packfile")
      (with-open [db (java.sql.DriverManager/getConnection db-path)]
        (.setAutoCommit db false)
        (execute db "CREATE TEMP TABLE tmp_parents (child BYTEA PRIMARY KEY, parents BYTEA ARRAY NOT NULL);")
        (loop [count num-entries
               state (receive-state. db (sorted-map-by compare-bytes) 0 [] (sorted-map-by compare-bytes))]
          (if (not (= count 0))
            (let [type-and-size (read-type-and-size in)
                  type (nth type-and-size 0)
                  size (nth type-and-size 1)]
              (if (= type :ofs-delta)
                (throw (Exception. "ofs-delta")))
              (if (= type :ref-delta)
                (let [hash (byte-array 20)]
                  (.read in hash)
                  (let [delta (try (read-compressed-object in size)
                                   (catch java.util.zip.DataFormatException e
                                     (println "DataFormatException after reading" count "objects")
                                     (println "Object type is" type)
                                     (throw e)))]
                    (recur (- count 1) (receive-object state (merge delta {:type type :base-hash hash})))))
                (recur
                 (- count 1)
                 (receive-object state (add-hash-to-object (assoc (read-compressed-object in size) :type type))))))
            (do (let [final-state (flush-all-objects state)]
                  (println "ready=" (:ready-objects final-state))
                  (when (not (empty? (:unexpanded-deltas final-state)))
                    (throw (Exception. (str (.size (:unexpanded-deltas final-state)) " deltas are missing their bases")))
                    (println (bytes-to-hex (nth (first (:unexpanded-deltas final-state)) 0))))))))
        (.commit db))
      (let [b (byte-array 20)]
        ;; TODO: Check the hash

        (.read in b 0 20)))))

(defn read-pkt-line [in]
  (let [len-bytes (byte-array 4)]
    (.read in len-bytes 0 4)
    (let [line-len (from-hex (String. len-bytes))]
      (cond
        (= line-len 0)
        :flush
        (= line-len 1)
        :delimit
        (= line-len 2)
        :end
        true
        (let [line (byte-array (- line-len 4))]
          (.read in line 0 (- line-len 4))
          line)))))

(defn read-capability-line [in]
  ;; Since the protocol documentation says that the client MUST send at least one want line
  ;; one may be fooled into thinking that the canonical client will send one.
  ;; If the client considers a push to be large it will first make an empty request in order to
  ;; ensure that it won't make a request with insufficient authentication.
  ;; (in git/remote_curl.c:915: post_rpc calls probe_rpc if large_request is set)
  (let [line (read-pkt-line in)]
    (if (= line :flush)
      :flush
      (let [split (loop [i 0]
                    (cond
                      (= i (alength line))
                      (throw (Exception. (str "Invalid capabilities line: " (String. line))))
                      (= (aget line i) 0)
                      i
                      true (recur (+ i 1))))]
        ;; TODO: Handle capabilities
        (String. line 0 split)))))

(defn read-capability-section [in]
  ;; TODO: Return capabilities
  (loop [lines []
         line (read-capability-line in)]
    (if (= line :flush)
      lines
      (recur (conj lines line) (read-pkt-line in)))))

(defn write-status-line [out status line]
  (let [bs (if (string? line) (.getBytes line) line)]
    (write-pkt-header out (+ (alength bs) 1))
    (.write out (case status
                  :data 1
                  :status 2
                  :error 3))
    (.write out bs)))

(def zero-id "0000000000000000000000000000000000000000")

(defn git-receive-pack [request]
  (let [in (java.io.PushbackInputStream. (:body request) inflate-buffer-size)
        out (java.io.ByteArrayOutputStream.)
        commands (read-capability-section in)]
    (if (= commands [])
      ;; The client doesn't actually care what we give it here
      ;; its only checking for authentication.
      {:status 200
       :headers {"Content-Type" "application/x-git-receive-pack-response"}
       :body "0000"}
      (do
        (read-pack-file in)
        (println "Objects stored")
        ;; TODO: Handle errors in packfile
        (write-pkt-line out "unpack ok\n")
        (with-open [db (java.sql.DriverManager/getConnection db-path)]
          (doseq [command commands]
            (let [prev (.substring command 0 40)
                  next (.substring command 41 81)
                  name (.substring command 82)
                  repo (:repo (:route-params request))
                  curr (query db "SELECT hash FROM refs WHERE repo = ? AND name = ?;" [repo name])]
              (println "Updating ref" name "from" prev "to" next)
              (if (= prev (if (empty? curr)
                            zero-id
                            (:hash (nth curr 0))))
                (do (write-pkt-line out (str "ok " name "\n"))
                    (if (empty? curr)
                      (execute db "INSERT INTO refs (repo, name, hash) VALUES (?, ?, ?);" [repo name (bytes-from-hex next)])
                      (execute db "UPDATE refs SET hash = ? WHERE repo = ? AND name = ?;" [(bytes-from-hex next) repo name])))
                (write-pkt-line out (str "ng " name " old ref doesn't match"))))))
        (write-pkt-header out :flush)
        (let [bs (.toByteArray out)
              out2 (java.io.ByteArrayOutputStream.)] ;;(java.nio.ByteBuffer/allocate (+ (alength bs) 9))]
          (write-status-line out2 :data bs)
          (write-pkt-header out2 :flush)
          {:status 200
           :headers {"Content-Type" "application/x-git-receive-pack-response"}
           :body (java.io.ByteArrayInputStream. (.toByteArray out2))})))))
  
(defn read-upload-wants [in]
  (loop [refs []]
    (let [length (read-pkt-header in)]
      (cond
        (= length :flush)
        refs
        (>= length 45)
        (let [bytes (byte-array 40)]
          (.read in bytes 0 5)
          (when (not (= (String. bytes 0 5) "want "))
            (throw (Exception. (str "Expected 'want', got " (String. bytes)))))
          (.read in bytes 0 40)
          (.read in (byte-array (- length 45)))
          (recur (conj refs (bytes-from-hex (String. bytes)))))
        true
        (throw (Exception. (str "Expected 'want', but client sent " (let [bytes (byte-array length)]
                                                                      (.read in bytes)
                                                                      (String. bytes)))))))))

(defn read-upload-haves [in]
  (loop [refs []]
    (let [length (read-pkt-header in)]
      (if (= length :flush)
        {:refs refs :done false}
        (let [bytes (byte-array length)]
          (.read in bytes)
          (cond
            (or (= length 4) (= length 5))
            (do
              (when (not (= (String. bytes 0 4) "done"))
                (throw (Exception. (str "Expected 'done', got " (String. bytes)))))
              {:refs refs :done true})
            (or (= length 45) (= length 46))
            (do
              (when (not (= (String. bytes 0 5) "have "))
                (throw (Exception. (str "Expected 'have', got " (String. bytes)))))
              (recur (conj refs (bytes-from-hex (String. bytes 5 40)))))))))))

(defn git-upload-pack [request]
  (let [in (:body request)
        want-refs (read-upload-wants in)
        haves (read-upload-haves in)
        have-refs (:refs haves)
        done? (:done haves)]
    {:status 200
     :headers {"Content-Type" "application/x-git-upload-pack-response"}
     :body
     (piped-input-stream
      (fn [out]
        (with-open [db (java.sql.DriverManager/getConnection db-path)]
            (loop [refs have-refs]
              (if (empty? refs)
                (do (write-pkt-line out "NAK\n")
                    (send-packfile (sideband-stream. out) db want-refs have-refs))
                (let [ref (first refs)
                      found (query-single db ["SELECT COUNT(*) AS count FROM objects WHERE hash = ?;" (bytes-from-hex ref)])]
                  (if (= found 0)
                    (recur (next refs))
                    (do
                      (write-pkt-line out (str "ACK " ref "\n"))
                      (send-packfile (sideband-stream. out) db want-refs have-refs)))))))))}))

(defroutes app-routes
  (GET "/repo/:repo/info/refs" [repo service]
       (case service
         "git-upload-pack" advertise-refs
         "git-receive-pack" advertise-refs
         nil))
  (POST "/repo/:repo/git-receive-pack" [repo]
        git-receive-pack)
  (POST "/repo/:repo/git-upload-pack" [repo]
        git-upload-pack)
  (route/not-found (fn [req] (println req) "Not Found")))

(def app
  (wrap-defaults app-routes (merge site-defaults {:security {:anti-forgery false}})))
