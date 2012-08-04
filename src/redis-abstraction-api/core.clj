;; AUTHOR : Naveen Nandan

(ns redis-abstraction-api.core
  (:gen-class)
  (:require [clojure.walk :as walk])
  (:require [labs.redis.core :as redis])
  (:require [redis.core :as redis-security])
  (:require [clojure.set]))



;; This function applies CRC-32 on a string passed to it.
;; Input : key - string to use as pass code
;;         value - string to apply crc-32
(defn generate-hash [key value]
  (def crc32 (new java.util.zip.CRC32))
  (.reset crc32)
  (.update crc32 (.getBytes key) 0 (count (.getBytes key)))
  (.update crc32 (.getBytes value) 0 (count (.getBytes value)))
  (java.lang.Long/toHexString (.getValue crc32)))



;; This function is used to reset the password to redis.
;; Input : password - current password to authenticate
;;         db - db name
(defn reset-password [password db]
  (try
    (redis-security/with-server
      {:host "127.0.0.1" :port 6379 :db db :password (generate-hash db password)}
      (redis-security/config "set" "requirepass" "")
      (redis-security/quit))
    (catch Exception e
      (println "password reset declined")
      (throw e))))



;; This function is used to set a password to redis.
;; Input : password - new password to set
;;         db - db name
(defn set-password [password db]
  (try
    (redis-security/with-server
      {:host "127.0.0.1" :port 6379 :db db}
      (redis-security/config "set" "requirepass" (generate-hash db password))
      (redis-security/quit))
    (catch Exception e
      (println "password set declined")
      (throw e))))



;; This function is used to change the password of redis.
;; Input : old-password - old password to authenticate
;;         new-password - new password to set
;;         db - db name
(defn change-password [old-password new-password db]
  (try
    (redis-security/with-server
      {:host "127.0.0.1" :port 6379 :db db :password (generate-hash db old-password)}
      (redis-security/config "set" "requirepass" (generate-hash db new-password))
      (redis-security/quit))
    (catch Exception e
      (println "password change declined")
      (throw e))))



;; This function is used to create a redis client by establishing a connection - without any authentication.
;; Input : db - db name
;; Output : redis client
(defn get-connection [db]
  (try
    (let [connection (redis/client)]
      (if (> (count @@(redis/select connection db)) 2)
        (if (= "ERR" (subs @@(redis/select connection db) 0 3))
          (throw (Exception. "connection refused"))))
      (redis/select connection db)
      connection)
    (catch Exception e
      (println (.getMessage e))
      (throw e))))



;; This function is used to create a redis client through an authenticated connection.
;; Input : password - password to connect to redis
;;         db - db name
;; Output : redis client
(defn get-secure-connection [password db]
  (try
    (let [connection (redis/client)]
      (if (= "OK" @@(redis/auth connection (generate-hash db password)))
        (redis/select connection db)
        (:else
         (throw (Exception. "incorrect password"))))
        connection)
    (catch Exception e
      (println (.getMessage e))
      (throw e))))



;; This function is used to initialize the redis client to be used throughout the namespace.
;; Input : return value from get-connection / get-secure-connection
(defn init-client [connection]
  (def client connection))



;; This funciton is used to retrieve the lookup key for class-id.
;; Input : class-id
(defn >class-id [class-id]
  (str "class:" class-id))



;; This function is used to retrieve the lookup key for objects that are currently being processed.
;; Input : class-id
(defn >objects-in-process [class-id]
  (str "objects-in-process:class:" class-id))



;; This function is used to retrieve the lookup key for already processed objects of a class.
;; Input : class-id
(defn >processed-objects [class-id]
  (str "processed-objects:class:" class-id))



;; This function is used to retrieve the lookup key for the classes being monitored for a particular class type.
;; Input : class-type
(defn >classes-monitored [class-type]
  (str "classes-monitored:" class-type))



;; This function is used to retrieve the lookup key for the classes that are currently being processed.
;; Input : class-type
(defn >classes-in-process [class-type]
  (str "classes-in-process:" class-type))



;; This function is used to retrieve the lookup key for the objects that belong to a particular class.
;; Input : class-id
(defn >class-objects [class-id]
  (str "class:" class-id ":objects"))



;; This function is used to retrieve the lookup key for an object.
;; Input : object-id
(defn >object [object-id]
  (str "object:" object-id))



;; This function is used to retrieve the class map.
;; Input : class-id
;; Output : map containing class_id, etc
(defn get-classes [class-id]
  (try
    (walk/keywordize-keys (apply hash-map (redis/->>str @(redis/hgetall client (>class-id class-id)))))
    (catch Exception e
      (println "cannot retrieve classes")
      (throw e))))



;; This function is used to grab new classes from the queue based on their score.
;; Input : num-classes - number of classes to take
;;         class-type
;; Output : list of top classes from the class queue
(defn take-new-classes [num-classes class-type]
  (try
    (let [classes (redis/->>str @(redis/zrangebyscore client (>classes-monitored class-type) "-inf" "+inf" "LIMIT" 0 num-classes))]
        (map get-classes classes))
    (catch Exception e
      (println "no new classes")
      (throw e))))



;; This function is used to update the status of a class.
;; Input : class - map that specifies the updates
(defn update-class-status [class]
  (try
    (apply redis/hmset client (>class-id (:class_id class)) class)
    (catch Exception e
      (println "cannot update")
      (throw e))))



;; This function is used to insert classes that are currently being processed.
;; Input : class
(defn process-class [class]
  (try
    @(redis/watch client (>class-id (:class_id class)))
    (redis/atomically client
                      (redis/zadd client (>classes-in-process (:class_type class)) (System/currentTimeMillis) (:class_id class))
                      (update-class-status {:class_id (:class_id class) :status 1}))
    (catch Exception e
      (println "unable to insert class")
      (throw e))))



;; This function is used to remove classes that are not in process.
;; Input : class-id
;;         class-type
(defn remove-class [class-id class-type]
  (try
    @(redis/watch client (>classes-in-process class-id))
    (redis/atomically client
                      (redis/zrem client (>classes-in-process class-type) class-id)
                      (update-class-status {:class_id class-id :status 0}))
    (catch Exception e
      (println "unable to remove class")
      (throw e))))



;; This function is used to update the score of a class.
;; Input : class-id
;;         score
;;         class-type
(defn update-class-score [class-id score class-type]
  (try
    @(redis/watch client (>classes-monitored class-type))
    (redis/atomically client
                      (redis/zadd client (>classes-monitored class-type) time-out class-id))
    (catch Exception e
      (println "no class with this id")
      (throw e))))



;; This function is used to retrieve the score of a class.
;; Input : class-id
;;         class-type
;; Output : class score
(defn check-class-score [class-id class-type]
  (try
    (read-string (redis/->>str @(redis/zscore client (>classes-monitored class-type) class-id)))
    (catch Exception e
           (println "no such class")
           (throw e))))



;; This function is used to update a class.
;; Input : class - map that specifies the updates
(defn update-class [class]
  (try
    @(redis/watch client (>class-id (:class_id class)))
    (redis/atomically client
                      (apply redis/hmset client (>class-id (:class_id class)) class))
    (catch Exception e
      (println "cannot update")
      (throw e))))



;; This function is used to obtain the value of the globally incremented object-counter.
;; Input : none
(defn increment-object-id []
  (try
    @(redis/watch client "object::counter")
    (redis/atomically client
                      (redis/hincrby client "object::counter" "id" 1))
    (catch Exception e
      (println "cannot increment counter")
      (throw e))))


;; This function is used to get the latest value of object-counter.
;; Input : none
(defn get-new-object-id []
  (try
    (increment-object-id)
    (redis/->>str @(redis/hget client "object::counter" "id"))
    (catch Exception e
      (println "unable to retrieve object counter")
      (throw e))))



;; This function is used to update object's index.
;; Input : object - an object map
(defn update-objects-index [object]
  (try
    (redis/hmset client (>object (:object_id object)) "class_id" (:class_id object))
    (catch Exception e
      (println "unable to update object's index")
      (throw e))))



;; This function is used to update class-objects index.
;; Input : object - an object map
(defn update-class-objects-index [object]
  (try
    (redis/sadd client (>class-objects (:class_id object)) (:object_id object))
    (catch Exception e
      (println "unable to update class-objects index")
      (throw e))))



;; This function is used to store the objects which are currently being processed.
;; Input : object - an object map (required keys are class_id, object_id)
(defn insert-objects-in-process [object]
  (try
    (let [current_object (walk/keywordize-keys object)]
      @(redis/watch client
                    (>objects-in-process (:class_id current_object))
                    (>object (:object_id current_object))
                    (>class-objects (:class_id current_object)))
      (redis/atomically client
                        (redis/zadd client (>objects-in-process (:class_id current_object)))
                        (update-objects-index current_object)
                        (update-class-objects-index current_object)))
    (catch Exception e
      (println "cannot insert objects")
      (throw e))))



;; This function is used to retrieve the objects of a class which are currently being processed.
;; Input : class-id
;; Output : set of objects in a class that are currently being processed
(defn get-objects-in-process [class-id]
  (try
    (set (redis/->>str
     @(redis/zrange client (>objects-in-process class-id) 0 (redis/->>str @(redis/zcard client (>objects-in-process class-id))))))
    (catch Exception e
      (println "unable to retrieve objects")
      (throw e))))



;; This function is used to store the latest objects of a class.
;; Input : object - an object map (required keys are class_id)
(defn insert-processed-objects [object]
  (try
      (let [current_object (walk/keywordize-keys object)]
        @(redis/watch client (>processed-objects (:class_id current_object)) (>objects-in-process (:class_id current_object)))
        (redis/atomically client
                          (redis/zadd client (>processed-objects (:class_id current_object)))
                          (redis/zrem client (>objects-in-process (:class_id current_object)))))
    (catch Exception e
      (println "cannot insert objects")
      (throw e))))



;; This function is used to retrieve the objects of a class that have been processed.
;; Input : class-id
;; Output : set of objects in a class that have been processed
(defn get-processed-objects [class-id]
  (try
    (set (redis/->>str
     @(redis/zrange client (>processed-objects class-id) 0 (redis/->>str @(redis/zcard client (>processed-objects class-id))))))
    (catch Exception e
      (println "unable to retrieve objects")
      (throw e))))



;; This function is used to find all the objects that belong to a class that are currently in the cache (either processed / in-process).
;; Input : class-id
;; Output : set of all objects belonging to a class in cache
(defn get-all-objects [class-id]
  (try
    (clojure.set/union (get-objects-in-process class-id) (get-processed-objects class-id))
    (catch Exception e
      (println "unable to retrieve objects")
      (throw e))))



;; This function is used to identify the new objects in a class, the ones that are not either being processed or already been processed.
;; Input : objects - a set containing all current objects present in a class
;; Output : set of new objects in class
(defn class-object-filter [class-id objects]
  (try
    (clojure.set/difference objects (clojure.set/intersection objects (get-all-objects class-id)))
    (catch Exception e
      (println "unable to filter the objects")
      (throw e))))



;; This function is used to identify the outdated objects that have already been processed.
;; Input : objects - a set containing all current objects present in a class
;; Output : set of processed objects that are no more present in the class
(defn processed-objects-not-in-class [class-id objects]
  (try
    (clojure.set/difference (get-processed-objects class-id) (clojure.set/intersection objects (get-processed-objects class-id)))
    (catch Exception e
      (println "unable to retrieve objects")
      (throw e))))



;; This function is used to remove the outdated objects of a class.
;; Input : class-id
;;         objects - set of objects which are present in current class
(defn remove-outdated-objects [class-id objects]
  (try
    (doseq [object (processed-objects-not-in-class class-id objects)]
      @(redis/watch client (>processed-objects class-id))
      (redis/atomically client
                        (redis/zrem client (>processed-objects class-id) object)))
    (catch Exception e
      (println "cannot remove objects")
      (throw e))))
