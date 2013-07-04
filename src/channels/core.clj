(ns channels.core
  (:require [clojure.core.async :as async :refer [<!! >!! <! >! go chan close!]]))

;; 1) demonstrate routing w/ interceptor that matches something in request and invokes another vector of interceptors
;; 2) update make-processor to detect if interceptor threw exception OR returned exception through channel and rethrow
;; 3) fix short-circuit code so that subset of leave fns run - "unwinding" the "stack"

(defn channel? [c] (instance? clojure.core.async.impl.protocols.Channel c))


(defn make-processor
  [interceptors]
  (fn 
    [ctx]
    (go 
     (try
       (loop [[enter leave] (first interceptors)
              interceptors (rest interceptors)
              leave-stack nil
              ctx ctx]
         (let [res (if enter (enter ctx) ctx)
               real-res (if (channel? res)
                          (<! res)
                          res)]
           (if (or (empty? interceptors) (real-res :response))
             (do ;; have to unwind leave stack here
               (println "REVERSING")
               (loop [leave (peek leave-stack)
                      leave-stack (pop leave-stack)
                      ctx real-res]
                 (let [res (if leave (leave ctx) ctx)
                       real-res (if (channel? res)
                                  (<! res)
                                  res)]
                   (if-not (empty? leave-stack)
                     (recur (peek leave-stack) (pop leave-stack) real-res)
                     real-res))))
             (recur (first interceptors) (rest interceptors) (conj leave-stack leave) real-res))))
       (catch Exception ex ex)))))

(defn processor
  [ctx]
  (go 
   (try
     (loop [ctx ctx]
       (let [queue (ctx ::queue)
             [enter leave] (peek queue)
             stack (ctx ::stack)
             new-ctx (-> ctx
                         (assoc ::queue (pop queue))
                         (assoc ::stack (conj stack leave)))]
         (let [res (if enter (enter new-ctx) new-ctx)
               real-res (if (channel? res)
                          (<! res)
                          res)]
           (if (or (empty? queue) (real-res :response))
             (do ;; have to unwind leave stack here
               (println "REVERSING")
               (loop [ctx real-res]
                 (let [stack (ctx ::stack)
                       leave (peek stack)
                       ctx real-res
                       new-ctx (-> ctx
                                   (assoc ::stack (when-not (empty? stack) (pop stack))))]
                   (let [res (if leave (leave new-ctx) new-ctx)
                         real-res (if (channel? res)
                                    (<! res)
                                    res)]
                     (if (empty? stack)
                       real-res
                       (recur real-res))))))
             (recur real-res)))))
     (catch Exception ex ex))))

(defn exception? [e] (instance? Exception e))

(defn process-result
  [in]
  (let [result (<!! in)]
    (println "RESULT" result (type result))
    (if-not (exception? result)
      (if-let [body (get-in result [:response :body])]
        (cond
         (string? body) (println body)
         (channel? body) (loop [event (<!! body)]
                           (when event
                             (println event)
                             (recur (<!! body))))
         :else
         (println "Unexpected body" body))
        (println "Unexpected result" result))
      (println result))))

(def test-fns [(fn [ctx] (println "test-fns enter") (update-in ctx [:enter] (fnil inc 0)))
               (fn [ctx] (println "test-fns leave") (update-in ctx [:leave] (fnil inc 0)))])

(def test-fns-async [(fn [ctx]
                       (println "test-fns-async enter")
                       (let [res (chan)]
                         (future
                           (Thread/sleep 3000)
                           (>!! res (update-in ctx [:enter] (fnil inc 0))))
                         (go (<! res))))
                     (fn [ctx]
                       (println "test-fns-async leave")
                       (let [res (chan)]
                         (future
                           (Thread/sleep 3000)
                           (>!! res (update-in ctx [:leave] (fnil inc 0))))
                         (go (<! res))))])

(def test-fns-except [(fn [ctx] (println "test-fns-except enter") (throw (ex-info "Ow!" {})))])

(def test-fns-short-circuit [(fn [ctx]
                               (println "test-gns-short-circuit enter")
                               (assoc ctx :response {:body "done early!"}))])

(def test-fns-events [(fn [ctx]
                        (println "test-fns-events enter")
                        (let [pipe (chan)]
                          (go
                           (dotimes [i 10]
                             (Thread/sleep 2000)
                             (>! pipe (str "event " i)))
                           (close! pipe))
                          (assoc ctx :response {:body pipe})))])

(def test-handler [(fn [ctx]
                     (println "test-handler enter")
                     (Thread/sleep 5000)
                     (assoc ctx :response {:body "done!"}))])

(comment

(def proc (make-processor [test-fns test-fns test-handler]))
(process-result (proc {}))

(def proc (make-processor [test-fns test-fns test-fns-async test-handler]))
(process-result (proc {}))

(def proc (make-processor [test-fns test-fns test-fns-async test-fns-except]))
(process-result (proc {}))

(def proc (make-processor [test-fns test-fns test-fns-async test-fns-events]))
(process-result (proc {}))

(process-result
 (processor
  {::queue (into clojure.lang.PersistentQueue/EMPTY [test-fns test-fns test-handler])}))

(process-result
 (processor
  {::queue (into clojure.lang.PersistentQueue/EMPTY [test-fns test-fns test-fns-async test-handler])}))

(process-result
 (processor
  {::queue (into clojure.lang.PersistentQueue/EMPTY [test-fns test-fns test-fns-async test-fns-except])}))

(process-result
 (processor
  {::queue (into clojure.lang.PersistentQueue/EMPTY [test-fns test-fns test-fns-async test-fns-events])}))


)