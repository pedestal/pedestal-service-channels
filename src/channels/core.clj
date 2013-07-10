(ns channels.core
  (:require [clojure.core.async :as async :refer [<!! >!! <! >! go chan close! thread]]))

;; #TODO update make-processor to detect if interceptor threw exception OR returned exception through channel
;;   a) rethrow it
;;   b) add error fn to interceptor definition (may want to switch from vector to map) and emulate current interceptor logic (?)

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
                         (thread
                           (Thread/sleep 3000)
                           (>!! res (update-in ctx [:enter] (fnil inc 0))))
                         (go (<! res))))
                     (fn [ctx]
                       (println "test-fns-async leave")
                       (let [res (chan)]
                         (thread
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

(def double-routes [(fn [ctx] (println "double-route enter") (update-in ctx [:enter] (fnil #(* % 2) 1)))
                    (fn [ctx] (println "double-route leave") (update-in ctx [:leave] (fnil #(* % 2) 1)))])

(def routes [{:route-name :double
              :path "double"
              :interceptors [double-routes test-handler]}
             {:route-name :quadruple
              :path "quadruple"
              :interceptors [double-routes double-routes test-handler]}])

(defn matched-route
  [{{path :path :as request} :request :as ctx} routes]
  (when path
    (if-let [matched-route (some #(when (= path (:path %)) %) routes)]
      matched-route)))

(def test-router-1 [(fn [ctx] 
                      (let [matched-route (matched-route ctx routes)
                            ctx (assoc ctx :route matched-route)
                            route-interceptors (:interceptors matched-route)]
                        ((make-processor route-interceptors) ctx)))]) ;; adds another go block

(def test-router-2 [(fn [ctx] 
                      (let [matched-route (matched-route ctx routes)
                            ctx (assoc ctx :route matched-route)
                            route-interceptors (:interceptors matched-route)]
                        (println "MATCHED ROUTE" matched-route)
                        (println "ROUTE INTERCEPTORS" route-interceptors)
                        (update-in ctx [::queue]
                                   (fnil into clojure.lang.PersistentQueue/EMPTY)
                                   route-interceptors)))]) ;; adds interceptors to existing go block

(def double-request {:request {:path "double"}})
(def quadruple-request {:request {:path "quadruple"}})

(comment
  (def proc (make-processor [test-fns test-fns test-router-1]))

  (process-result (proc {:request nil}))
  (process-result (proc {:request {:path "foo"}}))
  (process-result (proc double-request))
  (process-result (proc quadruple-request))

  ;; (def proc (make-processor [test-fns test-fns test-fns-async test-handler] ::router router))
  ;; (process-result (proc quadruple-request))

  ;; (def proc (make-processor [test-fns test-fns test-fns-async test-fns-except] ::router router))
  ;; (process-result (proc quadruple-request))

  ;; (def proc (make-processor [test-fns test-fns test-fns-async test-fns-events] ::router router))
  ;; (process-result (proc quadruple-request))

  (process-result
   (processor
    {::queue (into clojure.lang.PersistentQueue/EMPTY [test-fns test-fns test-router-2])}))

  (process-result
   (processor
    (merge double-request {::queue (into clojure.lang.PersistentQueue/EMPTY [test-fns test-fns test-router-2])})))

  ;; (process-result
  ;;  (processor
  ;;   (merge quadruple-request {::queue (into clojure.lang.PersistentQueue/EMPTY [test-fns test-fns test-handler]) ::router router})))

  ;; (process-result
  ;;  (processor
  ;;   (merge quadruple-request {::queue (into clojure.lang.PersistentQueue/EMPTY [test-fns test-fns test-fns-async test-handler]) ::router router})))

  ;; (process-result
  ;;  (processor
  ;;   (merge quadruple-request {::queue (into clojure.lang.PersistentQueue/EMPTY [test-fns test-fns test-fns-async test-fns-except]) ::router router})))

  ;; (process-result
  ;;  (processor
  ;;   (merge quadruple-request {::queue (into clojure.lang.PersistentQueue/EMPTY [test-fns test-fns test-fns-async test-fns-events]) ::router router})))
  )
