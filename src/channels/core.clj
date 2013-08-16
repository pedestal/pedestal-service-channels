(ns channels.core
  (:require [clojure.core.async :as async :refer [<!! >!! <! >! go chan close! thread]]))

;; #TODO add error fn to interceptor definition and emulate current interceptor logic (?)

(defn channel? [c] (instance? clojure.core.async.impl.protocols.Channel c))

(defn make-processor
  [interceptors]
  (fn
    [ctx]
    (go
     (try
       (loop [{enter :enter leave :leave} (first interceptors)
              interceptors (rest interceptors)
              leave-stack nil
              ctx ctx]
         (let [res (if enter (try (enter ctx)
                                  (catch Throwable t t))
                       ctx)
               real-res (if (channel? res)
                          (<! res)
                          res)]
           (when (instance? Throwable real-res)
             (throw real-res))
           (if (or (empty? interceptors) (real-res :response))
             (do ;; have to unwind leave stack here
               (println "REVERSING")
               (loop [leave (peek leave-stack)
                      leave-stack (pop leave-stack)
                      ctx real-res]
                 (let [res (if leave (try (leave ctx)
                                          (catch Throwable t t))
                               ctx)
                       real-res (if (channel? res)
                                  (<! res)
                                  res)]
                   (when (instance? Throwable real-res)
                     (throw real-res))
                   (if-not (empty? leave-stack)
                     (recur (peek leave-stack) (pop leave-stack) real-res)
                     real-res))))
             (recur (first interceptors) (rest interceptors) (conj leave-stack leave) real-res))))
       (catch Throwable t
         t)))))

(defn processor
  [ctx]
  (go
   (try
     (loop [ctx ctx]
       (let [queue (ctx ::queue)
             {enter :enter leave :leave} (peek queue)
             stack (ctx ::stack)
             new-ctx (-> ctx
                         (assoc ::queue (pop queue))
                         (assoc ::stack (conj stack leave)))]
         (let [res (if enter (try (enter new-ctx)
                                  (catch Throwable t t))
                       new-ctx)
               real-res (if (channel? res)
                          (<! res)
                          res)]
           (when (instance? Throwable real-res)
             (throw real-res))
           (if (or (empty? queue) (real-res :response))
             (do ;; have to unwind leave stack here
               (println "REVERSING")
               (loop [ctx real-res]
                 (let [stack (ctx ::stack)
                       leave (peek stack)
                       new-ctx (-> ctx
                                   (assoc ::stack (when-not (empty? stack) (pop stack))))]
                   (let [res (if leave (try (leave new-ctx)
                                            (catch Throwable t t)) new-ctx)
                         real-res (if (channel? res)
                                    (<! res)
                                    res)]
                     (when (instance? Throwable real-res)
                       (throw real-res))
                     (if (empty? stack)
                       real-res
                       (recur real-res))))))
             (recur real-res)))))
     (catch Throwable t
       t))))

(defn exception? [e] (instance? Throwable e))

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

(def test-fns {:enter (fn [ctx] (println "test-fns enter") (update-in ctx [:enter] (fnil inc 0)))
               :leave (fn [ctx] (println "test-fns leave") (update-in ctx [:leave] (fnil inc 0)))})

(def test-fns-async {:enter (fn [ctx]
                              (println "test-fns-async enter")
                              (thread
                               (Thread/sleep 3000)
                               (update-in ctx [:enter] (fnil inc 0))))
                     :leave (fn [ctx]
                              (println "test-fns-async leave")
                              (thread
                               (Thread/sleep 3000)
                               (update-in ctx [:leave] (fnil inc 0))))})

(def test-fns-async-except {:enter (fn [ctx]
                                     (println "test-fns-async enter")
                                     (thread
                                      (try
                                        (Thread/sleep 3000)
                                        (throw (ex-info "Ow!" {}))
                                        (update-in ctx [:enter] (fnil inc 0))
                                        (catch Throwable t t))))})

(def test-fns-except {:enter (fn [ctx] (println "test-fns-except enter") (throw (ex-info "Ow!" {})))})

(def test-fns-channel-except {:enter (fn [ctx] (println "test-fns-channel-except enter")
                                       (let [pipe (chan)]
                                         (go
                                          (>! pipe (ex-info "Ow!" {}))
                                          (close! pipe))
                                         pipe))})

(def test-fns-short-circuit {:enter (fn [ctx]
                                      (println "test-fns-short-circuit enter")
                                      (assoc ctx :response {:body "done early!"}))})

(def test-fns-events {:enter (fn [ctx]
                               (println "test-fns-events enter")
                               (let [pipe (chan)]
                                 (go
                                  (dotimes [i 10]
                                    (Thread/sleep 2000)
                                    (>! pipe (str "event " i)))
                                  (close! pipe))
                                 (assoc ctx :response {:body pipe})))})

(def test-handler {:enter (fn [ctx]
                            (println "test-handler enter")
                            (Thread/sleep 5000)
                            (assoc ctx :response {:body "done!"}))})

(def double-routes {:enter (fn [ctx] (println "double-route enter") (update-in ctx [:enter] (fnil #(* % 2) 1)))
                    :leave (fn [ctx] (println "double-route leave") (update-in ctx [:leave] (fnil #(* % 2) 1)))})

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

(def test-router-1 {:enter (fn [ctx]
                             (let [matched-route (matched-route ctx routes)
                                   ctx (assoc ctx :route matched-route)
                                   route-interceptors (:interceptors matched-route)]
                               ((make-processor route-interceptors) ctx)))}) ;; adds another go block

(def test-router-2 {:enter (fn [ctx]
                             (let [matched-route (matched-route ctx routes)
                                   ctx (assoc ctx :route matched-route)
                                   route-interceptors (:interceptors matched-route)]
                               (println "MATCHED ROUTE" matched-route)
                               (println "ROUTE INTERCEPTORS" route-interceptors)
                               (update-in ctx [::queue]
                                          (fnil into clojure.lang.PersistentQueue/EMPTY)
                                          route-interceptors)))}) ;; adds interceptors to existing go block

(def double-request {:request {:path "double"}})
(def quadruple-request {:request {:path "quadruple"}})

(comment
  (def proc (make-processor [test-fns test-fns test-router-1]))

  ;; Returns Unexpected result, since there is no request path
  (process-result (proc {:request nil}))
  ;; Returns Unexpected result, since there is an invalid request path
  (process-result (proc {:request {:path "foo"}}))
  (process-result (proc double-request))
  (process-result (proc quadruple-request))

  (def proc (make-processor [test-fns test-fns test-fns-async test-router-1]))
  (process-result (proc double-request))

  (def proc (make-processor [test-fns test-fns test-fns-async test-fns-except test-router-1]))
  (process-result (proc double-request))

  (def proc (make-processor [test-fns test-fns test-fns-channel-except test-router-1]))
  (process-result (proc double-request))

  (def proc (make-processor [test-fns test-fns test-fns-async test-fns-events test-router-1]))
  (process-result (proc double-request))

  ;; Returns Unexpected result, since there is no request path
  (process-result
   (processor
    {::queue (into clojure.lang.PersistentQueue/EMPTY [test-fns test-fns test-router-2])}))

  (process-result
   (processor
    (merge double-request {::queue (into clojure.lang.PersistentQueue/EMPTY [test-fns test-fns test-router-2])})))

  (process-result
   (processor
    (merge double-request {::queue (into clojure.lang.PersistentQueue/EMPTY [test-fns test-fns test-fns-channel-except test-router-2])})))

  (process-result
   (processor
    (merge double-request {::queue (into clojure.lang.PersistentQueue/EMPTY [test-fns test-fns test-fns-async test-router-2])})))

  (process-result
   (processor
    (merge double-request {::queue (into clojure.lang.PersistentQueue/EMPTY [test-fns test-fns test-fns-async test-fns-except test-router-2])})))

  (process-result
   (processor
    (merge double-request {::queue (into clojure.lang.PersistentQueue/EMPTY [test-fns test-fns test-fns-async test-fns-events test-router-2])})))
  )
