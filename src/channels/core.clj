(ns channels.core
  (:require [clojure.core.async :as async :refer [<!! >!! <! >! go chan close!]]))

;; 1) demonstrate routing w/ interceptor that matches something in request and invokes another vector of interceptors
;; 2) update make-processor to detect if interceptor threw exception OR returned exception through channel and rethrow

(defn channel? [c] (instance? clojure.core.async.impl.protocols.Channel c))


(defn make-processor
  [fs]
  (fn 
    [ctx]
    (let [enters (map first fs)
          leaves (reverse (map second fs))]
      (go 
       (try
         (let [entered (loop [f (first enters)
                              fs (rest enters)
                              ctx ctx]
                         (let [res (f ctx)
                               real-res (if (channel? res)
                                          (<! res)
                                          res)]
                           (println "Entering" f real-res (nil? (real-res :response)) (empty? fs))
                           (if (or (empty? fs) (real-res :response))
                             (do (println "real-res" real-res)
                                 real-res)
                             (do (println "recurring")
                                 (recur (first fs) (rest fs) real-res)))))]
           (println "Entered" entered)
           (if (nil? (entered :response))
             (do (println "Processing")
                 (let [processed (do (Thread/sleep 5000)
                                     (assoc entered :response {:body "done!"}))]
                   (println "Processed")
                   (loop [f (first leaves)
                          fs (rest leaves)
                          ctx processed]
                     (let [res (f ctx)
                           real-res (if (channel? res)
                                      (<! res)
                                      res)]
                       (println "Leaving" f real-res)
                       (if-not (empty? fs)
                         (recur (first fs) (rest fs) real-res)
                         real-res)))))
             (do (println "Short-circuiting")
                 entered)))
         (catch Exception ex
           ex))))))

(defn exception? [e] (instance? Exception e))

(defn process-result
  [in]
  (let [result (<!! in)]
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

(def test-fns [(fn [ctx] (update-in ctx [:enter] (fnil inc 0)))
               (fn [ctx] (update-in ctx [:leave] (fnil inc 0)))])

(def test-fns-async [(fn [ctx] (go (Thread/sleep 3000) (update-in ctx [:enter] (fnil inc 0))))
                     (fn [ctx] (go (Thread/sleep 3000) (update-in ctx [:leave] (fnil inc 0))))])

(def test-fns-except [(fn [ctx] (throw (ex-info "Ow!" {})))])

(def test-fns-short-circuit [(fn [ctx] (assoc ctx :response {:body "done early!"}))])

(def test-fns-events [(fn [ctx]
                        (let [pipe (chan)]
                          (go
                           (dotimes [i 10]
                             (Thread/sleep 2000)
                             (>! pipe (str "event " i)))
                           (close! pipe))
                          (assoc ctx :response {:body pipe})))])


(comment

(def proc (make-processor [test-fns test-fns]))
(process-result (proc {}))

(def proc (make-processor [test-fns test-fns test-fns-async]))
(process-result (proc {}))

(def proc (make-processor [test-fns test-fns test-fns-async test-fns-except]))
(process-result (proc {}))

(def proc (make-processor [test-fns test-fns test-fns-async test-fns-events]))
(process-result (proc {}))

)