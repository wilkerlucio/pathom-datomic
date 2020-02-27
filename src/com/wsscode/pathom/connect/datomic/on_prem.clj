(ns com.wsscode.pathom.connect.datomic.on-prem)

(defn- lazily [sym]
  (let [f-thunk (delay (require (symbol (namespace sym)))
                       (resolve sym))]
    (fn [& args]
      (apply @f-thunk args))))

(def on-prem-config
  {:com.wsscode.pathom.connect.datomic/datomic-driver-q  (lazily 'datomic.api/q)
   :com.wsscode.pathom.connect.datomic/datomic-driver-db (lazily 'datomic.api/db)})
