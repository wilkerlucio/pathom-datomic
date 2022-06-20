(ns com.wsscode.pathom.connect.datomic.helper)

(defn lazily [sym]
  (let [f-thunk (delay (require (symbol (namespace sym)))
                       (resolve sym))]
    (fn [& args]
      (apply @f-thunk args))))
