(ns com.wsscode.pathom.connect.datomic.client
  (:require [com.wsscode.pathom.connect.datomic.helper :as helper]))

(def client-config
  {:com.wsscode.pathom.connect.datomic/datomic-driver-q  (helper/lazily 'datomic.client.api/q)
   :com.wsscode.pathom.connect.datomic/datomic-driver-db (helper/lazily 'datomic.client.api/db)})
