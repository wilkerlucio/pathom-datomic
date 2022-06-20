(ns com.wsscode.pathom.connect.datomic.on-prem
  (:require [com.wsscode.pathom.connect.datomic.helper :as helper]))

(def on-prem-config
  {:com.wsscode.pathom.connect.datomic/datomic-driver-q  (helper/lazily 'datomic.api/q)
   :com.wsscode.pathom.connect.datomic/datomic-driver-db (helper/lazily 'datomic.api/db)})
