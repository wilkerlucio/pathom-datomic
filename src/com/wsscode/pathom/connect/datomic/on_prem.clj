(ns com.wsscode.pathom.connect.datomic.on-prem
  (:require [datomic.api :as d]))

(def on-prem-config
  {:com.wsscode.pathom.connect.datomic/datomic-driver-q  d/q
   :com.wsscode.pathom.connect.datomic/datomic-driver-db d/db})
