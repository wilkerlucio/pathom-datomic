(ns com.wsscode.pathom.connect.datomic.client
  (:require [datomic.client.api :as d]))

(def client-config
  {:com.wsscode.pathom.connect.datomic/datomic-driver-q  d/q
   :com.wsscode.pathom.connect.datomic/datomic-driver-db d/db})
