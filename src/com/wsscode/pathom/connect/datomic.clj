(ns com.wsscode.pathom.connect.datomic
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [com.wsscode.pathom.connect :as pc]
            [com.wsscode.pathom.connect.indexes :as pci]
            [com.wsscode.pathom.connect.planner :as pcp]
            [com.wsscode.pathom.core :as p]
            [com.wsscode.pathom.sugar :as ps]
            [edn-query-language.core :as eql])
  (:import [org.slf4j LoggerFactory Logger]))

(defonce logger ^Logger (LoggerFactory/getLogger "pathom-datomic"))

(s/def ::db any?)
(s/def ::schema (s/map-of ::p/attribute map?))
(s/def ::schema-keys (s/coll-of ::p/attribute :kind set?))
(s/def ::schema-uniques ::schema-keys)
(s/def ::ident-attributes ::schema-keys)
(s/def ::whitelist
  (s/or :whitelist (s/coll-of ::p/attribute :kind set?)
        :all-all #{::DANGER_ALLOW_ALL!}))

(s/def ::schema-entry
  (s/keys
    :req [:db/ident :db/id :db/valueType :db/cardinality]
    :opt [:db/doc :db/unique]))

(s/def ::schema (s/map-of :db/ident ::schema-entry))

(defn raw-datomic-q [{::keys [datomic-driver-q]} & args]
  (.debug logger "{}" args)
  (apply datomic-driver-q args))

(defn raw-datomic-db [{::keys [datomic-driver-db]} conn]
  (datomic-driver-db conn))

(defn allowed-attr? [{::keys [whitelist]} attr]
  (or (and (set? whitelist) (contains? whitelist attr))
      (= ::DANGER_ALLOW_ALL! whitelist)))

(defn db-id-allowed? [config]
  (allowed-attr? config :db/id))

(defn db->schema
  "Extracts the schema from a Datomic db."
  [env db]
  (->> (raw-datomic-q env '[:find (pull ?e [* {:db/valueType [:db/ident]}
                                            {:db/cardinality [:db/ident]}])
                            :where
                            [_ :db.install/attribute ?e]
                            [?e :db/ident ?ident]]
         db)
       (map first)
       (reduce
         (fn [schema entry]
           (if (allowed-attr? env (:db/ident entry))
             (assoc schema (:db/ident entry) entry)
             schema))
         {})))

(defn schema->uniques
  "Return a set with the ident of the unique attributes in the schema."
  [schema]
  (->> schema
       vals
       (filter :db/unique)
       (into #{} (map :db/ident))))

(defn- prop [k]
  {:type :prop :dispatch-key k :key k})

(defn inject-ident-subqueries [{::keys [ident-attributes]} query]
  (->> query
       eql/query->ast
       (p/transduce-children
         (map (fn [{:keys [key query] :as ast}]
                (if (and (contains? ident-attributes key) (not query))
                  (assoc ast :type :join :query [:db/ident] :children [(prop :db/ident)])
                  ast))))
       eql/ast->query))

(defn pick-ident-key
  "Figures which key to use to request data from Datomic. This will
  try to pick :db/id if available, returning the number directly.
  Otherwise will look for some attribute that is a unique and is on
  the map, in case of multiple one will be selected by random. The
  format of the unique return is [:attribute value]."
  [{::keys [schema-uniques] :as config} m]
  (if (and (contains? m :db/id)
           (db-id-allowed? config))
    (:db/id m)

    (let [available (set/intersection schema-uniques (into #{} (keys m)))]
      (if (seq available)
        [(first available) (get m (first available))]))))

(def post-process-entity-parser
  (p/parser {::p/env {::p/reader [(fn [{::keys [ident-attributes]
                                        :keys  [ast query]
                                        :as    env}]
                                    (let [k (:key ast)]
                                      (if (and (contains? ident-attributes k) (not query))
                                        (get-in (p/entity env) [k :db/ident])
                                        ::p/continue)))
                                  p/map-reader]}}))

(defn post-process-entity
  "Post process the result from the datomic query. Operations that it does:

  - Pull :db/ident from ident fields"
  [{::keys [ident-attributes]} subquery entity]
  (post-process-entity-parser
    {::p/entity         entity
     ::ident-attributes ident-attributes}
    subquery))

(defn node-subquery [{::pcp/keys [node]}]
  (eql/ast->query (::pcp/foreign-ast node)))

(defn datomic-resolve
  "Runs the resolver to fetch Datomic data from identities."
  [config
   {::keys [db]
    :as    env}]
  (let [id       (pick-ident-key config (p/entity env))
        subquery (node-subquery env)]
    (cond
      (nil? id) nil

      (integer? id)
      (post-process-entity env subquery
        (ffirst
          (raw-datomic-q config [:find (list 'pull '?e (inject-ident-subqueries config subquery))
                                 :in '$ '?e]
            db
            id)))

      (p/ident? id)
      (let [[k v] id]
        (post-process-entity env subquery
          (ffirst
            (raw-datomic-q config [:find (list 'pull '?e (inject-ident-subqueries config subquery))
                                   :in '$ '?v
                                   :where ['?e k '?v]]
              db
              v)))))))

(defn entity-subquery
  "Using the current :query in the env, compute what part of it can be
  delegated to Datomic."
  [{:keys [query] ::pc/keys [indexes] ::pcp/keys [node] :as env}]
  (let [graph        (pcp/compute-run-graph
                       (assoc indexes
                         :edn-query-language.ast/node
                         (->> query eql/query->ast (pcp/prepare-ast env))

                         ::pcp/available-data {:db/id {}}))
        datomic-node (pcp/get-node graph (-> graph ::pcp/index-syms (get `datomic-resolver) first))
        subquery     (node-subquery {::pcp/node datomic-node})]
    (conj subquery :db/id)))

(defn query-entities
  "Use this helper from inside a resolver to run a Datomic query.

  You must send dquery using a datalog map format. The :find section
  of the query will be populated by this function with [[pull ?e SUB_QUERY] '...].
  The SUB_QUERY will be computed by Pathom, considering the current user sub-query.

  Example resolver (using Datomic mbrainz sample database):

      (pc/defresolver artists-before-1600 [env _]
        {::pc/output [{:artist/artists-before-1600 [:db/id]}]}
        {:artist/artists-before-1600
         (pcd/query-entities env
           '{:where [[?e :artist/name ?name]
                     [?e :artist/startYear ?year]
                     [(< ?year 1600)]]})})

  Notice the result binding entities must be named as `?e`.

  Them the user can run queries like:

      [{:artist/artists-before-1600
        [:artist/name
         {:artist/country
          :not-in/datomic
          [:country/name]}]}]

  The sub-query will be send to Datomic, filtering out unsupported keys
  like `:not-in/datomic`."
  [{::keys [db] :as env} dquery]
  (let [subquery (entity-subquery env)]
    (mapv (comp #(or % {}) first)
      (raw-datomic-q env (assoc dquery :find [(list 'pull '?e (inject-ident-subqueries env subquery))])
        db))))

(defn query-entity
  "Like query-entities, but returns a single result. This leverage Datomic
  single result :find, meaning it is effectively more efficient than query-entities."
  [{::keys [db] :as env} dquery]
  (let [subquery (entity-subquery env)]
    (post-process-entity env subquery
      (ffirst
        (raw-datomic-q env (assoc dquery :find [(list 'pull '?e (inject-ident-subqueries env subquery))])
          db)))))

(defn ref-attribute? [{::keys [schema]} attr]
  (= :db.type/ref (get-in schema [attr :db/valueType :db/ident])))

(defn schema-provides
  [{::keys [schema-keys] :as config}]
  (reduce
    (fn [provides attr]
      (assoc provides attr (if (ref-attribute? config attr)
                             {:db/id {}}
                             {})))
    {}
    schema-keys))

(defn index-oir
  [{::keys [schema schema-uniques]}]
  (let [resolver  `datomic-resolver
        oir-paths {#{:db/id} #{resolver}}]
    (->> (reduce
           (fn [idx {:db/keys [ident]}]
             (assoc idx ident oir-paths))
           {:db/id (zipmap (map hash-set schema-uniques) (repeat #{resolver}))}
           (vals schema)))))

(defn index-io
  [{::keys [schema schema-uniques]}]
  (-> (zipmap
        (map #(hash-set %) schema-uniques)
        (repeat {:db/id {}}))
      (assoc #{:db/id}
        (into
          {}
          (comp
            (remove (comp #(some->> % (re-find #"^db\.?")) namespace :db/ident))
            (map
              (fn [{:db/keys [ident valueType]}]
                [ident (if (= {:db/ident :db.type/ref} valueType)
                         {:db/id {}}
                         {})])))
          (vals schema)))))

(defn index-idents
  [{::keys [schema-uniques] :as config}]
  (into #{} (cond-> schema-uniques (db-id-allowed? config) (conj :db/id))))

(defn index-schema
  "Creates Pathom index from Datomic schema."
  [config]
  (let [resolver `datomic-resolver]
    {::pc/index-resolvers
     {resolver {::datomic?             true
                ::pc/sym               resolver
                ::pc/cache?            false
                ::pc/dynamic-resolver? true
                ::pc/provides          (schema-provides config)
                ::pc/resolve           (fn [env _] (datomic-resolve config env))}}

     ::pc/index-oir
     (index-oir config)

     ::pc/index-io
     (index-io config)

     ::pc/idents
     (index-idents config)

     ::pc/autocomplete-ignore
     (if (db-id-allowed? config) #{} #{:db/id})}))

(def registry
  [(pc/single-attr-resolver2 ::conn ::db raw-datomic-db)
   (pc/single-attr-resolver2 ::db ::schema db->schema)
   (pc/single-attr-resolver ::schema ::schema-keys #(into #{:db/id} (keys %)))
   (pc/single-attr-resolver ::schema ::schema-uniques schema->uniques)
   (pc/constantly-resolver ::ident-attributes #{})])

(def config-parser (-> registry ps/connect-serial-parser ps/context-parser))

(defn normalize-config
  "Fulfill missing configuration options using inferences."
  [config]
  (config-parser config config
    [::conn ::db ::schema ::schema-uniques ::schema-keys ::ident-attributes
     ::datomic-driver-db ::datomic-driver-q ::whitelist]))

(defn datomic-connect-plugin
  "Plugin to add datomic integration.

  Options:

  ::conn (required) - Datomic connection
  ::ident-attributes - a set containing the attributes to be treated as idents
  ::db - Datomic db, if not provided will be computed from ::conn
  "
  [{::keys [conn] :as config}]
  (let [config'       (normalize-config config)
        datomic-index (index-schema config')]
    {::p/intercept-output
     (fn [env v]
       v)

     ::p/wrap-parser2
     (fn [parser {::p/keys [plugins]}]
       (let [idx-atoms (keep ::pc/indexes plugins)]
         (doseq [idx* idx-atoms]
           (swap! idx* pc/merge-indexes datomic-index))
         (fn [{::keys [db] :as env} tx]
           (let [db       (or db (raw-datomic-db config' conn))
                 ; update datomic db on every parser call
                 config'' (assoc config' ::db db)]
             (parser (merge env config'') tx)))))}))
