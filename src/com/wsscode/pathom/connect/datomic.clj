(ns com.wsscode.pathom.connect.datomic
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [com.wsscode.pathom.connect :as pc]
            [com.wsscode.pathom.core :as p]
            [com.wsscode.pathom.sugar :as ps]
            [edn-query-language.core :as eql]))

(s/def ::db any?)
(s/def ::schema (s/map-of ::p/attribute map?))
(s/def ::schema-keys (s/coll-of ::p/attribute :kind set?))
(s/def ::schema-uniques ::schema-keys)
(s/def ::ident-attributes ::schema-keys)

(s/def ::schema-entry
  (s/keys
    :req [:db/ident :db/id :db/valueType :db/cardinality]
    :opt [:db/doc :db/unique]))

(s/def ::schema (s/map-of :db/ident ::schema-entry))

(defn raw-datomic-q [{::keys [datomic-driver-q]} & args]
  (apply datomic-driver-q args))

(defn raw-datomic-db [{::keys [datomic-driver-db]} conn]
  (datomic-driver-db conn))

(defn db->schema
  "Extracts the schema from a Datomic db."
  [env db]
  (->> (raw-datomic-q env '[:find (pull ?e [*])
              :where
              [_ :db.install/attribute ?e]
              [?e :db/ident ?ident]]
         db)
       (map first)
       (reduce
         (fn [schema entry]
           (assoc schema (:db/ident entry) entry))
         {})))

(defn schema->uniques
  "Return a set with the ident of the unique attributes in the schema."
  [schema]
  (->> schema
       vals
       (filter :db/unique)
       (into #{} (map :db/ident))))

(defn project-dependencies
  "Given a ::p/parent-query and ::schema-keys, compute all dependencies required to fulfil
  the ::p/parent-query."
  [{::keys    [schema-keys]
    ::p/keys  [parent-query]
    ::pc/keys [indexes sort-plan]
    :as       env}]
  (let [sort-plan   (or sort-plan pc/default-sort-plan)
        [good bad] (pc/split-good-bad-keys (p/entity env))
        non-datomic (keep
                      (fn [{:keys [key]}]
                        (if (contains? schema-keys key)
                          nil
                          key))
                      (:children (eql/query->shallow-ast parent-query)))]
    (into #{}
          (mapcat (fn [key]
                    (->> (pc/compute-paths*
                           (::pc/index-oir indexes)
                           good bad
                           key
                           #{key})
                         (sort-plan env)
                         first
                         (map first))))
          non-datomic)))

(defn ensure-minimum-subquery
  "Ensure the subquery has at least one element, this prevent empty sub-queries to be
  sent to datomic."
  [ast]
  (update ast :children
    (fn [items]
      (if (seq items)
        (mapv ensure-minimum-subquery items)
        [{:type :property :key :db/id :dispatch-key :db/id}]))))

(defn datomic-subquery
  "Given a ::p/parent-query, projects dependencies and compute the part of the query
  that Pathom delegates to Datomic to fulfil."
  [{::p/keys [parent-query]
    ::keys   [schema-keys]
    :as      env}]
  (let [ent         (p/entity env)
        parent-keys (into #{} (map :key) (:children (eql/query->shallow-ast parent-query)))
        deps        (project-dependencies env)
        new-deps    (into []
                          (comp (filter schema-keys)
                                (map #(hash-map :key %)))
                          (set/difference deps parent-keys))]
    (->> parent-query
         (p/lift-placeholders env)
         p/query->ast
         (p/transduce-children
           (comp (filter (comp schema-keys :key))
                 (map #(dissoc % :params))))
         ensure-minimum-subquery
         :children
         (into new-deps (comp (remove #(contains? ent (:key %))) ; remove already known keys
                              ; remove ident attributes
                              (remove (comp vector? :key))))
         (hash-map :type :root :children)
         p/ast->query)))

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
  [{::keys [schema-uniques]} m]
  (if (contains? m :db/id)
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

(defn datomic-resolve
  "Runs the resolver to fetch Datomic data from identities."
  [{::keys [db]
    :as    config}
   env]
  (let [id       (pick-ident-key config (p/entity env))
        subquery (datomic-subquery (merge env config))]
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
  [{:keys [query] :as env}]
  (let [subquery (datomic-subquery (assoc env ::p/parent-query query ::p/entity {:db/id nil}))]
    (cond-> subquery (not (seq subquery)) (conj :db/id))))

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
    (map first
     (raw-datomic-q env (assoc dquery :find [[(list 'pull '?e (inject-ident-subqueries env subquery))]])
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

(defn index-schema
  "Creates Pathom index from Datomic schema."
  [{::keys [schema] :as config}]
  (let [resolver  `datomic-resolver
        oir-paths {#{:db/id} #{resolver}}]
    {::pc/index-resolvers
     {resolver {::pc/sym            resolver
                ::pc/cache?         false
                ::pc/compute-output (fn [env]
                                      (datomic-subquery (merge env config)))
                ::datomic?          true
                ::pc/resolve        (fn [env _] (datomic-resolve config env))}}

     ::pc/index-oir
     (->> (reduce
            (fn [idx {:db/keys [ident]}]
              (assoc idx ident oir-paths))
            {:db/id (zipmap (map hash-set (schema->uniques schema)) (repeat #{resolver}))}
            (vals schema)))}))

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
     ::datomic-driver-db ::datomic-driver-q]))

(defn datomic-connect-plugin
  "Plugin to add datomic integration.

  Options:

  ::conn (required) - Datomic connection
  ::ident-attributes - a set containing the attributes to be treated as idents
  ::db - Datomic db, if not provided will be computed from ::conn
  "
  [config]
  (let [config'       (normalize-config config)
        datomic-index (index-schema config')]
    {::p/wrap-parser2
     (fn [parser {::p/keys [plugins]}]
       (let [idx-atoms (keep ::pc/indexes plugins)]
         (doseq [idx* idx-atoms]
           (swap! idx* pc/merge-indexes datomic-index))
         (fn [env tx]
           (parser (merge env config') tx))))}))
