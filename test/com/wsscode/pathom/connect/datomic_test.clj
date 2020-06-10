(ns com.wsscode.pathom.connect.datomic-test
  (:require [clojure.test :refer :all]
            [com.wsscode.pathom.connect :as pc]
            [com.wsscode.pathom.connect.datomic :as pcd]
            [com.wsscode.pathom.connect.datomic.on-prem :refer [on-prem-config]]
            [com.wsscode.pathom.connect.planner :as pcp]
            [com.wsscode.pathom.core :as p]
            [datomic.api :as d]
            [edn-query-language.core :as eql]))

(def uri "datomic:free://localhost:4334/mbrainz-1968-1973")
(def conn (d/connect uri))

(def db (d/db conn))

(def db-config
  (assoc on-prem-config
    ::pcd/whitelist ::pcd/DANGER_ALLOW_ALL!))

(def whitelist
  #{:artist/country
    :artist/gid
    :artist/name
    :artist/sortName
    :artist/type
    :country/name
    :medium/format
    :medium/name
    :medium/position
    :medium/trackCount
    :medium/tracks
    :release/artists
    :release/country
    :release/day
    :release/gid
    :release/labels
    :release/language
    :release/media
    :release/month
    :release/name
    :release/packaging
    :release/script
    :release/status
    :release/year
    :track/artists
    :track/duration
    :track/name
    :track/position})

(def db-schema-output
  {:abstractRelease/artistCredit #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The string represenation of the artist(s) to be credited on the abstract release"
                                      :fulltext    true
                                      :id          82
                                      :ident       :abstractRelease/artistCredit
                                      :valueType   #:db{:ident :db.type/string}}
   :abstractRelease/artists      #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :doc         "The set of artists contributing to the abstract release"
                                      :id          81
                                      :ident       :abstractRelease/artists
                                      :valueType   #:db{:ident :db.type/ref}}
   :abstractRelease/gid          #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The globally unique MusicBrainz ID for the abstract release"
                                      :id          78
                                      :ident       :abstractRelease/gid
                                      :unique      #:db{:id 38}
                                      :valueType   #:db{:ident :db.type/uuid}}
   :abstractRelease/name         #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The name of the abstract release"
                                      :id          79
                                      :ident       :abstractRelease/name
                                      :index       true
                                      :valueType   #:db{:ident :db.type/string}}
   :abstractRelease/type         #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Enum, one
  of: :release.type/album, :release.type/single, :release.type/ep, :release.type/audiobook,
  or :release.type/other"
                                      :id          80
                                      :ident       :abstractRelease/type
                                      :valueType   #:db{:ident :db.type/ref}}
   :artist/country               #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The artist's country of origin"
                                      :id          71
                                      :ident       :artist/country
                                      :valueType   #:db{:ident :db.type/ref}}
   :artist/endDay                #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The day the artist stopped actively recording"
                                      :id          77
                                      :ident       :artist/endDay
                                      :valueType   #:db{:ident :db.type/long}}
   :artist/endMonth              #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The month the artist stopped actively recording"
                                      :id          76
                                      :ident       :artist/endMonth
                                      :valueType   #:db{:ident :db.type/long}}
   :artist/endYear               #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The year the artist stopped actively recording"
                                      :id          75
                                      :ident       :artist/endYear
                                      :valueType   #:db{:ident :db.type/long}}
   :artist/gender                #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Enum, one of :artist.gender/male, :artist.gender/female, or :artist.gender/other."
                                      :id          70
                                      :ident       :artist/gender
                                      :valueType   #:db{:ident :db.type/ref}}
   :artist/gid                   #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The globally unique MusicBrainz ID for an artist"
                                      :id          66
                                      :ident       :artist/gid
                                      :unique      #:db{:id 38}
                                      :valueType   #:db{:ident :db.type/uuid}}
   :artist/name                  #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The artist's name"
                                      :fulltext    true
                                      :id          67
                                      :ident       :artist/name
                                      :index       true
                                      :valueType   #:db{:ident :db.type/string}}
   :artist/sortName              #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The artist's name for use in alphabetical sorting, e.g. Beatles, The"
                                      :id          68
                                      :ident       :artist/sortName
                                      :index       true
                                      :valueType   #:db{:ident :db.type/string}}
   :artist/startDay              #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The day the artist started actively recording"
                                      :id          74
                                      :ident       :artist/startDay
                                      :valueType   #:db{:ident :db.type/long}}
   :artist/startMonth            #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The month the artist started actively recording"
                                      :id          73
                                      :ident       :artist/startMonth
                                      :valueType   #:db{:ident :db.type/long}}
   :artist/startYear             #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The year the artist started actively recording"
                                      :id          72
                                      :ident       :artist/startYear
                                      :index       true
                                      :valueType   #:db{:ident :db.type/long}}
   :artist/type                  #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Enum, one of :artist.type/person, :artist.type/other, :artist.type/group."
                                      :id          69
                                      :ident       :artist/type
                                      :valueType   #:db{:ident :db.type/ref}}
   :country/name                 #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The name of the country"
                                      :id          63
                                      :ident       :country/name
                                      :unique      #:db{:id 37}
                                      :valueType   #:db{:ident :db.type/string}}
   :db.alter/attribute           #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :doc         "System attribute with type :db.type/ref. Asserting this attribute on :db.part/db with value v will alter the definition of existing attribute v."
                                      :id          19
                                      :ident       :db.alter/attribute
                                      :valueType   #:db{:ident :db.type/ref}}
   :db.excise/attrs              #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :id          16
                                      :ident       :db.excise/attrs
                                      :valueType   #:db{:ident :db.type/ref}}
   :db.excise/before             #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :id          18
                                      :ident       :db.excise/before
                                      :valueType   #:db{:ident :db.type/instant}}
   :db.excise/beforeT            #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :id          17
                                      :ident       :db.excise/beforeT
                                      :valueType   #:db{:ident :db.type/long}}
   :db.install/attribute         #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :doc         "System attribute with type :db.type/ref. Asserting this attribute on :db.part/db with value v will install v as an attribute."
                                      :id          13
                                      :ident       :db.install/attribute
                                      :valueType   #:db{:ident :db.type/ref}}
   :db.install/function          #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :doc         "System attribute with type :db.type/ref. Asserting this attribute on :db.part/db with value v will install v as a data function."
                                      :id          14
                                      :ident       :db.install/function
                                      :valueType   #:db{:ident :db.type/ref}}
   :db.install/partition         #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :doc         "System attribute with type :db.type/ref. Asserting this attribute on :db.part/db with value v will install v as a partition."
                                      :id          11
                                      :ident       :db.install/partition
                                      :valueType   #:db{:ident :db.type/ref}}
   :db.install/valueType         #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :doc         "System attribute with type :db.type/ref. Asserting this attribute on :db.part/db with value v will install v as a value type."
                                      :id          12
                                      :ident       :db.install/valueType
                                      :valueType   #:db{:ident :db.type/ref}}
   :db.sys/partiallyIndexed      #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "System-assigned attribute set to true for transactions not fully incorporated into the index"
                                      :id          8
                                      :ident       :db.sys/partiallyIndexed
                                      :valueType   #:db{:ident :db.type/boolean}}
   :db.sys/reId                  #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "System-assigned attribute for an id e in the log that has been changed to id v in the index"
                                      :id          9
                                      :ident       :db.sys/reId
                                      :valueType   #:db{:ident :db.type/ref}}
   :db/cardinality               #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Property of an attribute. Two possible values: :db.cardinality/one for single-valued attributes, and :db.cardinality/many for many-valued attributes. Defaults to :db.cardinality/one."
                                      :id          41
                                      :ident       :db/cardinality
                                      :valueType   #:db{:ident :db.type/ref}}
   :db/code                      #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "String-valued attribute of a data function that contains the function's source code."
                                      :fulltext    true
                                      :id          47
                                      :ident       :db/code
                                      :valueType   #:db{:ident :db.type/string}}
   :db/doc                       #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Documentation string for an entity."
                                      :fulltext    true
                                      :id          62
                                      :ident       :db/doc
                                      :valueType   #:db{:ident :db.type/string}}
   :db/excise                    #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :id          15
                                      :ident       :db/excise
                                      :valueType   #:db{:ident :db.type/ref}}
   :db/fn                        #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "A function-valued attribute for direct use by transactions and queries."
                                      :id          52
                                      :ident       :db/fn
                                      :valueType   #:db{:ident :db.type/fn}}
   :db/fulltext                  #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Property of an attribute. If true, create a fulltext search index for the attribute. Defaults to false."
                                      :id          51
                                      :ident       :db/fulltext
                                      :valueType   #:db{:ident :db.type/boolean}}
   :db/ident                     #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Attribute used to uniquely name an entity."
                                      :id          10
                                      :ident       :db/ident
                                      :unique      #:db{:id 38}
                                      :valueType   #:db{:ident :db.type/keyword}}
   :db/index                     #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Property of an attribute. If true, create an AVET index for the attribute. Defaults to false."
                                      :id          44
                                      :ident       :db/index
                                      :valueType   #:db{:ident :db.type/boolean}}
   :db/isComponent               #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Property of attribute whose vtype is :db.type/ref. If true, then the attribute is a component of the entity referencing it. When you query for an entire entity, components are fetched automatically. Defaults to nil."
                                      :id          43
                                      :ident       :db/isComponent
                                      :valueType   #:db{:ident :db.type/boolean}}
   :db/lang                      #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Attribute of a data function. Value is a keyword naming the implementation language of the function. Legal values are :db.lang/java and :db.lang/clojure"
                                      :id          46
                                      :ident       :db/lang
                                      :valueType   #:db{:ident :db.type/ref}}
   :db/noHistory                 #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Property of an attribute. If true, past values of the attribute are not retained after indexing. Defaults to false."
                                      :id          45
                                      :ident       :db/noHistory
                                      :valueType   #:db{:ident :db.type/boolean}}
   :db/txInstant                 #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Attribute whose value is a :db.type/instant. A :db/txInstant is recorded automatically with every transaction."
                                      :id          50
                                      :ident       :db/txInstant
                                      :index       true
                                      :valueType   #:db{:ident :db.type/instant}}
   :db/unique                    #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Property of an attribute. If value is :db.unique/value, then attribute value is unique to each entity. Attempts to insert a duplicate value for a temporary entity id will fail. If value is :db.unique/identity, then attribute value is unique, and upsert is enabled. Attempting to insert a duplicate value for a temporary entity id will cause all attributes associated with that temporary id to be merged with the entity already in the database. Defaults to nil."
                                      :id          42
                                      :ident       :db/unique
                                      :valueType   #:db{:ident :db.type/ref}}
   :db/valueType                 #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Property of an attribute that specifies the attribute's value type. Built-in value types include, :db.type/keyword, :db.type/string, :db.type/ref, :db.type/instant, :db.type/long, :db.type/bigdec, :db.type/boolean, :db.type/float, :db.type/uuid, :db.type/double, :db.type/bigint,  :db.type/uri."
                                      :id          40
                                      :ident       :db/valueType
                                      :valueType   #:db{:ident :db.type/ref}}
   :fressian/tag                 #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Keyword-valued attribute of a value type that specifies the underlying fressian type used for serialization."
                                      :id          39
                                      :ident       :fressian/tag
                                      :index       true
                                      :valueType   #:db{:ident :db.type/keyword}}
   :label/country                #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The country where the record label is located"
                                      :id          87
                                      :ident       :label/country
                                      :valueType   #:db{:ident :db.type/ref}}
   :label/endDay                 #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The day the label stopped business"
                                      :id          93
                                      :ident       :label/endDay
                                      :valueType   #:db{:ident :db.type/long}}
   :label/endMonth               #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The month the label stopped business"
                                      :id          92
                                      :ident       :label/endMonth
                                      :valueType   #:db{:ident :db.type/long}}
   :label/endYear                #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The year the label stopped business"
                                      :id          91
                                      :ident       :label/endYear
                                      :valueType   #:db{:ident :db.type/long}}
   :label/gid                    #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The globally unique MusicBrainz ID for the record label"
                                      :id          83
                                      :ident       :label/gid
                                      :unique      #:db{:id 38}
                                      :valueType   #:db{:ident :db.type/uuid}}
   :label/name                   #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The name of the record label"
                                      :fulltext    true
                                      :id          84
                                      :ident       :label/name
                                      :index       true
                                      :valueType   #:db{:ident :db.type/string}}
   :label/sortName               #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The name of the record label for use in alphabetical sorting"
                                      :id          85
                                      :ident       :label/sortName
                                      :index       true
                                      :valueType   #:db{:ident :db.type/string}}
   :label/startDay               #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The day the label started business"
                                      :id          90
                                      :ident       :label/startDay
                                      :valueType   #:db{:ident :db.type/long}}
   :label/startMonth             #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The month the label started business"
                                      :id          89
                                      :ident       :label/startMonth
                                      :valueType   #:db{:ident :db.type/long}}
   :label/startYear              #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The year the label started business"
                                      :id          88
                                      :ident       :label/startYear
                                      :index       true
                                      :valueType   #:db{:ident :db.type/long}}
   :label/type                   #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Enum, one of :label.type/distributor, :label.type/holding,
  :label.type/production, :label.type/originalProduction,
  :label.type/bootlegProduction, :label.type/reissueProduction, or
  :label.type/publisher."
                                      :id          86
                                      :ident       :label/type
                                      :valueType   #:db{:ident :db.type/ref}}
   :language/name                #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The name of the written and spoken language"
                                      :id          64
                                      :ident       :language/name
                                      :unique      #:db{:id 37}
                                      :valueType   #:db{:ident :db.type/string}}
   :medium/format                #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The format of the medium. An enum with lots of possible values"
                                      :id          111
                                      :ident       :medium/format
                                      :valueType   #:db{:ident :db.type/ref}}
   :medium/name                  #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The name of the medium itself, distinct from the name of the release"
                                      :fulltext    true
                                      :id          113
                                      :ident       :medium/name
                                      :valueType   #:db{:ident :db.type/string}}
   :medium/position              #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The position of this medium in the release relative to the other media, i.e. disc 1"
                                      :id          112
                                      :ident       :medium/position
                                      :valueType   #:db{:ident :db.type/long}}
   :medium/trackCount            #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The total number of tracks on the medium"
                                      :id          114
                                      :ident       :medium/trackCount
                                      :valueType   #:db{:ident :db.type/long}}
   :medium/tracks                #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :doc         "The set of tracks found on this medium"
                                      :id          110
                                      :ident       :medium/tracks
                                      :isComponent true
                                      :valueType   #:db{:ident :db.type/ref}}
   :release/abstractRelease      #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "This release is the physical manifestation of the
  associated abstract release, e.g. the the 1984 US vinyl release of
  \"The Wall\" by Columbia, as opposed to the 2000 US CD release of
  \"The Wall\" by Capitol Records."
                                      :id          108
                                      :ident       :release/abstractRelease
                                      :valueType   #:db{:ident :db.type/ref}}
   :release/artistCredit         #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The string represenation of the artist(s) to be credited on the release"
                                      :fulltext    true
                                      :id          106
                                      :ident       :release/artistCredit
                                      :valueType   #:db{:ident :db.type/string}}
   :release/artists              #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :doc         "The set of artists contributing to the release"
                                      :id          107
                                      :ident       :release/artists
                                      :valueType   #:db{:ident :db.type/ref}}
   :release/barcode              #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The barcode on the release packaging"
                                      :id          99
                                      :ident       :release/barcode
                                      :valueType   #:db{:ident :db.type/string}}
   :release/country              #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The country where the recording was released"
                                      :id          95
                                      :ident       :release/country
                                      :valueType   #:db{:ident :db.type/ref}}
   :release/day                  #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The day of the release"
                                      :id          105
                                      :ident       :release/day
                                      :valueType   #:db{:ident :db.type/long}}
   :release/gid                  #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The globally unique MusicBrainz ID for the release"
                                      :id          94
                                      :ident       :release/gid
                                      :unique      #:db{:id 38}
                                      :valueType   #:db{:ident :db.type/uuid}}
   :release/labels               #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :doc         "The label on which the recording was released"
                                      :id          96
                                      :ident       :release/labels
                                      :valueType   #:db{:ident :db.type/ref}}
   :release/language             #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The language used in the release"
                                      :id          98
                                      :ident       :release/language
                                      :valueType   #:db{:ident :db.type/ref}}
   :release/media                #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :doc         "The various media (CDs, vinyl records, cassette tapes, etc.) included in the release."
                                      :id          101
                                      :ident       :release/media
                                      :isComponent true
                                      :valueType   #:db{:ident :db.type/ref}}
   :release/month                #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The month of the release"
                                      :id          104
                                      :ident       :release/month
                                      :valueType   #:db{:ident :db.type/long}}
   :release/name                 #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The name of the release"
                                      :fulltext    true
                                      :id          100
                                      :ident       :release/name
                                      :index       true
                                      :valueType   #:db{:ident :db.type/string}}
   :release/packaging            #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The type of packaging used in the release, an enum, one
  of: :release.packaging/jewelCase, :release.packaging/slimJewelCase, :release.packaging/digipak, :release.packaging/other
  , :release.packaging/keepCase, :release.packaging/none,
  or :release.packaging/cardboardPaperSleeve"
                                      :id          102
                                      :ident       :release/packaging
                                      :valueType   #:db{:ident :db.type/ref}}
   :release/script               #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The script used in the release"
                                      :id          97
                                      :ident       :release/script
                                      :valueType   #:db{:ident :db.type/ref}}
   :release/status               #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The status of the release"
                                      :id          109
                                      :ident       :release/status
                                      :index       true
                                      :valueType   #:db{:ident :db.type/string}}
   :release/year                 #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The year of the release"
                                      :id          103
                                      :ident       :release/year
                                      :index       true
                                      :valueType   #:db{:ident :db.type/long}}
   :script/name                  #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Name of written character set, e.g. Hebrew, Latin, Cyrillic"
                                      :id          65
                                      :ident       :script/name
                                      :unique      #:db{:id 37}
                                      :valueType   #:db{:ident :db.type/string}}
   :track/artistCredit           #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The artists who contributed to the track"
                                      :fulltext    true
                                      :id          116
                                      :ident       :track/artistCredit
                                      :valueType   #:db{:ident :db.type/string}}
   :track/artists                #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :doc         "The artists who contributed to the track"
                                      :id          115
                                      :ident       :track/artists
                                      :valueType   #:db{:ident :db.type/ref}}
   :track/duration               #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The duration of the track in msecs"
                                      :id          119
                                      :ident       :track/duration
                                      :index       true
                                      :valueType   #:db{:ident :db.type/long}}
   :track/name                   #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The track name"
                                      :fulltext    true
                                      :id          118
                                      :ident       :track/name
                                      :index       true
                                      :valueType   #:db{:ident :db.type/string}}
   :track/position               #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The position of the track relative to the other tracks on the medium"
                                      :id          117
                                      :ident       :track/position
                                      :valueType   #:db{:ident :db.type/long}}})

(deftest test-db->schema
  (is (= (pcd/db->schema db-config db)
         db-schema-output)))

(deftest test-schema->uniques
  (is (= (pcd/schema->uniques db-schema-output)
         #{:abstractRelease/gid
           :artist/gid
           :country/name
           :db/ident
           :label/gid
           :language/name
           :release/gid
           :script/name})))

(deftest test-inject-ident-subqueries
  (testing "add ident sub query part on ident fields"
    (is (= (pcd/inject-ident-subqueries
             {::pcd/ident-attributes #{:foo}}
             [:foo])
           [{:foo [:db/ident]}]))))

(deftest test-pick-ident-key
  (let [config (pcd/normalize-config (merge db-config {::pcd/conn conn}))]
    (testing "nothing available"
      (is (= (pcd/pick-ident-key config
               {})
             nil))
      (is (= (pcd/pick-ident-key config
               {:id  123
                :foo "bar"})
             nil)))
    (testing "pick from :db/id"
      (is (= (pcd/pick-ident-key config
               {:db/id 123})
             123)))
    (testing "picking from schema unique"
      (is (= (pcd/pick-ident-key config
               {:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"})
             [:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"])))
    (testing "prefer :db/id"
      (is (= (pcd/pick-ident-key config
               {:db/id      123
                :artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"})
             123)))))

(def index-oir-output
  `{:abstractRelease/artistCredit {#{:db/id} #{pcd/datomic-resolver}}
    :abstractRelease/artists      {#{:db/id} #{pcd/datomic-resolver}}
    :abstractRelease/gid          {#{:db/id} #{pcd/datomic-resolver}}
    :abstractRelease/name         {#{:db/id} #{pcd/datomic-resolver}}
    :abstractRelease/type         {#{:db/id} #{pcd/datomic-resolver}}
    :artist/country               {#{:db/id} #{pcd/datomic-resolver}}
    :artist/endDay                {#{:db/id} #{pcd/datomic-resolver}}
    :artist/endMonth              {#{:db/id} #{pcd/datomic-resolver}}
    :artist/endYear               {#{:db/id} #{pcd/datomic-resolver}}
    :artist/gender                {#{:db/id} #{pcd/datomic-resolver}}
    :artist/gid                   {#{:db/id} #{pcd/datomic-resolver}}
    :artist/name                  {#{:db/id} #{pcd/datomic-resolver}}
    :artist/sortName              {#{:db/id} #{pcd/datomic-resolver}}
    :artist/startDay              {#{:db/id} #{pcd/datomic-resolver}}
    :artist/startMonth            {#{:db/id} #{pcd/datomic-resolver}}
    :artist/startYear             {#{:db/id} #{pcd/datomic-resolver}}
    :artist/type                  {#{:db/id} #{pcd/datomic-resolver}}
    :db/id                        {#{:abstractRelease/gid} #{pcd/datomic-resolver}
                                   #{:artist/gid}          #{pcd/datomic-resolver}
                                   #{:country/name}        #{pcd/datomic-resolver}
                                   #{:db/ident}            #{pcd/datomic-resolver}
                                   #{:label/gid}           #{pcd/datomic-resolver}
                                   #{:language/name}       #{pcd/datomic-resolver}
                                   #{:release/gid}         #{pcd/datomic-resolver}
                                   #{:script/name}         #{pcd/datomic-resolver}}
    :country/name                 {#{:db/id} #{pcd/datomic-resolver}}
    :db.alter/attribute           {#{:db/id} #{pcd/datomic-resolver}}
    :db.excise/attrs              {#{:db/id} #{pcd/datomic-resolver}}
    :db.excise/before             {#{:db/id} #{pcd/datomic-resolver}}
    :db.excise/beforeT            {#{:db/id} #{pcd/datomic-resolver}}
    :db.install/attribute         {#{:db/id} #{pcd/datomic-resolver}}
    :db.install/function          {#{:db/id} #{pcd/datomic-resolver}}
    :db.install/partition         {#{:db/id} #{pcd/datomic-resolver}}
    :db.install/valueType         {#{:db/id} #{pcd/datomic-resolver}}
    :db.sys/partiallyIndexed      {#{:db/id} #{pcd/datomic-resolver}}
    :db.sys/reId                  {#{:db/id} #{pcd/datomic-resolver}}
    :db/cardinality               {#{:db/id} #{pcd/datomic-resolver}}
    :db/code                      {#{:db/id} #{pcd/datomic-resolver}}
    :db/doc                       {#{:db/id} #{pcd/datomic-resolver}}
    :db/excise                    {#{:db/id} #{pcd/datomic-resolver}}
    :db/fn                        {#{:db/id} #{pcd/datomic-resolver}}
    :db/fulltext                  {#{:db/id} #{pcd/datomic-resolver}}
    :db/ident                     {#{:db/id} #{pcd/datomic-resolver}}
    :db/index                     {#{:db/id} #{pcd/datomic-resolver}}
    :db/isComponent               {#{:db/id} #{pcd/datomic-resolver}}
    :db/lang                      {#{:db/id} #{pcd/datomic-resolver}}
    :db/noHistory                 {#{:db/id} #{pcd/datomic-resolver}}
    :db/txInstant                 {#{:db/id} #{pcd/datomic-resolver}}
    :db/unique                    {#{:db/id} #{pcd/datomic-resolver}}
    :db/valueType                 {#{:db/id} #{pcd/datomic-resolver}}
    :fressian/tag                 {#{:db/id} #{pcd/datomic-resolver}}
    :label/country                {#{:db/id} #{pcd/datomic-resolver}}
    :label/endDay                 {#{:db/id} #{pcd/datomic-resolver}}
    :label/endMonth               {#{:db/id} #{pcd/datomic-resolver}}
    :label/endYear                {#{:db/id} #{pcd/datomic-resolver}}
    :label/gid                    {#{:db/id} #{pcd/datomic-resolver}}
    :label/name                   {#{:db/id} #{pcd/datomic-resolver}}
    :label/sortName               {#{:db/id} #{pcd/datomic-resolver}}
    :label/startDay               {#{:db/id} #{pcd/datomic-resolver}}
    :label/startMonth             {#{:db/id} #{pcd/datomic-resolver}}
    :label/startYear              {#{:db/id} #{pcd/datomic-resolver}}
    :label/type                   {#{:db/id} #{pcd/datomic-resolver}}
    :language/name                {#{:db/id} #{pcd/datomic-resolver}}
    :medium/format                {#{:db/id} #{pcd/datomic-resolver}}
    :medium/name                  {#{:db/id} #{pcd/datomic-resolver}}
    :medium/position              {#{:db/id} #{pcd/datomic-resolver}}
    :medium/trackCount            {#{:db/id} #{pcd/datomic-resolver}}
    :medium/tracks                {#{:db/id} #{pcd/datomic-resolver}}
    :release/abstractRelease      {#{:db/id} #{pcd/datomic-resolver}}
    :release/artistCredit         {#{:db/id} #{pcd/datomic-resolver}}
    :release/artists              {#{:db/id} #{pcd/datomic-resolver}}
    :release/barcode              {#{:db/id} #{pcd/datomic-resolver}}
    :release/country              {#{:db/id} #{pcd/datomic-resolver}}
    :release/day                  {#{:db/id} #{pcd/datomic-resolver}}
    :release/gid                  {#{:db/id} #{pcd/datomic-resolver}}
    :release/labels               {#{:db/id} #{pcd/datomic-resolver}}
    :release/language             {#{:db/id} #{pcd/datomic-resolver}}
    :release/media                {#{:db/id} #{pcd/datomic-resolver}}
    :release/month                {#{:db/id} #{pcd/datomic-resolver}}
    :release/name                 {#{:db/id} #{pcd/datomic-resolver}}
    :release/packaging            {#{:db/id} #{pcd/datomic-resolver}}
    :release/script               {#{:db/id} #{pcd/datomic-resolver}}
    :release/status               {#{:db/id} #{pcd/datomic-resolver}}
    :release/year                 {#{:db/id} #{pcd/datomic-resolver}}
    :script/name                  {#{:db/id} #{pcd/datomic-resolver}}
    :track/artistCredit           {#{:db/id} #{pcd/datomic-resolver}}
    :track/artists                {#{:db/id} #{pcd/datomic-resolver}}
    :track/duration               {#{:db/id} #{pcd/datomic-resolver}}
    :track/name                   {#{:db/id} #{pcd/datomic-resolver}}
    :track/position               {#{:db/id} #{pcd/datomic-resolver}}})

(def index-io-output
  {#{:abstractRelease/gid} {:db/id {}}
   #{:artist/gid}          {:db/id {}}
   #{:db/id}               {:abstractRelease/artistCredit {}
                            :abstractRelease/artists      {:db/id {}}
                            :abstractRelease/gid          {}
                            :abstractRelease/name         {}
                            :abstractRelease/type         {:db/id {}}
                            :artist/country               {:db/id {}}
                            :artist/endDay                {}
                            :artist/endMonth              {}
                            :artist/endYear               {}
                            :artist/gender                {:db/id {}}
                            :artist/gid                   {}
                            :artist/name                  {}
                            :artist/sortName              {}
                            :artist/startDay              {}
                            :artist/startMonth            {}
                            :artist/startYear             {}
                            :artist/type                  {:db/id {}}
                            :country/name                 {}
                            :fressian/tag                 {}
                            :label/country                {:db/id {}}
                            :label/endDay                 {}
                            :label/endMonth               {}
                            :label/endYear                {}
                            :label/gid                    {}
                            :label/name                   {}
                            :label/sortName               {}
                            :label/startDay               {}
                            :label/startMonth             {}
                            :label/startYear              {}
                            :label/type                   {:db/id {}}
                            :language/name                {}
                            :medium/format                {:db/id {}}
                            :medium/name                  {}
                            :medium/position              {}
                            :medium/trackCount            {}
                            :medium/tracks                {:db/id {}}
                            :release/abstractRelease      {:db/id {}}
                            :release/artistCredit         {}
                            :release/artists              {:db/id {}}
                            :release/barcode              {}
                            :release/country              {:db/id {}}
                            :release/day                  {}
                            :release/gid                  {}
                            :release/labels               {:db/id {}}
                            :release/language             {:db/id {}}
                            :release/media                {:db/id {}}
                            :release/month                {}
                            :release/name                 {}
                            :release/packaging            {:db/id {}}
                            :release/script               {:db/id {}}
                            :release/status               {}
                            :release/year                 {}
                            :script/name                  {}
                            :track/artistCredit           {}
                            :track/artists                {:db/id {}}
                            :track/duration               {}
                            :track/name                   {}
                            :track/position               {}}
   #{:country/name}        {:db/id {}}
   #{:db/ident}            {:db/id {}}
   #{:label/gid}           {:db/id {}}
   #{:language/name}       {:db/id {}}
   #{:release/gid}         {:db/id {}}
   #{:script/name}         {:db/id {}}})

(def index-idents-output
  #{:abstractRelease/gid
    :artist/gid
    :country/name
    :db/id
    :db/ident
    :label/gid
    :language/name
    :release/gid
    :script/name})

(deftest test-index-schema
  (let [index (pcd/index-schema
                (pcd/normalize-config {::pcd/schema db-schema-output ::pcd/whitelist ::pcd/DANGER_ALLOW_ALL!}))]
    (is (= (::pc/index-oir index)
           index-oir-output))

    (is (= (::pc/index-io index)
           index-io-output))

    (is (= (::pc/idents index)
           index-idents-output))))

(def index-io-secure-output
  {#{:artist/gid}   {:db/id {}}
   #{:country/name} {:db/id {}}
   #{:db/id}        {:artist/country    {:db/id {}}
                     :artist/gid        {}
                     :artist/name       {}
                     :artist/sortName   {}
                     :artist/type       {:db/id {}}
                     :country/name      {}
                     :medium/format     {:db/id {}}
                     :medium/name       {}
                     :medium/position   {}
                     :medium/trackCount {}
                     :medium/tracks     {:db/id {}}
                     :release/artists   {:db/id {}}
                     :release/country   {:db/id {}}
                     :release/day       {}
                     :release/gid       {}
                     :release/labels    {:db/id {}}
                     :release/language  {:db/id {}}
                     :release/media     {:db/id {}}
                     :release/month     {}
                     :release/name      {}
                     :release/packaging {:db/id {}}
                     :release/script    {:db/id {}}
                     :release/status    {}
                     :release/year      {}
                     :track/artists     {:db/id {}}
                     :track/duration    {}
                     :track/name        {}
                     :track/position    {}}
   #{:release/gid}  {:db/id {}}})

(def index-idents-secure-output
  #{:artist/gid
    :country/name
    :release/gid})

(deftest test-index-schema-secure
  (let [index (pcd/index-schema
                (pcd/normalize-config
                  (assoc db-config
                    ::pcd/conn conn
                    ::pcd/whitelist whitelist)))]
    (is (= (::pc/index-oir index)
           '{:artist/country    {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :artist/gid        {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :artist/name       {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :artist/sortName   {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :artist/type       {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :country/name      {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :db/id             {#{:artist/gid}   #{com.wsscode.pathom.connect.datomic/datomic-resolver}
                                 #{:country/name} #{com.wsscode.pathom.connect.datomic/datomic-resolver}
                                 #{:release/gid}  #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :medium/format     {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :medium/name       {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :medium/position   {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :medium/trackCount {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :medium/tracks     {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :release/artists   {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :release/country   {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :release/day       {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :release/gid       {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :release/labels    {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :release/language  {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :release/media     {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :release/month     {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :release/name      {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :release/packaging {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :release/script    {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :release/status    {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :release/year      {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :track/artists     {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :track/duration    {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :track/name        {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}
             :track/position    {#{:db/id} #{com.wsscode.pathom.connect.datomic/datomic-resolver}}}))

    (is (= (::pc/index-io index)
           index-io-secure-output))

    (is (= (::pc/idents index)
           index-idents-secure-output))

    (is (= (::pc/autocomplete-ignore index)
           #{:db/id}))))

(deftest test-post-process-entity
  (is (= (pcd/post-process-entity
           {::pcd/ident-attributes #{:artist/type}}
           [:artist/type]
           {:artist/type {:db/ident :artist.type/person}})
         {:artist/type :artist.type/person})))

(def super-name
  (pc/single-attr-resolver :artist/name :artist/super-name #(str "SUPER - " %)))

(pc/defresolver years-active [env {:artist/keys [startYear endYear]}]
  {::pc/input  #{:artist/startYear :artist/endYear}
   ::pc/output [:artist/active-years-count]}
  {:artist/active-years-count (- endYear startYear)})

(pc/defresolver artists-before-1600 [env _]
  {::pc/output [{:artist/artists-before-1600 [:db/id]}]}
  {:artist/artists-before-1600
   (pcd/query-entities env
     '{:where [[?e :artist/name ?name]
               [?e :artist/startYear ?year]
               [(< ?year 1600)]]})})

(pc/defresolver artist-before-1600 [env _]
  {::pc/output [{:artist/artist-before-1600 [:db/id]}]}
  {:artist/artist-before-1600
   (pcd/query-entity env
     '{:where [[?e :artist/name ?name]
               [?e :artist/startYear ?year]
               [(< ?year 1600)]]})})

(pc/defresolver all-mediums [env _]
  {::pc/output [{:all-mediums [:db/id]}]}
  {:all-mediums
   (pcd/query-entities env
     '{:where [[?e :medium/name _]]})})

(def registry
  [super-name
   years-active
   artists-before-1600
   artist-before-1600
   all-mediums])

(def parser
  (p/parser
    {::p/env     {::p/reader               [p/map-reader
                                            pc/reader3
                                            pc/open-ident-reader
                                            p/env-placeholder-reader]
                  ::p/placeholder-prefixes #{">"}}
     ::p/mutate  pc/mutate
     ::p/plugins [(pc/connect-plugin {::pc/register registry})
                  (pcd/datomic-connect-plugin (assoc db-config
                                                ::pcd/conn conn
                                                ::pcd/ident-attributes #{:artist/type}))
                  p/error-handler-plugin
                  p/trace-plugin]}))

(deftest test-datomic-parser
  (testing "reading from :db/id"
    (is (= (parser {}
             [{[:db/id 637716744120508]
               [:artist/name]}])
           {[:db/id 637716744120508] {:artist/name "Janis Joplin"}})))

  (testing "reading from unique attribute"
    (is (= (parser {}
             [{[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
               [:artist/name]}])
           {[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
            {:artist/name "Janis Joplin"}})))

  (testing "explicit db"
    (is (= (parser {::pcd/db (:db-after (d/with (d/db conn)
                                                [{:artist/gid  #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"
                                                  :artist/name "not Janis Joplin"}]))}
                   [{[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
                     [:artist/name]}])
           {[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
            {:artist/name "not Janis Joplin"}})))

  (comment
    "after transact data (I will not transact in your mbrainz), parser should take a new db"
    (d/transact conn
                [{:artist/gid  #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"
                  :artist/name "not Janis Joplin"}])
    (is (= (parser {}
                   [{[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
                     [:artist/name]}])
           {[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
            {:artist/name "not Janis Joplin"}})))

  (testing "implicit dependency"
    (is (= (parser {}
             [{[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
               [:artist/super-name]}])
           {[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
            {:artist/super-name "SUPER - Janis Joplin"}})))

  (testing "process-query"
    (is (= (parser {}
             [{:artist/artists-before-1600
               [:artist/super-name
                {:artist/country
                 [:country/name]}]}])
           {:artist/artists-before-1600
            [{:artist/super-name "SUPER - Heinrich Schütz",
              :artist/country    {:country/name "Germany"}}
             {:artist/super-name "SUPER - Choir of King's College, Cambridge",
              :artist/country    {:country/name "United Kingdom"}}]}))

    (is (= (parser {}
             [{:artist/artist-before-1600
               [:artist/super-name
                {:artist/country
                 [:country/name
                  :db/id]}]}])
           {:artist/artist-before-1600
            {:artist/super-name "SUPER - Heinrich Schütz",
             :artist/country    {:country/name "Germany"
                                 :db/id        17592186045657}}}))

    (testing "partial missing information on entities"
      (is (= (parser {::pcd/db (-> (d/with db
                                     [{:medium/name "val"}
                                      {:medium/name "6val"
                                       :artist/name "bla"}
                                      {:medium/name "3"
                                       :artist/name "bar"}])
                                   :db-after)}
               [{:all-mediums
                 [:artist/name :medium/name]}])
             {:all-mediums [{:artist/name "bar", :medium/name "3"}
                            {:artist/name :com.wsscode.pathom.core/not-found, :medium/name "val"}
                            {:artist/name "bla", :medium/name "6val"}]})))

    (testing "nested complex dependency"
      (is (= (parser {}
               [{[:release/gid #uuid"b89a6f8b-5784-41d2-973d-dcd4d99b05c2"]
                 [{:release/artists
                   [:artist/super-name]}]}])
             {[:release/gid #uuid"b89a6f8b-5784-41d2-973d-dcd4d99b05c2"]
              {:release/artists [{:artist/super-name "SUPER - Horst Jankowski"}]}})))

    (testing "without subquery"
      (is (= (parser {}
               [:artist/artists-before-1600])
             {:artist/artists-before-1600
              [{:db/id 690493302253222} {:db/id 716881581319519}]}))

      (is (= (parser {}
               [:artist/artist-before-1600])
             {:artist/artist-before-1600
              {:db/id 690493302253222}})))

    (testing "ident attributes"
      (is (= (parser {}
               [{[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
                 [:artist/type]}])
             {[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
              {:artist/type :artist.type/person}}))
      (is (= (parser {}
               [{[:db/id 637716744120508]
                 [{:artist/type [:db/id]}]}])
             {[:db/id 637716744120508]
              {:artist/type {:db/id 17592186045423}}})))))

(def secure-parser
  (p/parser
    {::p/env     {::p/reader               [p/map-reader
                                            pc/reader3
                                            pc/open-ident-reader
                                            p/env-placeholder-reader]
                  ::p/placeholder-prefixes #{">"}}
     ::p/mutate  pc/mutate
     ::p/plugins [(pc/connect-plugin {::pc/register registry})
                  (pcd/datomic-connect-plugin
                    (assoc db-config
                      ::pcd/conn conn
                      ::pcd/whitelist whitelist
                      ::pcd/ident-attributes #{:artist/type}))
                  p/error-handler-plugin
                  p/trace-plugin]}))

(deftest test-datomic-secure-parser
  (testing "don't allow access with :db/id"
    (is (= (secure-parser {}
             [{[:db/id 637716744120508]
               [:artist/name]}])
           {[:db/id 637716744120508] {:artist/name ::p/not-found}})))

  (testing "not found for fields not listed"
    (is (= (secure-parser {}
             [{[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
               [:artist/name :artist/gender]}])
           {[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
            {:artist/name   "Janis Joplin"
             :artist/gender ::p/not-found}})))

  (testing "not found for :db/id when its not allowed"
    (is (= (secure-parser {}
             [{[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
               [:artist/name :db/id]}])
           {[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
            {:artist/name "Janis Joplin"
             :db/id       ::p/not-found}})))

  (testing "implicit dependency"
    (is (= (secure-parser {}
             [{[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
               [:artist/super-name]}])
           {[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
            {:artist/super-name "SUPER - Janis Joplin"}})))

  (testing "process-query"
    (is (= (secure-parser {}
             [{:artist/artists-before-1600
               [:artist/super-name
                {:artist/country
                 [:country/name]}]}])
           {:artist/artists-before-1600
            [{:artist/super-name "SUPER - Heinrich Schütz",
              :artist/country    {:country/name "Germany"}}
             {:artist/super-name "SUPER - Choir of King's College, Cambridge",
              :artist/country    {:country/name "United Kingdom"}}]}))

    (is (= (secure-parser {}
             [{:artist/artist-before-1600
               [:artist/super-name
                {:artist/country
                 [:country/name
                  :db/id]}]}])
           {:artist/artist-before-1600
            {:artist/super-name "SUPER - Heinrich Schütz",
             :artist/country    {:country/name "Germany"
                                 :db/id        17592186045657}}}))

    (testing "nested complex dependency"
      (is (= (secure-parser {}
               [{[:release/gid #uuid"b89a6f8b-5784-41d2-973d-dcd4d99b05c2"]
                 [{:release/artists
                   [:artist/super-name]}]}])
             {[:release/gid #uuid"b89a6f8b-5784-41d2-973d-dcd4d99b05c2"]
              {:release/artists [{:artist/super-name "SUPER - Horst Jankowski"}]}})))))

(comment
  (pcd/config-parser db-config {::pcd/conn conn}
    [::pcd/schema])

  (pcd/config-parser db-config {::pcd/conn conn} [::pcd/schema-keys])

  (pcp/compute-run-graph
    (merge
      (-> (pcd/index-schema
            (pcd/normalize-config (merge db-config {::pcd/conn conn})))
          (pc/register registry))
      {:edn-query-language.ast/node (eql/query->ast
                                      [{:release/artists
                                        [:artist/super-name]}])
       ::pcp/available-data         {:db/id {}}}))

  (parser {}
    [{::pc/indexes
      [::pc/index-oir]}])

  :q [:find ?a :where [?e ?a _] [(< ?e 100)]]
  (parser {}
    [{[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
      [:artist/active-years-count]}])

  (parser {}
    [{[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
      [:artist/type]}])

  (pcd/index-io {::pcd/schema         db-schema-output
                 ::pcd/schema-uniques (pcd/schema->uniques db-schema-output)})

  (pcd/index-idents {::pcd/schema         db-schema-output
                     ::pcd/schema-uniques (pcd/schema->uniques db-schema-output)})

  (parser {}
    [{:artist/artist-before-1600
      [:artist/name
       :artist/active-years-count
       {:artist/country
        [:country/name]}]}])

  (is (= (parser {}
           [{[:db/id 637716744120508]
             [:artist/type]}])
         {[:db/id 637716744120508]
          {:artist/type :artist.type/person}}))

  (d/q '[:find (pull ?e [:artist/name]) :where [?e :medium/name _]]
    (-> (d/with db
          [{:medium/name "val"}
           {:medium/name "6val"
            :artist/name "bla"}
           {:medium/name "3"
            :artist/name "bar"}])
        :db-after))

  (->> (d/q '[:find ?attr ?type ?card
              :where
              [_ :db.install/attribute ?a]
              [?a :db/valueType ?t]
              [?a :db/cardinality ?c]
              [?a :db/ident ?attr]
              [?t :db/ident ?type]
              [?c :db/ident ?card]]
         db)
       (mapv #(zipmap [:db/ident
                       :db/valueType
                       :db/cardinality] %)))

  (pcd/db->schema db)
  (d/q '[:find ?id ?type ?gender ?e
         :in $ ?name
         :where
         [?e :artist/name ?name]
         [?e :artist/gid ?id]
         [?e :artist/type ?teid]
         [?teid :db/ident ?type]
         [?e :artist/gender ?geid]
         [?geid :db/ident ?gender]]
    db
    "Janis Joplin")

  (d/q '{:find  [[(pull ?e [*]) ...]]
         :in [$ ?gid]
         :where [[?e :artist/gid ?gid]]}
    db
    #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f")

  (d/q '[:find (pull ?e [* :foo/bar]) .
         :in $ ?e]
    db
    637716744120508))
