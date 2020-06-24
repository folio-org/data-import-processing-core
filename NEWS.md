## 2020-06-05 v2.1.1-SNAPSHOT
* [MODINV-319](https://issues.folio.org/browse/MODINV-319) Mapping exception in mod-inventory with rules for notes - BUGFIX.
* [MODDICORE-62](https://issues.folio.org/browse/MODDICORE-62) Adjusted handling of repeatable fields

## 2020-06-10 v2.1.1
* Updated pubsub client dependency to v1.2.0
* [MODDICORE-52](https://issues.folio.org/browse/MODDICORE-52) Add support for MappingEngine work with a leader

## 2020-06-01 v2.1.0
* [MODDATAIMP-300](https://issues.folio.org/browse/MODDATAIMP-300) Updated marc4j version to 2.9.1
* [MODDICORE-41](https://issues.folio.org/browse/MODDICORE-41) Update mapping for Preceding/Succeeding Titles
* [MODDICORE-29](https://issues.folio.org/browse/MODDICORE-29) Support matching by STATIC_VALUE
* [MODDICORE-54](https://issues.folio.org/browse/MODDICORE-54) Field mappings: Date picker ###TODAY### logic does not work [BUGFIX]
* [MODDICORE-55](https://issues.folio.org/browse/MODDICORE-55) Added formatting of date from record to ISO format
* [MODDICORE-49](https://issues.folio.org/browse/MODDICORE-49) Applied archive/unarchive eventPayload mechanism
* Updated reference to raml-storage

## 2020-04-03 v2.0.0
* [MODDICORE-37](https://issues.folio.org/browse/MODDICORE-37) Added mechanism for archive/unarchive eventPayload
* [MODDICORE-38](https://issues.folio.org/browse/MODDICORE-38) Fixed DataImportEventPayload processing errors
* [MODDICORE-39](https://issues.folio.org/browse/MODDICORE-39) Fixed Matcher
* [MODDICORE-45](https://issues.folio.org/browse/MODDICORE-45) Fixed DI process finishes with ERROR status

## 2020-04-22 v1.1.2
* [MODDICORE-42](https://issues.folio.org/browse/MODDICORE-42) Filtered out electronic access entries with missing uri values in mapped Instances
* [MODDICORE-44](https://issues.folio.org/browse/MODDICORE-44) Null value if mapped field empty
* [MODDICORE-51](https://issues.folio.org/browse/MODDICORE-51) "Mode of issuance" values not assigned correctly using marc-to-instance map in Fameflower

## 2020-04-09 v1.1.1
* Changed algorithm for switching profiles

## 2020-04-06 v1.1.0
* [MODDICORE-38](https://issues.folio.org/browse/MODDICORE-38) Fixed DataImportEventPayload processing errors
* [MODDICORE-39](https://issues.folio.org/browse/MODDICORE-39) Fixed Matcher
* [MODDICORE-45](https://issues.folio.org/browse/MODDICORE-45) Fixed DI process finishes with ERROR status

## 2020-03-29 v1.0.2
* Fixed class cast in Matcher

## 2020-03-26 v1.0.1
* Implemented rule processor to work with mapping syntax
* Updated schemas reference

## 2020-03-06 v1.0.0
* Initial module setup
* Added event manager
* Added transport layer to publish event to consumer services
* Added mapping manager
* Added MARC readers
* Added common json writer
* Implemented MarcValueReader
* Added match expression processor
* Added HoldingsWriterFactory
* Implemented Event handling functionality
* Implemented Holdings, Instance, Item writers
* Implemented LoadQueryBuilder
* Implemented Rules processor
* Mechanism for zipping/unzipping added as util
