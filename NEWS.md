## 2020-04-03 v2.0.0
* [MODDICORE-37](https://issues.folio.org/browse/MODDICORE-37) Added mechanism for archive/unarchive eventPayload
* [MODDICORE-38](https://issues.folio.org/browse/MODDICORE-38) Fixed DataImportEventPayload processing errors
* [MODDICORE-39](https://issues.folio.org/browse/MODDICORE-39) Fixed Matcher
* [MODDICORE-45](https://issues.folio.org/browse/MODDICORE-45) Fixed DI process finishes with ERROR status
* [MODDICORE-44](https://issues.folio.org/browse/MODDICORE-44) Null value if mapped field empty

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
