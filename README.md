# data-import-processing-core

Copyright (C) 2019-2023 The Open Library Foundation

This software is distributed under the terms of the Apache License,
Version 2.0. See the file "[LICENSE](LICENSE)" for more information.

## Introduction

Core infrastructure for event processing for the DataImport.

## Match Engine

**Match Engine** is an abstract name for a functionality allowing for processing of the [MatchProfile](https://github.com/folio-org/data-import-raml-storage/blob/master/examples/mod-data-import-converter-storage/matchProfile.sample) logic of the data-import JobProfile.
Based on match results (MATCH or NON_MATCH) JobProfile processing flow can be branched.
Basically, matching is used for searching for a Record, on which a particular action should be applied. If such Record is found, actions specified "for matches" are executed, if not - "for non-matches" actions are performed.

![](images/match.png)

Match Engine consists of a number of components. The actual process of matching is invoked by calling match() method of the **MatchingManager**:

`MatchingManager.match(dataImportEventPayload)`

MatchingManager accepts [DataImportEventPayload](https://github.com/folio-org/data-import-raml-storage/blob/master/examples/mod-data-import/dataImportEventPayload.sample),
from which it extracts [MatchProfile](https://github.com/folio-org/data-import-raml-storage/blob/master/examples/mod-data-import-converter-storage/matchProfile.sample) and all the necessary information to perform matching.

The idea of matching is essentially a comparison of a particular value from incoming Record to the specified field of an existing one

![](images/incoming-to-existing.png)

To extract value from incoming Record a particular implementation of **MatchValueReader** is applied.
data-import-processing-core library currently contains implementation for reading values from MARC Bibliographic records:

![](images/incoming-marc-value.png)

There is also an implementation of a Reader for STATIC_VALUES (please note that match on static fields can only be used as a sub-match in a JobProfile):

![](images/static-values.png)

MatchValueReader also applies [Qualifier](https://github.com/folio-org/data-import-raml-storage/blob/master/schemas/mod-data-import-converter-storage/match-profile-detail/qualifierType.json) and [Comparison part](https://github.com/folio-org/data-import-raml-storage/blob/master/schemas/mod-data-import-converter-storage/match-profile-detail/comparisonPartType.json) to the value (based on the [MatchExpression](https://github.com/folio-org/data-import-raml-storage/blob/master/schemas/mod-data-import-converter-storage/match-profile-detail/matchExpression.json) for incoming Record specified in the MatchProfile)

![](images/qualifier.png)

![](images/comparison-part.png)

The extracted value from incoming Record is then matched against a particular field of an existing one:

![](images/existing-record-field.png)

To find (load) that existing Record - **MatchValueLoader** is used.

The implementation of MatchValueLoaders lies on the module, in which data-import processing is happening.

In order to allow MatchingManager to build MatchValueReader and MatchValueLoader (based on incoming and existing record types) one should register the appropriate implementations in MatchValueReaderFactory and MatchValueLoaderFactory respectively:

`MatchValueReaderFactory.register(new MarcValueReaderImpl());`

`MatchValueLoaderFactory.register(new InstanceLoader(storage, vertx));`

MatchingManager calls **Matcher** to perform the match itself.
Matcher uses **LoadQueryBuilder** to build a **LoadQuery** based on the value (extracted from incoming Record by MatchValueReader) and [MatchDetails](https://github.com/folio-org/data-import-raml-storage/blob/master/schemas/mod-data-import-converter-storage/match-profile-detail/matchDetail.json) specified in the profile.

LoadQueryBuilder supports building queries for String, List (multiple String values from incoming Record, like 035 field values from MARC Bibliographic records) and Date value types.

Resulting LoadQuery contains a CQL and SQL queries, that can be used by a particular MatchValueLoader implementation to find an entity.
SQL queries are suited for modules that have direct access to the db (mod-*-storage ones), while CQL queries are designed to be used in the business logic modules (like mod-inventory, since mod-inventory does not have the access to the db, it can only retrieve the entity from mod-inventory-storage via API, narrowing down the search with CQL).

The actual query is built based on the [MatchExpression](https://github.com/folio-org/data-import-raml-storage/blob/master/schemas/mod-data-import-converter-storage/match-profile-detail/matchExpression.json) for existing Record, which is extracted from the MatchDetails of the MatchProfile.
LoadQueryBuilder uses QueryHolder implementation for constructing basic query based on [MatchCriterion](https://github.com/folio-org/data-import-raml-storage/blob/master/schemas/mod-data-import-converter-storage/match-profile-detail/criterionType.json)

![](images/match-criterion.png)

It then applies [Qualifier](https://github.com/folio-org/data-import-raml-storage/blob/master/schemas/mod-data-import-converter-storage/match-profile-detail/qualifierType.json) and [Comparison part](https://github.com/folio-org/data-import-raml-storage/blob/master/schemas/mod-data-import-converter-storage/match-profile-detail/comparisonPartType.json) (see examples for incoming Record) to the query.

MatchValueLoader searches for an entity based on the constructed LoadQuery.
If entity with specified conditions is found, MATCH is considered successful, matched entity is saved to [DataImportEventPayload](https://github.com/folio-org/data-import-raml-storage/blob/master/examples/mod-data-import/dataImportEventPayload.sample) context
and actions for MATCH branch of the JobProfile are applied to that entity. If not - actions for NON_MATCH branch of the JobProfile are executed.
Multiple matches are not supported, in case multiple Records satisfy query conditions, an error is emitted and no action is performed.

## Mapping Engine

**Mapping Engine** is an abstract name for the functionality allowing for processing of the MappingProfile logic of the data-import . MappingEngine process incoming file (MarcBib, Marc Authority, etc) and maps them in FOLIO record type(Instance, Holdings, Item, etc)
Basically, mapping is used for updating, creating or modifying a record. UI provides functionality for defining [mapping rules](https://github.com/folio-org/data-import-raml-storage/blob/master/schemas/mod-data-import-converter-storage/mapping-profile-detail/mappingRule.json) for fields of selected record type.

![](images/mapping.png)

Mapping Engine consists of a number of components. The actual process of mapping is invoked by calling map() method of the MappingManager:

`MappingManager.map(dataImportEventPayload)`

MappingManager accepts [DataImportEventPayload](https://github.com/folio-org/data-import-raml-storage/blob/master/examples/mod-data-import/dataImportEventPayload.sample), from which it extracts MappingProfile and all the necessary information to perform mapping.


### Reader
To read value from incoming record by mapping rule is used **Reader**. The purpose of Reader is to read Value by rule from underlying entity.
Reader has to be initialized before read. Interface Reader has 2 methods:

`void initialize(DataImportEventPayload eventPayload, MappingContext mappingContext) throws IOException;`

`Value read(MappingRule ruleExpression);`

data-import-processing-core contains default realizations of reader for common incoming MARC and Edifact file: **MarcRecordReader**, **EdifactRecordReader**.

To define your own reader you need to implement the interface Reader and realize methods **initialize()** and **read()**.

In order to allow MappingManager to build Reader (based on incoming record types) one should register the appropriate implementations in ReaderFactory respectively:

`MappingManager.registerReaderFactory(new MarcBibReaderFactory());`

data-import-processing-core contains default realizations of reader factory for common incoming record types:
*  Marc bib => **MarcBibReaderFactory**
*  Marc authority => **MarcAuthorityReaderFactory**
*  Marc holdings => **MarcHoldingsReaderFactory**
*  Edifact record => **EdifactReaderFactory**

### Writer
To write the value to FOLIO record is used **Writer**. The purpose of Writer is to write a given Value to an underlying entity by the given fieldPath
Writer has to be initialized before writing. Interface Writer has 3 methods:

`void initialize(DataImportEventPayload eventPayload) throws IOException;`

`void write(String fieldPath, Value value);`

`DataImportEventPayload getResult(DataImportEventPayload eventPayload) throws JsonProcessingException;`

Method **write(String fieldPath, Value value)** accepts **fieldPath** which defines the place where the **value** should be located.

Result of writing could be received by calling **getResult(DataImportEventPayload eventPayload) throws JsonProcessingException;**, which defines result in **eventPayload**.

data-import-processing-core contains default realizations of writer for json: **JsonBasedWriter**.

In order to allow MappingManager to build Writer (based on existing record types) one should register the appropriate implementations in WriterFactory respectively:

`MappingManager.registerWriterFactory(new ItemWriterFactory());`

### Mapping flow
MappingManager calls Mapper to perform the mapping itself. Steps:
* Mapper goes through every [mapping rule](https://github.com/folio-org/data-import-raml-storage/blob/master/schemas/mod-data-import-converter-storage/mapping-profile-detail/mappingRule.json).
* Retrieve value via **Reader** by the rule
* Write values using **Writer** by fields path in [DataImportEventPayload](https://github.com/folio-org/data-import-raml-storage/blob/master/examples/mod-data-import/dataImportEventPayload.sample)

## Additional information

* See project [MODDICORE](https://issues.folio.org/browse/MODDICORE)
at the [FOLIO issue tracker](https://dev.folio.org/guidelines/issue-tracker).

* Other FOLIO Developer documentation is at [dev.folio.org](https://dev.folio.org/)

## Extended Authority Mapping
There is an extended Authority Mapping introduced to support advanced references classification in 4xx-5xx fields:
* broader terms (`$wg` tag)
* narrower terms (`$wh` tag)
* earlier headings (`$wa` tag)
* later headings (`$wb` tag)

To support this functionality `AuthorityExtended` is used together with `MarkToAuthorityExtendedMapper`.
