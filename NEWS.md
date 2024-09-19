## 2024-09-16 v4.2.2
* [MODDICORE-413](https://folio-org.atlassian.net/browse/MODDICORE-413) Adjust mapping of order to fill in donor information

## 2024-04-19 v4.2.1
* [MODDICORE-388](https://folio-org.atlassian.net/browse/MODDICORE-388) Allow to map vendor details with code that contains brackets during order creation

## 2024-03-18 v4.2.0
* [MODDICORE-398](https://issues.folio.org/browse/MODDICORE-398) Update RMB v35.2.0, Vertx 4.5.4
* [MODSOURMAN-1085](https://issues.folio.org/browse/MODSOURMAN-1085) MARC record with a 100 tag without a $a is being discarded on import.
* [MODDICORE-382](https://issues.folio.org/browse/MODDICORE-382) Remove from the payload extra key after Multiple Optimistic Locking error reveals
* [MODSOURMAN-1022](https://issues.folio.org/browse/MODSOURMAN-1022) Remove step of initial saving of incoming records to SRS
* [MODDICORE-370](https://issues.folio.org/browse/MODDICORE-370) Mapped administrative notes creates multiple notes properties instead of one note property
* [MODINV-906](https://issues.folio.org/browse/MODINV-906) Create Item action discards when stat codes are in field mapping but not incoming MARC file
* [MODINV-944](https://folio-org.atlassian.net/browse/MODINV-944) OOM issue in mod-inventory
* [MODDICORE-368](https://issues.folio.org/browse/MODDICORE-368) 'else' statement in Field Mapping Profile for Statistical Code is ignored
* [MODDICORE-395](https://folio-org.atlassian.net/browse/MODDICORE-395) Allow creating multiple holdings/items when conditional mapping is used in field mapping profiles
* [MODDICORE-392](https://folio-org.atlassian.net/browse/MODDICORE-392) "MARC authority" record is assigned to authority file if record's prefix includes prefix of authority file
* [MODDICORE-390](https://folio-org.atlassian.net/browse/MODDICORE-390) Create Item action discards when stat codes are in field mapping but not incoming MARC file
  [MODINV-1071](https://folio-org.atlassian.net/browse/MODINV-1071) Extend Authority with Additional fields

## 2023-10-11 v4.1.0
* [MODDICORE-360](https://issues.folio.org/browse/MODDICORE-360) Migrate to folio-kafka-wrapper 3.0.0 version
* [MODDICORE-356](https://issues.folio.org/browse/MODDICORE-356) Upgrade data-import-processing-core to Java 17
* [MODDICORE-347](https://issues.folio.org/browse/MODDICORE-347) MARC bib - FOLIO instance mapping | Adjust contributor and relator term mapping WRT punctuation
* [MODDICORE-366](https://issues.folio.org/browse/MODDICORE-366) Remove distinct by permanentLocationId for multiple Holdings during mapping if Holdings already contain at the context
* [MODDICORE-308](https://issues.folio.org/browse/MODDICORE-308) Link wasn't deleted when user deletes linked repeatable field via "Data import" update
* [MODDICORE-309](https://issues.folio.org/browse/MODDICORE-309) Error when adding uncontrolled subfield in linked "MARC bib" record upon data import
* [MODDICORE-311](https://issues.folio.org/browse/MODDICORE-311) Unlinked field looks like linked on UI when update/import "MARC Bib" record when $9 is included in the imported file
* [MODDICORE-306](https://issues.folio.org/browse/MODDICORE-306) Order import: fix product identifier problem with ISBN field
* [MODDICORE-307](https://issues.folio.org/browse/MODDICORE-307) Order import: fix product identifier problem with 028 field
* [MODDICORE-319](https://issues.folio.org/browse/MODDICORE-319) Link is removed from field when updating/deleting "$0" in linked "MARC bib" field upon data import if field mapping profile does not allow "$0" update
* [MODDICORE-312](https://issues.folio.org/browse/MODDICORE-312) Modify linking check when several $0 income
* [MODDICORE-304](https://issues.folio.org/browse/MODDICORE-304) Cannot import statistical code values based on the text
* [MODDICORE-305](https://issues.folio.org/browse/MODDICORE-305) Order import: fix product identifier mapping problem with multiple product ID TYPES
* [MODDICORE-324](https://issues.folio.org/browse/MODDICORE-324) Update MARC bib field should retain subfield $9
* [MODDICORE-327](https://issues.folio.org/browse/MODDICORE-327) Retain link if field is protected
* [MODDICORE-326](https://issues.folio.org/browse/MODDICORE-326) Update all subfield to all subfields with same code
* [MODDICORE-322](https://issues.folio.org/browse/MODDICORE-322) Multiple 050 fields or subfields all map to Holdings/Item Call Number if Holdings/Item Mapping is set to 050$a or 050$a " " 050$b
* [MODDICORE-320](https://issues.folio.org/browse/MODDICORE-320) Update links interactions according to 'instance-authority-links' interface change to 2.0
* [MODDICORE-323](https://issues.folio.org/browse/MODDICORE-323) Problems with default MARC-Instance mapping when some call number fields are repeated or have repeated subfields
* [MODDICORE-325](https://issues.folio.org/browse/MODDICORE-325) Use linkable tags from linking rules, not hardcoded
* [MODDICORE-338](https://issues.folio.org/browse/MODDICORE-338) Update of "MARC Bib" record completed with error
* [MODDICORE-335](https://issues.folio.org/browse/MODDICORE-335) Controlled subfields of Second repeatable and linked field could be overwritten by "Data import" update
* [MODSOURMAN-974](https://issues.folio.org/browse/MODSOURMAN-974) MARC bib $9 handling: remove $9 subfields from linkable fields
* [MODDICORE-342](https://issues.folio.org/browse/MODDICORE-342) Updated action profile schema
* [MODDICORE-346](https://issues.folio.org/browse/MODDICORE-346) Improve handling $0 and $9 in repeatable fields
* [MODDICORE-341](https://issues.folio.org/browse/MODDICORE-341) MARC Bib record doesn't open after update of repeatable linked field "$0" using mapping profile which allows to update
* [MODDICORE-349](https://issues.folio.org/browse/MODDICORE-349) Fix handling field updates for mapping details tag '*' in repeatable
* [MODDICORE-363](https://issues.folio.org/browse/MODDICORE-363) Electronic access mapping from the 856 to the holdings is not maintaining relationships between url and link text (MODDICORE-333)
* [MODDICORE-340](https://issues.folio.org/browse/MODDICORE-340) Allow mapping multiple items/holdings from context
* [MODDICORE-90](https://issues.folio.org/browse/MODDICORE-90) Change Mapper to allow mapping of multiple Items
* [MODDICORE-336](https://issues.folio.org/browse/MODDICORE-336) Change Mapper to allow mapping of multiple Holdings
* [MODDICORE-317](https://issues.folio.org/browse/MODDICORE-317) Add support for matching multiple entities from one incoming MARC record
* [MODDICORE-340](https://issues.folio.org/browse/MODDICORE-340) Allow mapping multiple items/holdings from context
* [MODINV-862](https://issues.folio.org/browse/MODINV-862) Add support for json array of entities represented in String
* [FAT-6493](https://issues.folio.org/browse/FAT-6493) Set not matched number for single holdings match
* [MODDICORE-314](https://issues.folio.org/browse/MODDICORE-314) Remove redundant RequiredFields enum
* [MODDATAIMP-926](https://issues.folio.org/browse/MODDATAIMP-926) Add consortium-based source values into authority source

## 2023-03-30 v4.0.5
* [MODDICORE-322](https://issues.folio.org/browse/MODDICORE-322) Multiple 050 fields or subfields all map to Holdings/Item Call Number if Holdings/Item Mapping is set to 050$a or 050$a " " 050$b

## 2023-02-17 v4.0.0
* [MODSOURMAN-873](https://issues.folio.org/browse/MODSOURMAN-873) Add MARC 720 field to default MARC Bib-Instance mapping and adjust relator term mapping. Expanded default mapper with alternative mapping logic
* [MODSOURMAN-837](https://issues.folio.org/browse/MODSOURMAN-837) MARC bib - FOLIO instance mapping | Update default mapping to change how Relator term is populated on instance record
* [MODDICORE-294](https://issues.folio.org/browse/MODDICORE-294) Add handling accepted values for repeatable fields
* [MODDICORE-290](https://issues.folio.org/browse/MODDICORE-290) Extend Mapper for Order/OrderLines.
* [MODDICORE-19](https://issues.folio.org/browse/MODDICORE-19) Add documentation for mapping engine
* [MODDICORE-246](https://issues.folio.org/browse/MODDICORE-246) Logging improvement
* [MODDICORE-294](https://issues.folio.org/browse/MODDICORE-294) Multiple item notes not creating
* [MODDATAIMP-736](https://issues.folio.org/browse/MODDATAIMP-736) Adjust logging configuration to display datetime in a proper format
* [MODDICORE-293](https://issues.folio.org/browse/MODDICORE-293) Handle bib-authority link when user updates a bib record via data import
* [MODSOURCE-567](https://issues.folio.org/browse/MODSOURCE-567) Generate srs schemas
* [MODDICORE-297](https://issues.folio.org/browse/MODDICORE-297) Mapping bib's $9 into subjects, series, alternativeTitles fields
* [MODINV-774](https://issues.folio.org/browse/MODINV-774) Set POLine id when Item is created by DI
* [MODDATAIMP-750](https://issues.folio.org/browse/MODDATAIMP-750) Update util libraries dependencies
* [KAFKAWRAP-33](https://issues.folio.org/browse/KAFKAWRAP-33) Updated folio-kafka-wrapper to 2.7.0
* [MODORDERS-844](https://issues.folio.org/browse/MODORDERS-844) Add organizations to MappingParameters
* [MODDICORE-303](https://issues.folio.org/browse/MODDICORE-303) Allow for mapping the vendor, material supplier, and access provider based on CODE in MARC record.

## 2022-10-19 v3.5.1
* [MODDICORE-281](https://issues.folio.org/browse/MODDICORE-281) Extend instance contributors schema with Authority ID
* [MODSOURMAN-838](https://issues.folio.org/browse/MODSOURMAN-838) Search by LCCN "010 $a" subfield value with "\" at the end don't retrieve results
* [MODDICORE-285](https://issues.folio.org/browse/MODDICORE-285) Assign each authority record to an Authority Source file list
* [MODDICORE-283](https://issues.folio.org/browse/MODDICORE-283) Change logic of sourceFileId populating by adding Authority Natural ID
* [MODDICORE-276](https://issues.folio.org/browse/MODDICORE-276) Create function to populate an Authority Source file ID
* [MODDICORE-271](https://issues.folio.org/browse/MODDICORE-271) Support MARC-MARC Holdings update action
* [MODDICORE-280](https://issues.folio.org/browse/MODDICORE-280) Import is failing due to authoritySourceFiles

## 2022-09-02 v3.4.1
* [MODDICORE-248](https://issues.folio.org/browse/MODDICORE-248) MARC field protections apply to MARC modifications of incoming records when they should not

## 2022-06-23 v3.4.0
* [MODINV-671](https://issues.folio.org/browse/MODINV-671) Check and fix the sending of DI_ERROR after DuplicateEventException appears
* [MODDICORE-265](https://issues.folio.org/browse/MODDICORE-265) Implement method to update record fields according to field protections
* [MODDICORE-269](https://issues.folio.org/browse/MODDICORE-269) Add support of exclusiveSubfield option
* [MODDICORE-276](https://issues.folio.org/browse/MODDICORE-276) Create function to populate an Authority Source file ID

## 2021-02-22 v3.3.0
* [MODDICORE-222](https://issues.folio.org/browse/MODDICORE-222) Authority: Add normalisation function to set note types
* [MODDATAIMP-491](https://issues.folio.org/browse/MODDATAIMP-491) Improve logging to be able to trace the path of each record and file_chunks
* [MODDICORE-225](https://issues.folio.org/browse/MODDICORE-225) When EDIFACT record is imported intermittently currency is not mapped when it comes from record field
* [MODDATAIMP-592](https://issues.folio.org/browse/MODDATAIMP-592) Data Import matches on identifier type and identifier value separately, resulting in incorrect matches
* [MODDICORE-188](https://issues.folio.org/browse/MODDICORE-188) Remove pubsub client dependency
* [MODDICORE-240](https://issues.folio.org/browse/MODDICORE-240) EDIFACT Inv: Empty Invoice line Description in field mapping profile fails Data Import job
* [MODSOURMAN-675](https://issues.folio.org/browse/MODSOURMAN-675) Data Import handles repeated 020 $a:s in an unexpected manner when creating Instance Identifiers
* [MODDICORE-204](https://issues.folio.org/browse/MODDICORE-204) Overlaying with single record import creates duplicate control fields
* [MODDICORE-205](https://issues.folio.org/browse/MODDICORE-205) Update the MARC-Instance field mapping for InstanceType (336$a and $b)
* [MODDICORE-206](https://issues.folio.org/browse/MODDICORE-206) Blank fields generated from MARC mapping create invalid Instance records in Inventory
* [MODDICORE-241](https://issues.folio.org/browse/MODDICORE-241) MARC 007 is retained although overlay does not contain MARC 007 field
* [MODDICORE-195](https://issues.folio.org/browse/MODDICORE-195) FOLIO snapshot throw optimistic locking error when updating an instance
* [MODDICORE-244](https://issues.folio.org/browse/MODDICORE-244) Rename fields in Authority schema
* [MODDICORE-237](https://issues.folio.org/browse/MODDICORE-237) Authority update: Make MarcRecordModifier support MARC Authority
* [MODDICORE-235](https://issues.folio.org/browse/MODDICORE-235) Authority update: Add new event types
* [MODDICORE-216](https://issues.folio.org/browse/MODDICORE-216) Cannot edit a MARC Holdings record created with Add MARC Holdings record
* upgrade dependency on folio-kafka-wrapper to v2.5.0

## 2022-02-08 v3.2.8
* [MODDICORE-242](https://issues.folio.org/browse/MODDICORE-242) MARC 007 is retained although overlay does not contain MARC 007 field

## 2021-12-21 v3.2.7
* Fix log4j vulnerability (upgrade to folio-kafka-wrapper v2.4.2)

## 2021-12-21 v3.2.6
* [MODDICORE-230](https://issues.folio.org/browse/MODDICORE-230) Fix log4j vulnerability (upgrade to folio-kafka-wrapper v2.4.1)
* [MODDICORE-231](https://issues.folio.org/browse/MODDICORE-231) Fix matching on identifier type and identifier value

## 2021-11-19 v3.2.5
* [MODDICORE-200](https://issues.folio.org/browse/MODDICORE-200) Fix duplicating values of repeatable control fields on MARC bib update

## 2021-11-15 v3.2.4
* [MODDICORE-199](https://issues.folio.org/browse/MODDICORE-199) Add EDIFACT mapping syntax for multiple fields mapping into 1 invoice field

## 2021-11-08 v3.2.3
* [MODDICORE-209](https://issues.folio.org/browse/MODDICORE-209) Support new property "_version" in the Instance

## 2021-10-29 v3.2.2
* [MODDICORE-187](https://issues.folio.org/browse/MODDICORE-187) Blank fields generated from MARC mapping create invalid Instance records in Inventory
* [MODDICORE-184](https://issues.folio.org/browse/MODDICORE-184) Update the MARC-Instance field mapping for InstanceType (336$a and $b)
* [MODDICORE-200](https://issues.folio.org/browse/MODDICORE-200) Overlaying with single record import creates duplicate control fields
* [MODDICORE-198](https://issues.folio.org/browse/MODDICORE-198) Fix the effect of DI_ERROR messages when trying to duplicate records on the import job progress bar

## 2021-10-08 v3.2.1
* [MODDICORE-192](https://issues.folio.org/browse/MODDICORE-192) Fix of Incorrect mapping of acquisition ids that caused validation issues and Invoices creating failures

## 2021-09-29 v3.2.0
* [MODDICORE-171](https://issues.folio.org/browse/MODDICORE-171) Add default mapping profile for MARC holdings
* [MODDICORE-175](https://issues.folio.org/browse/MODDICORE-175) Ad Mapper for Holdings
* [MODSOURCE-340](https://issues.folio.org/browse/MODSOURCE-340) Lower log level for messages when no handler found
* [MODDICORE-186](https://issues.folio.org/browse/MODDICORE-186) Fix import of EDIFACT invoices
* [MODSOURCE-286](https://issues.folio.org/browse/MODSOURCE-286) Remove zipping mechanism for data import event payloads and use cache for mapping params and job profile snapshot
* [MODDICORE-172](https://issues.folio.org/browse/MODDICORE-172) Add MARC-Instance field mapping for New identifier types
* Update folio-kafka-wrapper dependency to v2.4.0

## 2021-0804 v3.1.4
* [MODDICORE-166](https://issues.folio.org/browse/MODDICORE-166)  Near the day boundary data import calculates today incorrectly
* Update folio-kafka-wrapper dependency to v2.3.3

## 2021-07-21 v3.1.3
* [MODDICORE-162](https://issues.folio.org/browse/MODDICORE-162) Mode of issuance not updated when overlaying or updating record with existing SRS
* [MODDICORE-165](https://issues.folio.org/browse/MODDICORE-165) Data import matches to first possible location in list  instead of exact location.
* [MODDICORE-164](https://issues.folio.org/browse/MODDICORE-164) Error in marc to instance mapping
* [MODSOURMAN-527](https://issues.folio.org/browse/MODSOURMAN-527) Cannot import EDIFACT invoices

## 2021-06-25 v3.1.2
* [MODDICORE-153](https://issues.folio.org/browse/MODDICORE-153) Change dataType to have common type for MARC related subtypes

## 2021-06-17 v3.1.1
* Update folio-kafka-wrapper dependency to v2.3.1

## 2021-06-11 v3.1.0
* [MODSOURCE-279](https://issues.folio.org/browse/MODSOURCE-279) Store MARC Authority record
* [MODDICORE-150](https://issues.folio.org/browse/MODDICORE-150) Fix util methods to support modules with different rmb versions.

## 2021-07-15 v3.0.4
* [MODDICORE-159](https://issues.folio.org/browse/MODDICORE-159) Mode of issuance not updated when overlaying or updating record with existing SRS
* [MODDICORE-163](https://issues.folio.org/browse/MODDICORE-163) Use reliable apache commons fo mapping MARC records to Instance records

## 2021-06-17 v3.0.3
* Update folio-kafka-wrapper dependency to v2.0.8

## 2021-05-21 v3.0.2
* [MODDICORE-136](https://issues.folio.org/browse/MODDICORE-136) OCLC record imported via Inventory and then updated via Inventory does not update properly
* [MODDICORE-137](https://issues.folio.org/browse/MODDICORE-137) Fixed ###REMOVE### expression logic, added support for fields represented as object in entity schema

## 2021-04-22 v3.0.1
* [MODDICORE-127](https://issues.folio.org/browse/MODDICORE-127) Location code-Loan type assignment problems [BUGFIX]
* [MODSOURMAN-437](https://issues.folio.org/browse/MODSOURMAN-437) Add correlationId header to kafka record on publishing
* [MODDICORE-128](https://issues.folio.org/browse/MODDICORE-128) Holdings fails to create due to Location code not being recognized.
* [MODDICORE-135](https://issues.folio.org/browse/MODDICORE-135) Holdings fails to create due to Location code not being recognized[BUGFIX]
* [MODDICORE-132](https://issues.folio.org/browse/MODDICORE-132) Holdings and item record are not created due to electronicAccess without uri

## 2021-03-12 v3.0.0
* [MODDICORE-82](https://issues.folio.org/browse/MODDICORE-82) Change transport layer implementation to use Kafka
* [MODDICORE-114](https://issues.folio.org/browse/MODDICORE-114) Add MARC-Instance default mappings for 880 fields.
* [MODDSOURMAN-377](https://issues.folio.org/browse/MODSOURMAN-377) Update 5xx Notes mappings to indicate staff only for some notes.
* [MODDICORE-111](https://issues.folio.org/browse/MODDICORE-111) Add personal data disclosure form.
* [MODDICORE-115](https://issues.folio.org/browse/MODDICORE-115) Add implementation for EDIFACT reader
* [MODDICORE-116](https://issues.folio.org/browse/MODDICORE-116) Support for invoice adjustments mapping

## 2020-11-20 v2.2.1
* [MODDICORE-103](https://issues.folio.org/browse/MODDICORE-103) Fixed searching for next match profile

## 2020-10-09 v2.2.0
* [MODDICORE-59](https://issues.folio.org/browse/MODDICORE-59) Implemented MARC Record Writer/Modifier
* [MODDICORE-69](https://issues.folio.org/browse/MODDICORE-69) Implemented ###REMOVE### expression logic
* [MODDICORE-53](https://issues.folio.org/browse/MODDICORE-53) Refactored matching in asynchronous style
* [MODDICORE-81](https://issues.folio.org/browse/MODDICORE-81) 856$3 not mapping into holdings record
* [MODSOURMAN-281](https://issues.folio.org/browse/MODSOURMAN-281) Added support for event post-processing
* [MODDICORE-77](https://issues.folio.org/browse/MODDICORE-77) Applied MARC field mapping protection settings
* [MODSOURCE-184](https://issues.folio.org/browse/MODSOURCE-184) Added support for "Update" option of mapping profile for marc bib modification
* [MODDICORE-94](https://issues.folio.org/browse/MODDICORE-94) Edit action for Modify MARC action profile works only for explicitly specified fields
* [MODDICORE-85](https://issues.folio.org/browse/MODDICORE-85) Added support to match by multiple values (fix matching MARC 035 to Instance Identifier)
* [MODINV-346](https://issues.folio.org/browse/MODINV-346) Problem with the repeatable check in/out notes field mapping actions
* [MODDICORE-88](https://issues.folio.org/browse/MODDICORE-88) Refine identifier matching for Instances

## 2020-08-10 v2.1.6
* [MODDICORE-70](https://issues.folio.org/browse/MODDICORE-70) Actions in mapping profile don`t work correctly - BUGFIX
* [MODDICORE-72](https://issues.folio.org/browse/MODDICORE-72) Create holdings fails because mapping for holdings statement is not working - BUGFIX
* [MODDICORE-74](https://issues.folio.org/browse/MODDICORE-74) Create holdings fails because mapping for Former holdings ID is not working - BUGFIX
* [MODDICORE-75](https://issues.folio.org/browse/MODDICORE-75) Two 856 fields smushed into 1 eAccess row in holdings record
* [MODDICORE-76](https://issues.folio.org/browse/MODDICORE-76) Add support for wildcard indicators and empty indicators to the match engine

## 2020-07-08 v2.1.5
* [MODDICORE-61](https://issues.folio.org/browse/MODDICORE-61) Field mappings: Repeatable fields dropdown action without subfields support

## 2020-06-29 v2.1.4
* Fix error log

## 2020-06-26 v2.1.3
* Fix condition in normalization function to avoid string index out of bounds
* Add details to logs

## 2020-06-05 v2.1.2
* [MODDICORE-52](https://issues.folio.org/browse/MODDICORE-52) Add support for MappingEngine work with a leader
* [MODDICORE-66](https://issues.folio.org/browse/MODDICORE-66) Mapping exception in mod-inventory with rules for notes - BUGFIX.
* [MODDICORE-62](https://issues.folio.org/browse/MODDICORE-62) Adjusted handling of repeatable fields
* [MODDICORE-63](https://issues.folio.org/browse/MODDICORE-63) Support matching by setting name in case schema contains UUID

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
