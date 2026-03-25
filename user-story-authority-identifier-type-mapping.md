## Purpose/Overview

As a cataloger importing MARC Authority records,
I want the system to resolve authority identifier types by code during data mapping,
So that identifier type IDs are automatically populated based on the identifier type code, enabling accurate and efficient authority record imports.

Currently, the data-import-processing-core module supports resolving various reference data types (e.g., classification types, contributor types) during MARC-to-FOLIO mapping. However, there is no support for resolving **Authority Identifier Types** by code. This means authority identifiers cannot be automatically matched to their corresponding type IDs during import, requiring manual intervention or causing incomplete records.

### Technical Approach

- Define JSON schemas for `AuthorityIdentifierType` (single object) and `Authorityidentifiertypes` (collection) under `ramls/settings/`.
- Register the collection schema in `pom.xml` for Java POJO generation via the `jsonschema2pojo` Maven plugin.
- Extend `MappingParameters` with a new `authorityIdentifierTypes` field (getter, setter, fluent builder).
- Add a new `SET_AUTHORITY_IDENTIFIER_TYPE_ID_BY_CODE` normalization function that looks up an identifier type ID by matching the `code` parameter (case-insensitive, trimmed).

---

## Requirements/Scope

### Functional Requirements
1. A new JSON schema `authorityidentifiertype.json` shall define the structure of an authority identifier type with `id`, `name`, `code`, `source`, and `metadata` properties.
2. A collection schema `authorityidentifiertypes.json` shall define a paginated list of authority identifier types.
3. Java POJOs (`AuthorityIdentifierType`, `Authorityidentifiertypes`) shall be generated from these schemas at build time.
4. `MappingParameters` shall support storing and retrieving a list of `AuthorityIdentifierType` objects.
5. A new normalization function `SET_AUTHORITY_IDENTIFIER_TYPE_ID_BY_CODE` shall resolve an authority identifier type ID by matching the `code` rule parameter against the list of authority identifier types in mapping parameters.
6. Code matching shall be case-insensitive and shall trim whitespace from stored codes.
7. If no matching identifier type is found, the function shall return an empty string.
8. If the identifier types list or the code parameter is null, the function shall return the `STUB_FIELD_TYPE_ID` constant.

---

## Acceptance Criteria

**AC1: Identifier type resolved by code**
- Given mapping parameters contain an authority identifier type with code "lccn" and a known ID
  When the `SET_AUTHORITY_IDENTIFIER_TYPE_ID_BY_CODE` function is invoked with rule parameter `{"code": "lccn"}`
  Then the function returns the matching identifier type's ID

**AC2: Case-insensitive matching**
- Given mapping parameters contain an authority identifier type with code "LCCN"
  When the function is invoked with rule parameter `{"code": "lccn"}`
  Then the function returns the matching identifier type's ID

**AC3: Whitespace-trimmed matching**
- Given mapping parameters contain an authority identifier type with code " lccn "
  When the function is invoked with rule parameter `{"code": "lccn"}`
  Then the function returns the matching identifier type's ID

**AC4: No match returns empty string**
- Given mapping parameters contain authority identifier types but none with code "nonexistent"
  When the function is invoked with rule parameter `{"code": "nonexistent"}`
  Then the function returns an empty string

**AC5: Null identifier types returns stub ID**
- Given mapping parameters have a null authority identifier types list
  When the function is invoked with any code parameter
  Then the function returns `STUB_FIELD_TYPE_ID`

**AC6: Null code parameter returns stub ID**
- Given mapping parameters contain valid authority identifier types
  When the function is invoked with a null code parameter
  Then the function returns `STUB_FIELD_TYPE_ID`

**AC7: JSON schemas generate valid POJOs**
- Given the `authorityidentifiertype.json` and `authorityidentifiertypes.json` schemas are defined
  When the project is built with Maven
  Then `AuthorityIdentifierType` and `Authorityidentifiertypes` Java classes are generated with correct fields and accessors

---

## Testing Guidance

### Manual Testing

**Scenario 1: Verify identifier type resolution during MARC Authority import**
1. Configure the system with authority identifier types (e.g., "lccn", "isbn").
2. Import a MARC Authority record that contains an identifier field mapped with the `SET_AUTHORITY_IDENTIFIER_TYPE_ID_BY_CODE` function.
3. Verify the resulting Authority record has the correct identifier type ID populated.

**Scenario 2: Verify behavior with unknown code**
1. Import a MARC Authority record with an identifier code that does not match any configured authority identifier type.
2. Verify the identifier type ID field is empty (not populated with an incorrect value).

**Scenario 3: Verify build generates schemas**
1. Run `mvn clean install`.
2. Verify that `AuthorityIdentifierType.java` and `Authorityidentifiertypes.java` are generated in the `target/` directory.
3. Confirm the generated classes include `id`, `name`, `code`, `source`, and `metadata` fields with appropriate getters/setters.

**Note:** Unit test specs, integration test code, and test data details belong in the implementation plan, not here.

---

## Related Links
- Schema: `ramls/settings/authorityidentifiertype.json`
- Collection schema: `ramls/settings/authorityidentifiertypes.json`
- Normalization function: `NormalizationFunction.java`
- Mapping parameters: `MappingParameters.java`
- Unit test: `NormalizationFunctionTest.java`
