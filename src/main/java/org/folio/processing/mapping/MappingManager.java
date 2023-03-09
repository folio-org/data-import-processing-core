package org.folio.processing.mapping;

import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.Location;
import org.folio.MappingProfile;
import org.folio.Organization;
import org.folio.StatisticalCode;
import org.folio.processing.exceptions.MappingException;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.FactoryRegistry;
import org.folio.processing.mapping.mapper.Mapper;
import org.folio.processing.mapping.mapper.MappingContext;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.ReaderFactory;
import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.WriterFactory;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.jaxrs.model.RepeatableSubfieldMapping;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;

/**
 * MappingManager is the entry point to work with mapping.
 * Provides methods to register factories and start mapping.
 *
 * @see MappingProfile
 * @see FactoryRegistry
 * @see Reader
 * @see Writer
 */
public final class MappingManager {
  private static final Logger LOGGER = LogManager.getLogger(MappingManager.class);
  private static final FactoryRegistry FACTORY_REGISTRY = new FactoryRegistry();
  public static final String PERMANENT_LOCATION_ID = "permanentLocationId";
  public static final String TEMPORARY_LOCATION_ID = "temporaryLocationId";
  public static final String VENDOR_ID = "vendor";
  private static final String MATERIAL_SUPPLIER = "materialSupplier";
  private static final String ACCESS_PROVIDER = "accessProvider";
  private static final String STATISTICAL_CODE_ID = "statisticalCodeId";
  private static final String STATISTICAL_CODE_IDS = "statisticalCodeIds";

  private MappingManager() {
  }

  /**
   * The entry point for mapping.
   *
   * @param eventPayload event payload
   * @return event payload
   * @see MappingProfile
   * @see Mapper
   */
  public static DataImportEventPayload map(DataImportEventPayload eventPayload, MappingContext mappingContext) {
    try {
      if (eventPayload.getCurrentNode().getContentType() != MAPPING_PROFILE) {
        LOGGER.info("map:: Current node is not of {} content type", MAPPING_PROFILE);
        return eventPayload;
      }
      ProfileSnapshotWrapper mappingProfileWrapper = eventPayload.getCurrentNode();

      MappingProfile mappingProfile;
      if (mappingProfileWrapper.getContent() instanceof Map) {
        mappingProfile = new JsonObject((Map) mappingProfileWrapper.getContent()).mapTo(MappingProfile.class);
      } else {
        mappingProfile = (MappingProfile) mappingProfileWrapper.getContent();
      }

      //Fix MODDICORE-128 (The system doesn't update acceptedLocation in mapping profiles after the location list is changed)
      updateLocationsInMappingProfile(mappingProfile, mappingContext.getMappingParameters());
      updateOrganizationsInMappingProfile(mappingProfile, mappingContext.getMappingParameters());
      updateStatisticalCodesInMappingProfile(mappingProfile, mappingContext.getMappingParameters());

      Reader reader = FACTORY_REGISTRY.createReader(mappingProfile.getIncomingRecordType());
      Writer writer = FACTORY_REGISTRY.createWriter(mappingProfile.getExistingRecordType());
      return new Mapper() {
      }.map(reader, writer, mappingProfile, eventPayload, mappingContext);
    } catch (Exception e) {
      LOGGER.warn("map:: Failed to perform mapping", e);
      throw new MappingException(e);
    }
  }

  /**
   * Fill Permanent and Temporary Locations in MappingProfile from {@code mappingParameters}
   *
   * @param mappingProfile - MappingProfile
   * @param mappingParameters - mapping parameters
   */
  private static void updateLocationsInMappingProfile(MappingProfile mappingProfile, MappingParameters mappingParameters) {
    if ((mappingProfile.getMappingDetails() != null) && (mappingProfile.getMappingDetails().getMappingFields() != null)) {
      HashMap<String, String> locations = getLocationsFromMappingParameters(mappingParameters);
      if (!locations.isEmpty()) {
        for (MappingRule mappingRule : mappingProfile.getMappingDetails().getMappingFields()) {
          if ((mappingRule.getName() != null) && (mappingRule.getName().equals(PERMANENT_LOCATION_ID) || mappingRule.getName().equals(TEMPORARY_LOCATION_ID))) {
            mappingRule.setAcceptedValues(locations);
          }
        }
      }
    }
  }

  /**
   * Fill {@link Organization} accepted values in MappingProfile from {@code mappingParameters}
   *
   * @param mappingProfile - MappingProfile
   * @param mappingParameters - mapping parameters
   */
  private static void updateOrganizationsInMappingProfile(MappingProfile mappingProfile, MappingParameters mappingParameters) {
    if ((mappingProfile.getMappingDetails() != null) && (mappingProfile.getMappingDetails().getMappingFields() != null)) {
      HashMap<String, String> organizations = getOrganizationsFromMappingParameters(mappingParameters);
      if (!organizations.isEmpty()) {
        for (MappingRule mappingRule : mappingProfile.getMappingDetails().getMappingFields()) {
          if ((mappingRule.getName() != null) && (mappingRule.getName().equals(VENDOR_ID)
            || mappingRule.getName().equals(MATERIAL_SUPPLIER) || mappingRule.getName().equals(ACCESS_PROVIDER))) {
            mappingRule.setAcceptedValues(organizations);
          }
        }
      }
    }
  }

  /**
   * Fill {@link StatisticalCode} accepted values in MappingProfile from {@code mappingParameters}
   *
   * @param mappingProfile - MappingProfile
   * @param mappingParameters - mapping parameters
   */
  private static void updateStatisticalCodesInMappingProfile(MappingProfile mappingProfile, MappingParameters mappingParameters) {
    if ((mappingProfile.getMappingDetails() != null) && (mappingProfile.getMappingDetails().getMappingFields() != null)) {
      HashMap<String, String> statisticalCodes = getStatisticalCodesFromMappingParameters(mappingParameters);
      if (!statisticalCodes.isEmpty()) {
        for (MappingRule mappingRule : mappingProfile.getMappingDetails().getMappingFields()) {
          if (Objects.equals(mappingRule.getName(), STATISTICAL_CODE_IDS)) {
            for (RepeatableSubfieldMapping subfield : mappingRule.getSubfields()) {
              for (MappingRule field : subfield.getFields()) {
                if(Objects.equals(field.getName(), STATISTICAL_CODE_ID)) {
                  field.setAcceptedValues(statisticalCodes);
                }
              }
            }
          }
        }
      }
    }
  }

  private static HashMap<String, String> getLocationsFromMappingParameters(MappingParameters mappingParameters) {
    HashMap<String, String> locations = new HashMap<>();
    for (Location location : mappingParameters.getLocations()) {
      StringBuilder locationValue = new StringBuilder()
        .append(location.getName())
        .append(" (")
        .append(location.getCode())
        .append(")");
      locations.put(location.getId(), String.valueOf(locationValue));
    }
    return locations;
  }

  private static HashMap<String, String> getOrganizationsFromMappingParameters(MappingParameters mappingParameters) {
    HashMap<String, String> organizations = new HashMap<>();
    if (mappingParameters.getOrganizations() == null) return organizations;
    for (Organization organization : mappingParameters.getOrganizations()) {
      StringBuilder organizationValue = new StringBuilder()
        .append(organization.getCode())
        .append(" (")
        .append(organization.getId())
        .append(")");
      organizations.put(organization.getId(), String.valueOf(organizationValue));
    }
    return organizations;
  }

  private static HashMap<String, String> getStatisticalCodesFromMappingParameters(MappingParameters mappingParameters) {
    HashMap<String, String> statisticalCodes = new HashMap<>();
    if (mappingParameters.getStatisticalCodes() == null) return statisticalCodes;
    for (StatisticalCode statisticalCode : mappingParameters.getStatisticalCodes()) {
      StringBuilder statisticalCodeValue = new StringBuilder();
      if (!statisticalCode.getName().contains("(" + statisticalCode.getCode() + ")")) {
        statisticalCodeValue.append(statisticalCode.getName())
          .append(" (")
          .append(statisticalCode.getCode())
          .append(")");
      } else {
        statisticalCodeValue.append(statisticalCode.getName());
      }
      statisticalCodes.put(statisticalCode.getId(), statisticalCodeValue.toString());
    }
    return statisticalCodes;
  }

  /**
   * Registers reader factory
   *
   * @param factory reader factory
   * @return true if registry changed as a result of the call
   */
  public static boolean registerReaderFactory(ReaderFactory factory) {
    return FACTORY_REGISTRY.getReaderFactories().add(factory);
  }

  /**
   * Registers writer factory
   *
   * @param factory writer factory
   * @return true if registry changed as a result of the call
   */
  public static boolean registerWriterFactory(WriterFactory factory) {
    return FACTORY_REGISTRY.getWriterFactories().add(factory);
  }

  /**
   * Clears the registry of reader factories
   */
  public static void clearReaderFactories() {
    FACTORY_REGISTRY.getReaderFactories().clear();
  }

  /**
   * Clears the registry of writer factories
   */
  public static void clearWriterFactories() {
    FACTORY_REGISTRY.getWriterFactories().clear();
  }
}
