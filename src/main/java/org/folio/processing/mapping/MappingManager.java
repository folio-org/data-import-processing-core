package org.folio.processing.mapping;

import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.processing.exceptions.MappingException;
import org.folio.processing.mapping.mapper.FactoryRegistry;
import org.folio.processing.mapping.mapper.Mapper;
import org.folio.processing.mapping.mapper.MappingContext;
import org.folio.processing.mapping.mapper.mappers.MapperFactory;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.ReaderFactory;
import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.WriterFactory;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;

import java.util.Map;

import static org.folio.processing.events.utils.EventUtils.extractRecordId;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;

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
        LOGGER.info("map:: Current node is not of {} content type jobExecutionId: {} recordId: {}",
          MAPPING_PROFILE, eventPayload.getJobExecutionId(), extractRecordId(eventPayload));
        return eventPayload;
      }
      ProfileSnapshotWrapper mappingProfileWrapper = eventPayload.getCurrentNode();

      MappingProfile mappingProfile;
      if (mappingProfileWrapper.getContent() instanceof Map) {
        mappingProfile = new JsonObject((Map) mappingProfileWrapper.getContent()).mapTo(MappingProfile.class);
      } else {
        mappingProfile = (MappingProfile) mappingProfileWrapper.getContent();
      }

      Reader reader = FACTORY_REGISTRY.createReader(mappingProfile.getIncomingRecordType());
      Writer writer = FACTORY_REGISTRY.createWriter(mappingProfile.getExistingRecordType());

      Mapper mapper = FACTORY_REGISTRY.createMapper(eventPayload, reader, writer);
      return mapper.map(mappingProfile, eventPayload, mappingContext);
    } catch (Exception e) {
      LOGGER.warn("map:: Failed to perform mapping jobExecutionId: {} recordId: {}",
        eventPayload.getJobExecutionId(), extractRecordId(eventPayload), e);
      throw new MappingException(e);
    }
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

  public static boolean registerMapperFactory(MapperFactory factory) {
    return FACTORY_REGISTRY.getMapperFactories().add(factory);
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

  public static void clearMapperFactories() {
    FACTORY_REGISTRY.getMapperFactories().clear();
  }
}
