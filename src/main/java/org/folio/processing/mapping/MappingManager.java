package org.folio.processing.mapping;

import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.processing.exceptions.MappingException;
import org.folio.processing.mapping.mapper.FactoryRegistry;
import org.folio.processing.mapping.mapper.Mapper;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.ReaderFactory;
import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.WriterFactory;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;

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
  private static final Logger LOGGER = LoggerFactory.getLogger(MappingManager.class);
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
  public static DataImportEventPayload map(DataImportEventPayload eventPayload) {
    try {
      if (eventPayload.getCurrentNode().getContentType() != MAPPING_PROFILE) {
        LOGGER.info("Current node is not of {} content type", MAPPING_PROFILE);
        return eventPayload;
      }
      ProfileSnapshotWrapper mappingProfileWrapper = eventPayload.getCurrentNode();
      MappingProfile mappingProfile;
      if (mappingProfileWrapper.getContent() instanceof LinkedHashMap) {
        mappingProfile = new JsonObject((LinkedHashMap) mappingProfileWrapper.getContent()).mapTo(MappingProfile.class);
      } else {
        mappingProfile = (MappingProfile) mappingProfileWrapper.getContent();
      }
      Reader reader = FACTORY_REGISTRY.createReader(mappingProfile.getIncomingRecordType());
      Writer writer = FACTORY_REGISTRY.createWriter(mappingProfile.getExistingRecordType());
      return new Mapper() {
      }.map(reader, writer, eventPayload);
    } catch (Exception e) {
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
