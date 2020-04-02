package org.folio.processing.events.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;

/**
 * ZIP archiver util for zip and unzip specific string using GZIPOutputStream and Base64-algorithm.
 */
public final class ZIPArchiver {

  private ZIPArchiver() {
  }

  /**
   * ZIP via GZIOutputStream and encode via Base64-algorithm source string.
   * @param source - String which should be zipped.
   * @return - resulted zipped and encoded String.
   * @throws IOException - if error while zipping or encoding.
   */
  public static String zip(String source)
    throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    GZIPOutputStream gzip = new GZIPOutputStream(baos);
    gzip.write(source.getBytes());
    gzip.flush();
    gzip.close();
    return Base64.encodeBase64String(baos.toByteArray());
  }

  /**
   * unZIP via GZIOutputStream and decode via Base64-algorithm source string.
   * @param zippedString - String which should be unzipped.
   * @return - resulted unzipped and decoded String.
   * @throws IOException - if error while unzipping or decoding.
   */
  public static String unzip(String zippedString)
    throws IOException {
    String result;
    byte[] decoded = Base64.decodeBase64(zippedString);
    try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(decoded))) {
      result = IOUtils.toString(gzip, StandardCharsets.UTF_8);
    }
    return result;
  }
}
