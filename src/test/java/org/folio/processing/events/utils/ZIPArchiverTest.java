package org.folio.processing.events.utils;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class ZIPArchiverTest {

  private static final String SOURCE = "{\\\"leader\\\":\\\"01240cas a2200397   4500\\\",\\\"fields\\\":[{\\\"001\\\":\\\"366832\\\"},{\\\"005\\\":\\\"20141106221425.0\\\"},{\\\"008\\\":\\\"750907c19509999enkqr p       0   a0eng d\\\"},{\\\"010\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"   58020553 \\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"022\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"0022-0469\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"035\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"(CStRLIN)NYCX1604275S\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"035\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"(NIC)notisABP6388\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"035\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"366832\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"035\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"(OCoLC)1604275\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"040\\\":{\\\"subfields\\\":[{\\\"d\\\":\\\"CtY\\\"},{\\\"d\\\":\\\"MBTI\\\"},{\\\"d\\\":\\\"CtY\\\"},{\\\"d\\\":\\\"MBTI\\\"},{\\\"d\\\":\\\"NIC\\\"},{\\\"d\\\":\\\"CStRLIN\\\"},{\\\"d\\\":\\\"NIC\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"050\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"BR140\\\"},{\\\"b\\\":\\\".J6\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\" \\\"}},{\\\"082\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"270.05\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"222\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"The Journal of ecclesiastical history\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"4\\\"}},{\\\"245\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"The Journal of ecclesiastical history.\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"4\\\"}},{\\\"260\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"London,\\\"},{\\\"b\\\":\\\"Cambridge University Press [etc.]\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"265\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"32 East 57th St., New York, 10022\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"300\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"v.\\\"},{\\\"b\\\":\\\"25 cm.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"310\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Quarterly,\\\"},{\\\"b\\\":\\\"1970-\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"321\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Semiannual,\\\"},{\\\"b\\\":\\\"1950-69\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"362\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"v. 1-   Apr. 1950-\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\" \\\"}},{\\\"570\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Editor:   C. W. Dugmore.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"650\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Church history\\\"},{\\\"x\\\":\\\"Periodicals.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"650\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Church history\\\"},{\\\"2\\\":\\\"fast\\\"},{\\\"0\\\":\\\"(OCoLC)fst00860740\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\"7\\\"}},{\\\"655\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Periodicals\\\"},{\\\"2\\\":\\\"fast\\\"},{\\\"0\\\":\\\"(OCoLC)fst01411641\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\"7\\\"}},{\\\"700\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Dugmore, C. W.\\\"},{\\\"q\\\":\\\"(Clifford William),\\\"},{\\\"e\\\":\\\"ed.\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\" \\\"}},{\\\"853\\\":{\\\"subfields\\\":[{\\\"8\\\":\\\"1\\\"},{\\\"a\\\":\\\"v.\\\"},{\\\"i\\\":\\\"(year)\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"3\\\"}},{\\\"863\\\":{\\\"subfields\\\":[{\\\"8\\\":\\\"1\\\"},{\\\"a\\\":\\\"1-49\\\"},{\\\"i\\\":\\\"1950-1998\\\"}],\\\"ind1\\\":\\\"4\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"902\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"pfnd\\\"},{\\\"b\\\":\\\"Lintz\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"905\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"19890510120000.0\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"948\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"20141106\\\"},{\\\"b\\\":\\\"m\\\"},{\\\"d\\\":\\\"batch\\\"},{\\\"e\\\":\\\"lts\\\"},{\\\"x\\\":\\\"addfast\\\"}],\\\"ind1\\\":\\\"2\\\",\\\"ind2\\\":\\\" \\\"}},{\\\"950\\\":{\\\"subfields\\\":[{\\\"l\\\":\\\"OLIN\\\"},{\\\"a\\\":\\\"BR140\\\"},{\\\"b\\\":\\\".J86\\\"},{\\\"h\\\":\\\"01/01/01 N\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"999\\\":{\\\"ind1\\\":\\\"f\\\",\\\"ind2\\\":\\\"f\\\",\\\"subfields\\\":[{\\\"s\\\":\\\"b90cb1bc-601f-45d7-b99e-b11efd281dcd\\\"}]}}]}\"\n";

  @Test
  public void shouldArchiveString() throws IOException {
    String zippedString = ZIPArchiver.zip(SOURCE);
    assertNotEquals(SOURCE, zippedString);
    assertThat(SOURCE.length(), greaterThan(zippedString.length()));
  }

  @Test
  public void shouldUnzipString() throws IOException {
    String zippedString = "H4sIAAAAAAAAAK1WTVPbMBC991docmpmYnclW/7gBi4HGBoooUMZwsGxZKLBsUF2aFOG/15FTgIOWHGneJyJJa3ePmnfSvs07mU8ZlyOe3vjHmDiQhKXKCYEwAl9hJBLAca9wbiXCp6xUtldPylLwHqG43mBQ8a954HupLqTAHYxBo8Q7BJqw2Y40MM+hRD8BIfqXz08v3uQ6B7VD6hfDDy/RWw9DSuAPfVRzicNDrFGU/Y0AAKUOkjNuFFMRc5qdkgTV02ybj7XkISYIEGNW+B6YWc8h5rwPkej6vzkaNgfXkU/sQcu8enoo7CHR1E/LypR7h+ceU4QfBDuJrAfQvI0Kk6i/mrpnUHdlsAzbRdVVyuF1O1vBxdHjY6dBmrrmhPqML1j040vNQr14By761SY6B772NvGhjbswKhY4oMNnTeWmOV/MeXouJjLPM5QkSKeJBkvRVxWIlE9U1FWhVzsIO5ufLlGZXTyZXd25hkjcFLkrMgHjRhE8WwiBbvl6EcuHrksRbVAZ5KXJbrmVWLfdN5Uz5xPBB2qZSHqV1M0quwBGvJf6KqQdwOEQR9I3fw4YFzjo91YHqEomb3Zv1Zs81H7fR7Lists0dxCHPpgdXZBsMnFiM9EnOfzONv2QcHqfiA7nlHhjzbClro49u+l+lpCd81D6ht36JAJJdg9BR3Z6NJGX+e3s0LyzgHwzEdINJ3LZPoqBZdzfuuhMy5FwZZZU+7yBv/jrYZIlZTX1/PrEz4tK3XLe+Drk85Ewn8hYcybV+vqzGBZfXgu7srAN2fUKoaDOqQrnw+riz0TaVpIhi5Flol41l+rlutxzt7EArdEPqDO+ySC9TRttpXkoqax4LHs75Cws/Hk/YMnbLlhw5fOFhyGb+oMt0VkIRhT8T7NWSPTT0Re/emaLyEYxYPDQFlgVdaCeuxdmnyBdQPjbbuqbxu8Z42yYRJXybShhawqGwkbM7bScIMSaaPUlquZtjt9qVvaq41gzXhahwh/0S8adt6XMKxJbCzTpmXd3KZY1lsSQjLBk8TyAKeWS5lvTVT1b00w5ikjAWbJUgk3z+rtffoLAv3RvZkMAAA=";
    String unzippedString = ZIPArchiver.unzip(zippedString);
    assertEquals(SOURCE, unzippedString);
  }

  @Test
  public void shouldArchiveAndUnarchiveString() throws IOException {
    String zippedString = ZIPArchiver.zip(SOURCE);
    String unzippedString = ZIPArchiver.unzip(zippedString);
    assertNotEquals(SOURCE, zippedString);
    assertThat(SOURCE.length(), greaterThan(zippedString.length()));
    assertEquals(SOURCE.length(), unzippedString.length());
    assertEquals(SOURCE, unzippedString);
  }
}
