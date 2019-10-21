package org.folio.processing;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StubTest {

  @Test
  public void emptyTest() {
    Stub stub = new Stub();
    Assert.assertNotNull(stub);
  }
}
