package org.folio.processing.value;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static java.util.Arrays.asList;

public class DateValue implements Value<List<Date>> {

  private static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd";

  private final List<Date> dateRange;

  protected DateValue(List<Date> dateRange) {
    this.dateRange = dateRange;
  }

  public static DateValue of(Date from, Date till) {
    return new DateValue(asList(from, till));
  }

  @Override
  public List<Date> getValue() {
    return dateRange;
  }

  @Override
  public ValueType getType() {
    return ValueType.DATE;
  }

  public String getFromDate() {
    return new SimpleDateFormat(DATE_FORMAT_PATTERN).format(dateRange.get(0));
  }

  public String getToDate() {
    return new SimpleDateFormat(DATE_FORMAT_PATTERN).format(dateRange.get(1));
  }

}
