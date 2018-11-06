package com.pingcap.join.meta;

import java.lang.ref.SoftReference;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * This is the base class for all value classes.
 * It provides conversion and comparison methods.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public abstract class Value<String> {

  /**
   * The data type is unknown at this time.
   */
  public static final int UNKNOWN = -1;

  /**
   * The value type for NULL.
   */
  public static final int NULL = 0;

  /**
   * The value type for BOOLEAN values.
   */
  public static final int BOOLEAN = 1;

  /**
   * The value type for BYTE values.
   */
  public static final int BYTE = 2;

  /**
   * The value type for SHORT values.
   */
  public static final int SHORT = 3;

  /**
   * The value type for INT values.
   */
  public static final int INT = 4;

  /**
   * The value type for LONG values.
   */
  public static final int LONG = 5;

  /**
   * The value type for DECIMAL values.
   */
  public static final int DECIMAL = 6;

  /**
   * The value type for DOUBLE values.
   */
  public static final int DOUBLE = 7;

  /**
   * The value type for FLOAT values.
   */
  public static final int FLOAT = 8;

  /**
   * The value type for TIME values.
   */
  public static final int TIME = 9;

  /**
   * The value type for DATE values.
   */
  public static final int DATE = 10;

  /**
   * The value type for TIMESTAMP values.
   */
  public static final int TIMESTAMP = 11;

  /**
   * The value type for BYTES values.
   */
  public static final int BYTES = 12;

  /**
   * The value type for STRING values.
   */
  public static final int STRING = 13;

  /**
   * The value type for case insensitive STRING values.
   */
  public static final int STRING_IGNORECASE = 14;

  /**
   * The value type for BLOB values.
   */
  public static final int BLOB = 15;

  /**
   * The value type for CLOB values.
   */
  public static final int CLOB = 16;

  /**
   * The value type for ARRAY values.
   */
  public static final int ARRAY = 17;

  /**
   * The value type for RESULT_SET values.
   */
  public static final int RESULT_SET = 18;
  /**
   * The value type for JAVA_OBJECT values.
   */
  public static final int JAVA_OBJECT = 19;

  /**
   * The value type for UUID values.
   */
  public static final int UUID = 20;

  /**
   * The value type for string values with a fixed size.
   */
  public static final int STRING_FIXED = 21;

  /**
   * The value type for string values with a fixed size.
   */
  public static final int GEOMETRY = 22;

  /**
   * 23 was a short-lived experiment "TIMESTAMP UTC" which has been removed.
   */

  /**
   * The value type for TIMESTAMP WITH TIME ZONE values.
   */
  public static final int TIMESTAMP_TZ = 24;

  public static final int ROW = 25;

  /**
   * The number of value types.
   */
  public static final int TYPE_COUNT = TIMESTAMP_TZ;

  private static SoftReference<Value[]> softCache =
    new SoftReference<Value[]>(null);
  private static final BigDecimal MAX_LONG_DECIMAL =
    BigDecimal.valueOf(Long.MAX_VALUE);
  private static final BigDecimal MIN_LONG_DECIMAL =
    BigDecimal.valueOf(Long.MIN_VALUE);

  /**
   * Get the SQL expression for this value.
   *
   * @return the SQL expression
   */
  public abstract String getSQL();

  /**
   * Get the value type.
   *
   * @return the type
   */
  public abstract int getType();

  /**
   * Get the precision.
   *
   * @return the precision
   */
  public abstract long getPrecision();

  /**
   * Get the display size in characters.
   *
   * @return the display size
   */
  public abstract int getDisplaySize();

  /**
   * Get the value as a string.
   *
   * @return the string
   */
  public abstract String getString();

  /**
   * Get the value as an object.
   *
   * @return the object
   */
  public abstract Object getObject();

  /**
   * Set the value as a parameter in a prepared statement.
   *
   * @param prep           the prepared statement
   * @param parameterIndex the parameter index
   */
  public abstract void set(PreparedStatement prep, int parameterIndex)
    throws SQLException;

  @Override
  public abstract int hashCode();

  /**
   * Check if the two values have the same hash code. No data conversion is
   * made; this method returns false if the other object is not of the same
   * class. For some values, compareTo may return 0 even if equals return
   * false. Example: ValueDecimal 0.0 and 0.00.
   *
   * @param other the other value
   * @return true if they are equal
   */
  @Override
  public abstract boolean equals(Object other);

  /**
   * Get the order of this value type.
   *
   * @param type the value type
   * @return the order number
   */
  static int getOrder(int type) throws Exception {
    switch (type) {
      case UNKNOWN:
        return 1;
      case NULL:
        return 2;
      case STRING:
        return 10;
      case CLOB:
        return 11;
      case STRING_FIXED:
        return 12;
      case STRING_IGNORECASE:
        return 13;
      case BOOLEAN:
        return 20;
      case BYTE:
        return 21;
      case SHORT:
        return 22;
      case INT:
        return 23;
      case LONG:
        return 24;
      case DECIMAL:
        return 25;
      case FLOAT:
        return 26;
      case DOUBLE:
        return 27;
      case TIME:
        return 30;
      case DATE:
        return 31;
      case TIMESTAMP:
        return 32;
      case TIMESTAMP_TZ:
        return 34;
      case BYTES:
        return 40;
      case BLOB:
        return 41;
      case JAVA_OBJECT:
        return 42;
      case UUID:
        return 43;
      case GEOMETRY:
        return 44;
      case ARRAY:
        return 50;
      case RESULT_SET:
        return 51;
      default:
        throw new Exception("type:" + type);
    }
  }
}
