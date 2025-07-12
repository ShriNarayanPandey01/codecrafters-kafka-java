import java.util.ArrayList;

public class byteArrayManipulation {
  public static int byteArrayToInt(byte[] b) {

    int value = 0;
    for (byte b1 : b) {
      value = (value << 8) + (b1 & 0xFF);
    }
    return value;
  }

  public static short byteArrayToShort(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return 0;
    }
    if (bytes.length > 2) {
      throw new IllegalArgumentException("Byte array too long for short conversion (max 2 bytes)");
    }

    int value = 0;
    for (int i = 0; i < bytes.length; i++) {
      value = (value << 8) + (bytes[i] & 0xFF);
    }
    return (short) value;
  }

  public static long byteArrayToLong(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return 0L;
    }
    if (bytes.length > 8) {
      throw new IllegalArgumentException("Byte array too long for long conversion (max 8 bytes)");
    }

    long value = 0L;
    for (int i = 0; i < bytes.length; i++) {
      value = (value << 8) | (bytes[i] & 0xFF);
    }
    return value;
  }

  public static byte[] intToByteArray(int data) {
    byte[] result = new byte[4];
    result[0] = (byte) ((data >> 24) & 0xFF);
    result[1] = (byte) ((data >> 16) & 0xFF);
    result[2] = (byte) ((data >> 8) & 0xFF);
    result[3] = (byte) (data & 0xFF);
    return result;
  }

  public static byte[] shortToByteArray(short d) {
    byte[] result = new byte[2];
    result[0] = (byte) ((d >> 8) & 0xFF);
    result[1] = (byte) (d & 0xFF);
    return result;
  }

  public static int sizeOfMessage(ArrayList<byte[]> responses) {
    if (responses.size() == 0)
      return 0;
    int size = 0;
    for (byte[] response : responses) {
      size += response.length;
    }
    return size;
  }

  public static void printByteArray(byte[] value, String description) {
    if (value == null) {
      System.out.println("====== " + description + " ===== null");
      return;
    }

    System.out.println("====== " + description + " =====");

    if (value.length == 0) {
      System.out.println("(empty array)");
      return;
    }

    // Use StringBuilder for efficient string building
    StringBuilder sb = new StringBuilder(value.length * 3);
    for (byte b : value) {
      sb.append(String.format("%02X ", b));
    }
    System.out.println(sb.toString());
  }

  public static int zigZagDecode(int n) {
    return (n >>> 1) ^ -(n & 1);
  }

  public static int zigZagEncode(int value) {
    return (value << 1) ^ (value >> 31);
  }

  public static String byteArrayToString(byte[] bytes) {
    if (bytes == null) {
      return "null";
    }
    if (bytes.length == 0) {
      return "";
    }

    // Pre-allocate StringBuilder with appropriate capacity
    StringBuilder sb = new StringBuilder(bytes.length * 4); // Estimate 4 chars per byte
    for (byte b : bytes) {
      sb.append(b);
    }
    return sb.toString();
  }

  public static String byteArrayToHexString(byte[] bytes) {
    if (bytes == null) {
      return "null";
    }
    if (bytes.length == 0) {
      return "";
    }

    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  public static int safeLength(byte[] array) {
    return array == null ? 0 : array.length;
  }

  public static boolean arrayEquals(byte[] a, byte[] b) {
    if (a == b)
      return true;
    if (a == null || b == null)
      return false;
    if (a.length != b.length)
      return false;

    for (int i = 0; i < a.length; i++) {
      if (a[i] != b[i])
        return false;
    }
    return true;
  }
}
