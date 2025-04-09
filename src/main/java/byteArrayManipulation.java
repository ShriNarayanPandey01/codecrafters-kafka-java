import java.util.ArrayList;

public class byteArrayManipulation {
    /**
     * Converts a byte array to an integer, using big-endian byte order. That is,
     * the first byte in the array is the most significant byte of the integer.
     *
     * @param b the byte array to convert
     * @return the integer value of the byte array
     */
    public static int byteArrayToInt(byte[] b) {
    
        int value  = 0;
        for(byte b1 : b) {
          value = (value << 8) + (b1 & 0xFF);
        }
        return value ;
      }

    /**
     * Converts a byte array to a short, using big-endian byte order. That is,
     * the first byte in the array is the most significant byte of the short.
     *
     * @param b the byte array to convert
     * @return the short value of the byte array
     */
      public static short byteArrayToShort(byte[] b) {
        int value  = 0;
        for(byte b1 : b) {
          value = (value << 8) + (b1 & 0xFF);
        }
        return (short)value;
      }

      /**
       * Converts an integer to a byte array, using big-endian byte order. That is,
       * the most significant byte of the integer is the first byte in the array.
       * 
       * @param data the integer to convert
       * @return the byte array representation of the integer
       */

      public static long byteArrayToLong(byte[] bytes) {
        if (bytes.length > 8) {
            throw new IllegalArgumentException("Byte array too long to convert to long");
        }

        long value = 0;
        for (int i = 0; i < bytes.length; i++) {
            value <<= 8; // shift left by 8 bits
            value |= (bytes[i] & 0xFF); // mask byte to avoid sign extension
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
    
      /**
       * Converts a short to a byte array, using big-endian byte order. That is,
       * the most significant byte of the short is the first byte in the array.
       * 
       * @param d the short to convert
       * @return the byte array representation of the short
       */
      public static byte[] shortToByteArray(short d) {
        byte[] result = new byte[2];
        result[0] = (byte) ((d >> 8) & 0xFF); 
        result[1] = (byte) (d & 0xFF);       
        return result;
    }

      /**
       * Calculates the total size in bytes of all the messages in the ArrayList
       * of byte arrays. This is useful for determining the size of the message
       * when sending it over the wire.
       * 
       * @param responses the ArrayList of byte arrays to calculate the size of
       * @return the total size in bytes of all the messages in the ArrayList
       */
      public static int sizeOfMessage(ArrayList<byte[]> responses){
        if(responses == null) return 0;
        int size = 0;
        for (byte[] response : responses) {
            size += response.length;
        }
        System.out.println(size+"--");
        return size;
      }

      public static void printByteArray(byte[] value) {
        for (byte b : value) {
          System.out.printf("%02X ", b);
      }
        System.out.println();
    }

    public static int zigZagDecode(int n) {
      return (n >>> 1) ^ -(n & 1);
  }
}
