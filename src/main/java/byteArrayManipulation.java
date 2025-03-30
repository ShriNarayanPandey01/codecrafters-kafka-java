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
      public static int sizeOfMessage(ArrayList<byte[]> responses){
        int size = 0;
        for (byte[] response : responses) {
            size += response.length;
        }
        return size;
      }
}
