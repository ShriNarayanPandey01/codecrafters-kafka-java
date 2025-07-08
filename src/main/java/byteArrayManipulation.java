import java.util.ArrayList;

public class byteArrayManipulation {
    public static int byteArrayToInt(byte[] b) {
    
        int value  = 0;
        for(byte b1 : b) {
          value = (value << 8) + (b1 & 0xFF);
        }
        return value ;
      }
      public static short byteArrayToShort(byte[] b) {
        int value  = 0;
        for(byte b1 : b) {
          value = (value << 8) + (b1 & 0xFF);
        }
        return (short)value;
      }
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

      public static byte[] shortToByteArray(short d) {
        byte[] result = new byte[2];
        result[0] = (byte) ((d >> 8) & 0xFF); 
        result[1] = (byte) (d & 0xFF);       
        return result;
    }
      public static int sizeOfMessage(ArrayList<byte[]> responses){
        if(responses.size() == 0) return 0;
        int size = 0;
        for (byte[] response : responses) {
            size += response.length;
        }
        System.out.println(size+"--");
        return size;
      }

      public static void printByteArray(byte[] value , String s) {
        System.out.println("====== "+s+" =====" );
        for (byte b : value) {
          System.out.printf("%02X ", b);
      }
        System.out.println();
    }

    public static int zigZagDecode(int n) {
      return (n >>> 1) ^ -(n & 1);
    }

    public static String byteArrayToString(byte[] a){
      String s = "";
      for(byte i : a)
        s += ""+i;
      return s;
    }
}
