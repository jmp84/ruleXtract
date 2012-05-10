/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;

import uk.ac.cam.eng.extraction.hadoop.datatypes.GeneralPairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 Set of utilities. Static methods.
 */
public class Util {

    public static byte[] object2ByteArray(Writable obj) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        obj.write(out);
        return buffer.toByteArray();
    }

    /**
     * Get the bytes between 0 and the length of a BytesWritable. Note that the
     * first byte starts at 0 because there is no offset.
     * 
     * @param bytesWritable
     * @return the array of byte from 0 to the length of bytesWritable.
     */
    public static byte[] getBytes(BytesWritable bytesWritable) {
        byte[] buffer = bytesWritable.getBytes();
        int length = bytesWritable.getLength();
        byte[] res = new byte[length];
        for (int i = 0; i < length; i++) {
            res[i] = buffer[i];
        }
        return res;
    }

    public static ArrayWritable bytes2ArrayWritable(ByteBuffer bytes) {
        DataInputBuffer in = new DataInputBuffer();
        in.reset(bytes.array(), bytes.arrayOffset(), bytes.limit());
        ArrayWritable value = new ArrayWritable(GeneralPairWritable3.class);
        try {
            value.readFields(in);
        } catch (IOException e) {
            // Byte buffer is memory backed so no exception is possible. Just in
            // case chain it to a runtime exception
            throw new RuntimeException(e);
        }
        return value;
    }

    public static RuleWritable bytes2RuleWritable(ByteBuffer bytes) {
        DataInputBuffer in = new DataInputBuffer();
        in.reset(bytes.array(), bytes.arrayOffset(), bytes.limit());
        RuleWritable value = new RuleWritable();
        try {
            value.readFields(in);
        } catch (IOException e) {
            // Byte buffer is memory backed so no exception is possible. Just in
            // case chain it to a runtime exception
            throw new RuntimeException(e);
        }
        return value;
    }
}
