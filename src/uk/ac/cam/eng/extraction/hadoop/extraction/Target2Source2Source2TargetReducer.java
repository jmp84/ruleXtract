/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.extraction;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3ArrayWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author juan
 */
public class Target2Source2Source2TargetReducer
        extends
        // Reducer<BytesWritable, PairWritable3, BytesWritable, ArrayWritable> {
        Reducer<BytesWritable, PairWritable3, BytesWritable, PairWritable3ArrayWritable> {

    // TODO repeated code
    private static class ValueComparator<K extends Comparable<K>, V extends Comparable<V>>
            implements Comparator<K> {

        private final Map<K, V> map;

        public ValueComparator(Map<K, V> map) {
            super();
            this.map = map;
        }

        public int compare(K key1, K key2) {
            V value1 = this.map.get(key1);
            V value2 = this.map.get(key2);
            int c = value2.compareTo(value1);
            if (c != 0) {
                return c;
            }
            return key1.compareTo(key2);
        }
    }

    private static <K extends Comparable<K>, V extends Comparable<V>> Map<K, V>
            sortMapByValue(
                    Map<K, V> unsortedMap) {
        SortedMap<K, V> sortedMap = new TreeMap<K, V>(
                new ValueComparator<K, V>(unsortedMap));
        sortedMap.putAll(unsortedMap);
        return sortedMap;
    }

    private byte[] object2ByteArray(Writable obj) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        obj.write(out);
        return buffer.toByteArray();
    }

    private static PairWritable3 convertValueBytes(byte[] bytes) {
        DataInputBuffer in = new DataInputBuffer();
        // in.reset(bytes.array(), bytes.arrayOffset(), bytes.limit());
        in.reset(bytes, bytes.length);
        PairWritable3 value = new PairWritable3();
        try {
            value.readFields(in);
        }
        catch (IOException e) {
            // Byte buffer is memory backed so no exception is possible. Just in
            // case chain it to a runtime exception
            throw new RuntimeException(e);
        }
        return value;
    }

    protected void reduce(BytesWritable key, Iterable<PairWritable3> values,
            Context context) throws java.io.IOException, InterruptedException {
        // ArrayWritable value = new ArrayWritable(PairWritable3.class);
        PairWritable3ArrayWritable value = new PairWritable3ArrayWritable();
        // put all the targets in a TreeMap for sorting. This way the target
        // ordering
        // is the same as in the source-to-target job and allows merging the
        // source-to-target
        // job and the target-to-source job.
        Map<RuleWritable, ArrayWritable> targetsAndFeatures =
                new TreeMap<RuleWritable, ArrayWritable>();
        for (PairWritable3 targetAndFeatures: values) {
            PairWritable3 copy =
                    convertValueBytes(object2ByteArray(targetAndFeatures));
            targetsAndFeatures.put(copy.first, copy.second);
        }
        PairWritable3[] valueArray =
                new PairWritable3[targetsAndFeatures.size()];
        int i = 0;
        for (RuleWritable target: targetsAndFeatures.keySet()) {
            valueArray[i] =
                    new PairWritable3(target, targetsAndFeatures.get(target));
            i++;
        }
        value.set(valueArray);
        context.write(key, value);
    }
}
