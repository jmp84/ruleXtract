/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.retrieval;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable2;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84
 */
public class ConvertToBytesReducer extends
        Reducer<BytesWritable, PairWritable2, BytesWritable, ArrayWritable> {

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
            // Integer hashCode1 = key1.hashCode();
            // Integer hashCode2 = key2.hashCode();
            // return hashCode1.compareTo(hashCode2);
            // compare the keys because in case of ties, we want to keep the
            // most frequent source rules
            return key1.compareTo(key2);
        }
    }

    private static <K extends Comparable<K>, V extends Comparable<V>> Map<K, V> sortMapByValue(
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

    private static PairWritable2 convertValueBytes(byte[] bytes) {
        DataInputBuffer in = new DataInputBuffer();
        // in.reset(bytes.array(), bytes.arrayOffset(), bytes.limit());
        in.reset(bytes, bytes.length);
        PairWritable2 value = new PairWritable2();
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

    protected void reduce(BytesWritable key, Iterable<PairWritable2> values,
            Context context) throws java.io.IOException, InterruptedException {
        ArrayWritable value = new ArrayWritable(PairWritable2.class);
        List<PairWritable2> valuesArrayList = new ArrayList<PairWritable2>();
        Map<RuleWritable, Double> targetsAndProbs = new HashMap<RuleWritable, Double>();
        // System.err.println("list target and probs: ");
        // PairWritable2 prev = null;
        for (PairWritable2 targetAndProb: values) {
            PairWritable2 copy = convertValueBytes(object2ByteArray(targetAndProb));
            // valuesArrayList.add(copy);
            targetsAndProbs.put(copy.first, copy.second.get());
            // System.err.println(targetAndProb.first + " " +
            // targetAndProb.second + " " + (prev == targetAndProb));
            // prev = targetAndProb;
        }
        Map<RuleWritable, Double> sortedMap = sortMapByValue(targetsAndProbs);
        for (RuleWritable rule: sortedMap.keySet()) {
            valuesArrayList.add(new PairWritable2(rule, new DoubleWritable(
                    sortedMap.get(rule))));
        }
        PairWritable2[] valuesArray = valuesArrayList
                .toArray(new PairWritable2[0]);
        value.set(valuesArray);
        context.write(key, value);
    }
}
