/**
 * 
 */
package uk.ac.cam.eng.extraction.hadoop.extraction;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable2;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author juan
 *
 */
public class Target2Source2Source2TargetReducer extends
Reducer<BytesWritable, PairWritable3, BytesWritable, ArrayWritable> {

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
		ArrayWritable value = new ArrayWritable(PairWritable3.class);
		List<PairWritable3> valuesArrayList = new ArrayList<PairWritable3>();
		for (PairWritable3 targetAndProb: values) {
			PairWritable3 copy = convertValueBytes(object2ByteArray(targetAndProb));
			valuesArrayList.add(copy);
			// targetsAndProbs.put(copy.first, copy.second.get());
		}
		//Map<RuleWritable, Double> sortedMap = sortMapByValue(targetsAndProbs);
		//for (RuleWritable rule: sortedMap.keySet()) {
		//	valuesArrayList.add(new PairWritable2(rule, new DoubleWritable(
		//			sortedMap.get(rule))));
		//}
		PairWritable3[] valuesArray = valuesArrayList
				.toArray(new PairWritable3[0]);
		value.set(valuesArray);
		context.write(key, value);
	}
}
