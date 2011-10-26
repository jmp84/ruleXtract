/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.extraction;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import uk.ac.cam.eng.extraction.RuleExtractor;
import uk.ac.cam.eng.extraction.datatypes.Alignment;
import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.datatypes.SentencePair;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.TextArrayWritable;

/**
 * @author jmp84 This class is the mapper for rule extraction in the map-reduce
 *         framework
 */
// TODO replace Text by RuleWritable
public class ExtractorMapper1Method1 extends
        Mapper<IntWritable, TextArrayWritable, RuleWritable, IntWritable> {

    // private final static RuleWritable rule = new RuleWritable();
    private final static IntWritable one = new IntWritable(1);

    /**
     * This is the method that needs to completed. Ignore the key variable
     */
    @Override
    protected void map(IntWritable key, TextArrayWritable value, Context context)
            throws java.io.IOException, InterruptedException {
        // Get the associated records from the input array
        String sentenceAlign = ((Text) value.get()[0]).toString();
        String wordAlign = ((Text) value.get()[1]).toString();

        // Preprocess the lines
        SentencePair sp = new SentencePair(sentenceAlign, false);
        Alignment a = new Alignment(wordAlign, sp, false); // TODO replace 2 by
                                                           // side, get side
                                                           // from a config file

        RuleExtractor re = new RuleExtractor();

        for (Rule r: re.extract(a, sp)) {
            // replace this by a write method instead of creating an object
            RuleWritable rw = new RuleWritable(r);
            context.write(rw, one);
        }
    }

    /**
     * Main method to test mapper. Can safely ignore all code in here.
     * 
     * @param args
     * @throws InterruptedException
     * @throws IOException
     */
    public static void main(String[] args) throws IOException,
            InterruptedException {
        // Set up one aligned sentence
        final IntWritable key = new IntWritable(1);
        final TextArrayWritable value = new TextArrayWritable();
        Text[] arrayValue = new Text[2];
        value.set(arrayValue);
        /*
         * //Mytest data defined here String sentenceAlign = "5036 6 3 1709\n" +
         * "5126 143006 558 3 1391"; String wordAlign = "S 0 0\n" + "S 0 2\n" +
         * "S 1 1\n" + "S 1 3\n" + "S 2 1\n" + "S 3 4";
         */

        // String sentenceAlign =
        // "15 2623 2935 3 1709 6 3 28 50 4581 17 2874 1779 873 902 4 8 15 35 65 331 251 7 287 48 11 1303 81 182 9 3 196 10 48 3657 11 6421 18722 493 5\n"
        // +
        // "4991 7711 6 558 3 1391 143006 38 52 4 4796 6 2810 1836 3 888 307 4 11 6873 10 44 215 66 445 3 8 783 488 376 1245 4668 7";

        String sentenceAlign = "183 13 293 1084 3 2786 313 4 2202 66 1994 9 334 8 66 128 5\n"
                + "937 3 16 3047 54 14 3155 74 16 18002 20 2607 22 8 20 2607 106 5";

        // String wordAlign =
        // "S 0 0\nS 1 0\nS 2 1\nS 3 2\nS 4 3\nS 4 5\nS 5 4\nS 5 6\nS 6 6\nS 7 7\nS 8 8\nS 9 10\nS 10 11\nS 10 12\n"
        // +
        // "S 11 12\nS 12 13\nS 13 15\nS 13 16\nS 14 16\nS 15 17\nS 16 18\nS 17 23\nS 18 19\nS 19 19\nS 20 19\nS 21 19\n"
        // +
        // "S 22 20\nS 23 24\nS 24 21\nS 24 22\nS 26 1\nS 27 1\nS 28 16\nS 31 24\nS 32 26\nS 33 26\nS 33 27\nS 34 28\nS 35 29\nS 36 30\nS 37 31\nS 38 31\nS 39 32";

        String wordAlign = "S 0 0\nS 1 2\nS 2 3\nS 3 4\nS 5 6\nS 6 6\nS 7 7\nS 8 9\nS 9 10\nS 10 11\nS 10 15\nS 11 12\nS 12 12\nS 13 13\nS 14 14\nS 15 16\nS 16 17";

        arrayValue[0] = new Text(sentenceAlign);
        arrayValue[1] = new Text(wordAlign);

        ExtractorMapper1Method1 ruleMapper = new ExtractorMapper1Method1() {

            @Override
            public void run(Context context) throws IOException,
                    InterruptedException {
                map(key, value, context);
            };
        };
        Context context = ruleMapper.new Context() {

            @Override
            public void write(RuleWritable key, IntWritable value)
                    throws IOException, InterruptedException {
                System.out.printf("%s\t%d\n", key, value.get());
            }

            @Override
            public InputSplit getInputSplit() {
                // Auto-generated method stub
                return null;
            }

            @Override
            public Counter getCounter(Enum<?> counterName) {
                // Auto-generated method stub
                return null;
            }

            @Override
            public Counter getCounter(String groupName, String counterName) {
                // Auto-generated method stub
                return null;
            }

            @Override
            public IntWritable getCurrentKey() throws IOException,
                    InterruptedException {
                // Auto-generated method stub
                return null;
            }

            @Override
            public TextArrayWritable getCurrentValue() throws IOException,
                    InterruptedException {
                // Auto-generated method stub
                return null;
            }

            @Override
            public OutputCommitter getOutputCommitter() {
                // Auto-generated method stub
                return null;
            }

            @Override
            public boolean nextKeyValue() throws IOException,
                    InterruptedException {
                // Auto-generated method stub
                return false;
            }

            @Override
            public String getStatus() {
                // Auto-generated method stub
                return null;
            }

            @Override
            public TaskAttemptID getTaskAttemptID() {
                // Auto-generated method stub
                return null;
            }

            @Override
            public void setStatus(String msg) {
                // Auto-generated method stub

            }

            @Override
            public Path[] getArchiveClassPaths() {
                // Auto-generated method stub
                return null;
            }

            @Override
            public String[] getArchiveTimestamps() {
                // Auto-generated method stub
                return null;
            }

            @Override
            public URI[] getCacheArchives() throws IOException {
                // Auto-generated method stub
                return null;
            }

            @Override
            public URI[] getCacheFiles() throws IOException {
                // Auto-generated method stub
                return null;
            }

            @Override
            public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
                    throws ClassNotFoundException {
                // Auto-generated method stub
                return null;
            }

            @Override
            public Configuration getConfiguration() {
                // Auto-generated method stub
                return null;
            }

            @Override
            public Path[] getFileClassPaths() {
                // Auto-generated method stub
                return null;
            }

            @Override
            public String[] getFileTimestamps() {
                // Auto-generated method stub
                return null;
            }

            @Override
            public RawComparator<?> getGroupingComparator() {
                // Auto-generated method stub
                return null;
            }

            @Override
            public Class<? extends InputFormat<?, ?>> getInputFormatClass()
                    throws ClassNotFoundException {
                // Auto-generated method stub
                return null;
            }

            @Override
            public String getJar() {
                // Auto-generated method stub
                return null;
            }

            @Override
            public JobID getJobID() {
                // Auto-generated method stub
                return null;
            }

            @Override
            public String getJobName() {
                // Auto-generated method stub
                return null;
            }

            @Override
            public boolean getJobSetupCleanupNeeded() {
                // Auto-generated method stub
                return false;
            }

            @Override
            public Path[] getLocalCacheArchives() throws IOException {
                // Auto-generated method stub
                return null;
            }

            @Override
            public Path[] getLocalCacheFiles() throws IOException {
                // Auto-generated method stub
                return null;
            }

            @Override
            public Class<?> getMapOutputKeyClass() {
                // Auto-generated method stub
                return null;
            }

            @Override
            public Class<?> getMapOutputValueClass() {
                // Auto-generated method stub
                return null;
            }

            @Override
            public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
                    throws ClassNotFoundException {
                // Auto-generated method stub
                return null;
            }

            @Override
            public int getMaxMapAttempts() {
                // Auto-generated method stub
                return 0;
            }

            @Override
            public int getMaxReduceAttempts() {
                // Auto-generated method stub
                return 0;
            }

            @Override
            public int getNumReduceTasks() {
                // Auto-generated method stub
                return 0;
            }

            @Override
            public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
                    throws ClassNotFoundException {
                // Auto-generated method stub
                return null;
            }

            @Override
            public Class<?> getOutputKeyClass() {
                // Auto-generated method stub
                return null;
            }

            @Override
            public Class<?> getOutputValueClass() {
                // Auto-generated method stub
                return null;
            }

            @Override
            public Class<? extends Partitioner<?, ?>> getPartitionerClass()
                    throws ClassNotFoundException {
                // Auto-generated method stub
                return null;
            }

            @Override
            public boolean getProfileEnabled() {
                // Auto-generated method stub
                return false;
            }

            @Override
            public String getProfileParams() {
                // Auto-generated method stub
                return null;
            }

            @Override
            public IntegerRanges getProfileTaskRange(boolean isMap) {
                // Auto-generated method stub
                return null;
            }

            @Override
            public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
                    throws ClassNotFoundException {
                // Auto-generated method stub
                return null;
            }

            @Override
            public RawComparator<?> getSortComparator() {
                // Auto-generated method stub
                return null;
            }

            @Override
            public boolean getSymlink() {
                // Auto-generated method stub
                return false;
            }

            @Override
            public String getUser() {
                // Auto-generated method stub
                return null;
            }

            @Override
            public Path getWorkingDirectory() throws IOException {
                // Auto-generated method stub
                return null;
            }

            @Override
            public void progress() {
                // Auto-generated method stub

            }
        };
        ruleMapper.run(context);

    }

}
