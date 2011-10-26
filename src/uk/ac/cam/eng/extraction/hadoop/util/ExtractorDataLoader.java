
package uk.ac.cam.eng.extraction.hadoop.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import uk.ac.cam.eng.extraction.hadoop.datatypes.TextArrayWritable;

/**
 * Load all the rule alignment data onto HDFS ready to have rules extracted
 * 
 * @author aaw35
 */
public class ExtractorDataLoader {

    private static abstract class RecordReader {

        BufferedReader in;

        StringBuilder out = new StringBuilder();

        int sentence;

        void setFileName(String fileName) throws FileNotFoundException,
                IOException {
            in = new BufferedReader(new InputStreamReader(new GZIPInputStream(
                    new FileInputStream(fileName))));
        }

        abstract String getNext() throws IOException;
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        String sentenceAlignmentFile = args[0];
        String wordAlignmentFile = args[1];
        String hdfsName = args[2];

        RecordReader sentenceReader = new RecordReader() {

            @Override
            String getNext() throws IOException {
                String line = in.readLine();
                if (line == null) {
                    return null;
                }
                sentence = Integer.parseInt(line);
                out.setLength(0);
                out.append(in.readLine()).append("\n");
                out.append(in.readLine()).append("\n");
                return out.toString();
            }
        };
        sentenceReader.setFileName(sentenceAlignmentFile);

        RecordReader alignmentReader = new RecordReader() {

            String prevLine = null;

            @Override
            String getNext() throws IOException {
                if (prevLine == null) {
                    prevLine = in.readLine();
                }

                if (!prevLine.startsWith("SENT: ")) {
                    throw new RuntimeException(
                            "Error in the word alignment file");
                }
                sentence = Integer.parseInt(prevLine.substring("SENT: "
                        .length()));
                out.setLength(0);
                String line = in.readLine();
                if (line == null) {
                    return null;
                }
                while (line != null) {
                    if (line.startsWith("S ")) {
                        out.append(line).append("\n");
                    }
                    else {
                        prevLine = line;
                        break;
                    }
                    line = in.readLine();
                }
                return out.toString();
            }

        };
        alignmentReader.setFileName(wordAlignmentFile);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(hdfsName);

        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path,
                IntWritable.class, TextArrayWritable.class);
        try {
            Text sentenceText = new Text();
            Text alignmentText = new Text();
            Text[] array = new Text[2];
            array[0] = sentenceText;
            array[1] = alignmentText;
            TextArrayWritable arrayWritable = new TextArrayWritable();
            IntWritable sentenceNumber = new IntWritable();

            String sentence = sentenceReader.getNext();
            String alignment = alignmentReader.getNext();
            while (sentence != null || alignment != null) {
                // System.out.println("Sentence #: " + alignmentReader.sentence
                // + "\n " + sentence + alignment);
                sentenceNumber.set(alignmentReader.sentence);
                sentenceText.set(sentence);
                // handle empty alignment case
                if (alignment == null) {
                    alignmentText.set("");
                }
                else {
                    alignmentText.set(alignment);
                }
                arrayWritable.set(array);
                writer.append(sentenceNumber, arrayWritable);
                sentence = sentenceReader.getNext();
                alignment = alignmentReader.getNext();
            }
        }
        finally {
            writer.close();
        }

    }

}
