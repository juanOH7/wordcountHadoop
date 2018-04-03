import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Scanner;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class WordCount2 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    static enum CountersEnum { INPUT_WORDS }

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    private boolean caseSensitive;
    private Set<String> patternsToSkip = new HashSet<String>();

    private Configuration conf;
    private BufferedReader fis;

    @Override
    public void setup(Context context) throws IOException,
        InterruptedException {
      conf = context.getConfiguration();
      caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
      if (conf.getBoolean("wordcount.skip.patterns", false)) {
        URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
        for (URI patternsURI : patternsURIs) {
          Path patternsPath = new Path(patternsURI.getPath());
          String patternsFileName = patternsPath.getName().toString();
          parseSkipFile(patternsFileName);
        }
      }
    }

    private void parseSkipFile(String fileName) {
      try {
        fis = new BufferedReader(new FileReader(fileName));
        String pattern = null;
        while ((pattern = fis.readLine()) != null) {
          patternsToSkip.add(pattern);
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file '"
            + StringUtils.stringifyException(ioe));
      }
    }

    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = (caseSensitive) ?
          value.toString() : value.toString().toLowerCase();
      for (String pattern : patternsToSkip) {
        line = line.replaceAll(pattern, "");
      }
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
	if(itr.hasMoreTokens()){
		word.set(itr.nextToken());
	}
        context.write(word, one);
        Counter counter = context.getCounter(CountersEnum.class.getName(),
            CountersEnum.INPUT_WORDS.toString());
        counter.increment(1);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
       String filePath = "src/abcnews-date-text.csv";
       String filePath2 = "src/all2.csv";

       String stopPath = "src/stopwords.txt";
       String outputPath = "input/input.txt";
       File stopword = new File(stopPath);
       File mainFile = new File(filePath);
       File mainFile2 = new File(filePath2);

       BufferedWriter bw2 = null;
       FileWriter fw2 = null;
       BufferedReader br = null;
       BufferedReader br2 = null;
       ArrayList<String> stopwords = new ArrayList();
       PrintWriter writer2 = null;
       try {
           writer2 = new PrintWriter(outputPath, "UTF-8");
           fw2 = new FileWriter(outputPath, true);
           bw2 = new BufferedWriter(fw2);

           Scanner inputStreamStop = new Scanner(stopword).useDelimiter("\\s*\n\\s*");

           while (inputStreamStop.hasNext()) {
               stopwords.add(inputStreamStop.next());
           }
           br = new BufferedReader(new FileReader(mainFile));
           String next = br.readLine();
           while (next != null && !next.isEmpty()) {
               next = br.readLine();
               next = next.toLowerCase();
               for (int i = 0; i < stopwords.size(); i++) {
                   next = next.replaceAll("\\b" + stopwords.get(i) + "\\b", "");
               }
               next = next.replaceAll("[^a-z-A-Z ]", "");
               bw2.write(next + "\n");
           }
           System.out.println("All2");
           br2 = new BufferedReader(new FileReader(mainFile2));
           String next2 = br2.readLine();
           while (next2 != null && !next2.isEmpty()) {
               next2 = br.readLine();
               next2 = next2.toLowerCase();
               for (int i = 0; i < stopwords.size(); i++) {
                   next2 = next2.replaceAll("\\b" + stopwords.get(i) + "\\b", "");
               }
               next2 = next2.replaceAll("[^a-z-A-Z ]", "");
               bw2.write(next2 + "\n");
           }

       } catch (FileNotFoundException ex) {
           System.err.println(ex);
       } catch (IOException exx) {
           System.err.println(exx);
       } finally {
           try {
               if (br != null) {
                   br.close();
               }
               if (br2 != null) {
                   br2.close();
               }
               if (bw2 != null) {
                   bw2.close();
               }
               if (fw2 != null) {
                   fw2.close();
               }
               if (writer2 != null) {
                   writer2.close();
               }
           } catch (IOException ex) {
               System.err.println(ex);
           }
       }
    Configuration conf = new Configuration();
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
      System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount2.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    List<String> otherArgs = new ArrayList<String>();
    for (int i=0; i < remainingArgs.length; ++i) {
      if ("-skip".equals(remainingArgs[i])) {
        job.addCacheFile(new Path(remainingArgs[++i]).toUri());
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
      } else {
        otherArgs.add(remainingArgs[i]);
      }
    }
    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
