import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import opennlp.tools.stemmer.PorterStemmer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class DocFreq {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>
	{
		private Text word = new Text();
		private Text file = new Text();
		private boolean caseSensitive = false;
		private Set<String> patternsToSkip = new HashSet<String>();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			caseSensitive = conf.getBoolean("wordcount.case.sensitive", false);
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
				BufferedReader reader = new BufferedReader(new FileReader(fileName));
				String pattern = null;
				while ((pattern = reader.readLine()) != null) {
					patternsToSkip.add(pattern);
				}
				reader.close();
			} catch (IOException ioe) {
				System.err.println(
						"Caught exception while parsing the cached file '" + StringUtils.stringifyException(ioe));
			}
		}

		@Override
		public void map(Object Key, Text Value, Context context) throws IOException, InterruptedException
		{	
			String line = (caseSensitive) ? Value.toString() : Value.toString().toLowerCase();

			for (String pattern : patternsToSkip) {
				line = line.replaceAll(pattern, "");
			}
			String[] tokens = line.split("[^\\w']+");

			// initialising the stemmer to stem the words in the document.
			PorterStemmer stemmer = new PorterStemmer();
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

			for(String token : tokens)
			{
				String stem_token = stemmer.stem(token).toString();
				word.set(stem_token);
				file.set(fileName);
				context.write(word, file);
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text, Text, Text, IntWritable> {
		// stores the value of the doc frequency for the token.
		private IntWritable result = new IntWritable();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// Keeps a track of all unique words encountered till now.
			Set<Text> unique = new HashSet<Text>();

			for (Text val : values) {
				unique.add(val);
			}
			result.set(unique.size());
			context.write(key, result);
		}
	}
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "DocumentFrequency");

		// setting the Mapper class for the job
		job.setMapperClass(TokenizerMapper.class);
		// setting the Reducer class for the job
		job.setReducerClass(IntSumReducer.class);
		// Setting the datatype for the map output key, value pair
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// The output file datatype for key value
		// are Text and IntWritable
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// setting input path using arguments given while execution
		// Format: hadoop jar <jar path> className input file output file
		
		for (int i = 0; i < args.length; ++i) {
			if ("-skippatterns".equals(args[i])) {
				job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
				job.addCacheFile(new Path(args[++i]).toUri());
			} else if ("-casesensitive".equals(args[i])) {
				job.getConfiguration().setBoolean("wordcount.case.sensitive", true);
			}
		}
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// The program will run till the job is completed.
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
