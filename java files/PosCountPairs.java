import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//Code adapted from https://stackoverflow.com/questions/5836148/how-to-use-opennlp-with-java

import java.io.File;
import opennlp.tools.cmdline.postag.POSModelLoader;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSSample;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.stemmer.PorterStemmer;


public class PosCountPairs {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			
			POSModel model = new POSModelLoader().load(new File("/home/chinmay/chin/semester6/NoSQL/final/opennlp-en-ud-ewt-pos-1.0-1.9.3.bin")); //Edit path to the pre-trained model file
			POSTaggerME tagger = new POSTaggerME(model);
			String line = value.toString();

			if (line != null) {
				SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;
		    	String tokenizedLine[] = tokenizer.tokenize(line); //Tokenize line
		    	String[] tags = tagger.tag(tokenizedLine); //Instanciate tags
	
				//POS Tag
		    	POSSample sample = new POSSample(tokenizedLine, tags); //Identify tags
				for(String token : sample.getTags()){
					word.set(token);
					context.write(word, one);
			  	}
				
				
			}
			
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "poscount");

		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class); // enable to use 'local aggregation'
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}