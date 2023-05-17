import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import opennlp.tools.cmdline.postag.POSModelLoader;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSSample;
import opennlp.tools.postag.POSTaggerME;
//import opennlp.tools.stemmer.PorterStemmer;
import opennlp.tools.tokenize.SimpleTokenizer;


public class PosCountStripes
{
    public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {

        //private final static IntWritable one = new IntWritable(1);
        private Text word = new Text("Answer");
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            /* stemming words */
        	
            MapWritable myMap = new MapWritable();

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
                    if(myMap.containsKey(new Text(token))){
                        IntWritable temp = (IntWritable)myMap.get(new Text(token));
                        int x = temp.get();
                        x++;
                        temp.set(x);
                        myMap.put(new Text(token), temp);
                    } else {
                        myMap.put(new Text(token), new IntWritable(1));
                    }
//                    word.set(token);
					
					
			  	}
                context.write(word, myMap);

				
				
			}

    
        }
    }

    public static class Reduce extends Reducer<Text, MapWritable, Text, MapWritable> {

        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {
            MapWritable ret = new MapWritable();

            for (MapWritable value : values){

                for(MapWritable.Entry<Writable, Writable> e : value.entrySet()){
                    if (ret.containsKey(e.getKey())) {
                        int i = ((IntWritable)e.getValue()).get();
                        int j = ((IntWritable)ret.get(e.getKey())).get();
                        ret.put(e.getKey(), new IntWritable(i+j));
                    } else {
                        ret.put(e.getKey(), e.getValue());
                    }
                }

            }

            context.write(key, ret);
        }
    }

 
    public static void main(String[] args) throws Exception {
    	
    	if (args.length < 2) {
            System.err.println("Must pass InputPath and OutputPath.");
            System.exit(1);
        }
    	
        Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Stripes");
        job.setJarByClass(posCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        job.setMapperClass(Map.class);
        //job.setCombinerClass(Reduce.class); // enable this to use 'local aggregation'
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}