import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
{

	
	public void map(LongWritable key, Text value, Context context)throws java.io.IOException, InterruptedException
	{
		String data[]=value.toString().split(",");    
		
	
		for(String num:data)
		{
			int number=Integer.parseInt(num),mul=0;
			for(int i=2;i<(number/2)+1;i++){
			if((number%i)==0){
				mul=1;
				
				break;
				}
			}
			if (mul==0)
			{
				context.write(new Text("PRIME"), new IntWritable(number));  
			}
			else
			{
				context.write(new Text("NOT PRIME"), new IntWritable(number));
			}
			
			
					
		}
	}
}

