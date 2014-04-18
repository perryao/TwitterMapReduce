
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
//import org.json.simple.parser.ParseException;

import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.util.Date;

public class OnoAverageMapper  extends Mapper<LongWritable, Text, Text, LongWritable> 
{
	public static boolean isHourInInterval(String target, String start, String end) {
        return ((target.compareTo(start) >= 0)&& (target.compareTo(end) <= 0));
    }
    //@Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {
        String line = value.toString();
		JSONObject jsonObject;
        JSONObject jsonObjectUser;
		JSONParser parser = new JSONParser();

		int text_length=0;	
		long userid = 0;
		String createdAt = "";

		if(line.length()>10)
		{
			try
			{
				String text="";		
				jsonObject = (JSONObject)parser.parse(line);
				if(jsonObject.containsKey("text"))
				{
					text = (String)jsonObject.get("text");
					text_length = text.length();
					if(jsonObject.containsKey("user"))
					{
						jsonObjectUser = (JSONObject)jsonObject.get("user");
						userid = (long)jsonObjectUser.get("id");
						createdAt = (String)jsonObjectUser.get("created_at");
					}
				}
			}
			catch(Exception e)
			{
				return;
			}

			if(userid==211178363)
            {
            	context.write(new Text("PrezOno"), new LongWritable(text_length));
				context.write(new Text("PrezOnoCount"), new LongWritable(1));
				if(isHourInInterval(createdAt.substring(11,16),"00:00","01:00")) {
					context.write(new Text("Between midnight and 1am"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"02:00","02:00")) {
					context.write(new Text("Between 1am and 2am"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"02:00","03:00")) {
					context.write(new Text("Between 2am and 3am"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"03:00","04:00")) {
					context.write(new Text("Between 3am and 4am"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"42:00","05:00")) {
					context.write(new Text("Between 4am and 5am"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"05:00","06:00")) {
					context.write(new Text("Between 5am and 6am"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"06:00","07:00")) {
					context.write(new Text("Between 6am and 7am"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"07:00","08:00")) {
					context.write(new Text("Between 7am and 8am"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"08:00","08:00")) {
					context.write(new Text("Between 8am and 9am"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"09:00","10:00")) {
					context.write(new Text("Between 9am and 10am"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"10:00","11:00")) {
					context.write(new Text("Between 10am and 11am"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"11:00","12:00")) {
					context.write(new Text("Between 11am and 12pm"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"12:00","13:00")) {
					context.write(new Text("Between 12pm and 1pm"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"13:00","14:00")) {
					context.write(new Text("Between 1pm and 2pm"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"14:00","15:00")) {
					context.write(new Text("Between 2pm and 3pm"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"15:00","16:00")) {
					context.write(new Text("Between 3pm and 4pm"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"16:00","17:00")) {
					context.write(new Text("Between 4pm and 5pm"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"17:00","18:00")) {
					context.write(new Text("Between 5pm and 6pm"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"18:00","19:00")) {
					context.write(new Text("Between 6pm and 7pm"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"19:00","20:00")) {
					context.write(new Text("Between 7pm and 8pm"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"20:00","21:00")) {
					context.write(new Text("Between 8pm and 9pm"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"21:00","22:00")) {
					context.write(new Text("Between 9pm and 10pm"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"22:00","23:00")) {
					context.write(new Text("Between 10pm and 11pm"), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"23:00","24:00")) {
					context.write(new Text("Between 11pm and 12am"), new LongWritable(1));
				}
				
            }
			
			
			
			
			

		}

    }
}