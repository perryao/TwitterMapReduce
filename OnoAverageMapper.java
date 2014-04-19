
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


public class OnoAverageMapper  extends Mapper<LongWritable, Text, Text, LongWritable> 
{
	public static boolean isHourInInterval(String target, String start, String end) {
		return ((target.compareTo(start) >= 0)&& (target.compareTo(end) <= 0));
	}

	public static String fullDayFromAbbreviation(String abbrev) {
		if(abbrev.equalsIgnoreCase("Mon")) {
			return "Monday   ";
		}else if(abbrev.equalsIgnoreCase("Tue")) {
			return "Tuesday  ";
		}else if(abbrev.equalsIgnoreCase("Wed")) {
			return "Wednesday";
		}else if(abbrev.equalsIgnoreCase("Thu")) {
			return "Thursday ";
		}else if(abbrev.equalsIgnoreCase("Fri")) {
			return "Friday   ";
		}else if(abbrev.equalsIgnoreCase("Sat")) {
			return "Saturday ";
		}else if(abbrev.equalsIgnoreCase("Sun")) {
			return "Sunday   ";
		}else {
			return "Day not found";
		}
	}
	
	//context.write(new Text(String.format("%s 12am-1am", dayOfWeek)), new LongWritable(1));
	public static String timeRange(String timeString,String day) {
		int hour;
		int min;
		if(timeString.substring(0).equalsIgnoreCase("0") && !timeString.substring(1).equalsIgnoreCase("0")) {
			//Is it 1am-9am?
			hour = Integer.parseInt(timeString.substring(1));
			return String.format("%s %dam-%dam", day,hour,hour+1);
			
		}else if(timeString.substring(0).equalsIgnoreCase("1") && (timeString.substring(1).equalsIgnoreCase("0") || timeString.substring(1).equalsIgnoreCase("1"))) {
			//Hour should be either 10 or 11
			hour = Integer.parseInt(timeString.substring(0,2));
			if(hour == 10) {
				return String.format("%s %dam-%dam", day,hour,hour+1);
			}else if(hour == 11) {
				return String.format("%s %dam-%dpm", day,hour,hour+1);
			}
		}else {
			//hour should be between 12 and 23(noon and 11pm)
			hour = Integer.parseInt(timeString.substring(0,2));
			if(hour == 23) {
				return String.format("%s %dpm-12am", day,hour-12);
			}else {
				//Now hour can only be between 12 and 22(noon and 10pm)
				if(hour == 12) {
					return String.format("%s %dpm-1pm", day,hour);
				}else if(hour >=13 && hour <= 22) {
					//Now hour can only be between 13 and 22(1pm and 10pm)
					
					return String.format("%s %dpm-%dpm", day,(hour-12),(hour-11));
					
				}
			}
			
		}
		
		//It must be between midnight and 1am
		return String.format("%s 12am-1am", day);
		
		
	}
	//@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		String line = value.toString();
		JSONObject jsonObject;
		JSONObject jsonObjectUser;
		JSONParser parser = new JSONParser();

		long userid = 0;
		String createdAt = "";

		if(line.length()>10)
		{
			try
			{
				jsonObject = (JSONObject)parser.parse(line);
				if(jsonObject.containsKey("user"))
				{
					jsonObjectUser = (JSONObject)jsonObject.get("user");
					userid = (long)jsonObjectUser.get("id");
					createdAt = (String)jsonObject.get("created_at");
				}
			}
			catch(Exception e)
			{
				return;
			}

			if(userid==211178363)
			{
				
				String dayOfWeek = fullDayFromAbbreviation(createdAt.substring(0,3));
				String time = createdAt.substring(11,16);
				context.write(new Text(timeRange(time,dayOfWeek)), new LongWritable(1));
				context.write(new Text(String.format("Number of %s", dayOfWeek)), new LongWritable(1));

			}

		}

	}
}