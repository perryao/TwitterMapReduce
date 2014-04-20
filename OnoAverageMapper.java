
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
	//this is the direction I need to head to get rid of the awful if-elseif below but it currently is ignoring 1am-11am for some reason. 
	public static String timeRange(String timeString,String day) {
		int hour;
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
				context.write(new Text(String.format("#%s", dayOfWeek)), new LongWritable(1));
				if(isHourInInterval(createdAt.substring(11,16),"00:00","01:00")) {
					context.write(new Text(String.format("%s 12am-1am", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"02:00","02:00")) {
					context.write(new Text(String.format("%s 1am-2am", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"02:00","03:00")) {
					context.write(new Text(String.format("%s 2am-3am", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"03:00","04:00")) {
					context.write(new Text(String.format("%s 3am-4am", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"42:00","05:00")) {
					context.write(new Text(String.format("%s 4am-5am", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"05:00","06:00")) {
					context.write(new Text(String.format("%s 5am-6am", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"06:00","07:00")) {
					context.write(new Text(String.format("%s 6am-7am", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"07:00","08:00")) {
					context.write(new Text(String.format("%s 7am-8am", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"08:00","08:00")) {
					context.write(new Text(String.format("%s 8am-9am", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"09:00","10:00")) {
					context.write(new Text(String.format("%s 9am-10am", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"10:00","11:00")) {
					context.write(new Text(String.format("%s 10am-11am", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"11:00","12:00")) {
					context.write(new Text(String.format("%s 11am-12pm", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"12:00","13:00")) {
					context.write(new Text(String.format("%s 12pm-1pm", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"13:00","14:00")) {
					context.write(new Text(String.format("%s 1pm-2pm", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"14:00","15:00")) {
					context.write(new Text(String.format("%s 2pm-3pm", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"15:00","16:00")) {
					context.write(new Text(String.format("%s 3pm-4pm", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"16:00","17:00")) {
					context.write(new Text(String.format("%s 4pm-5pm", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"17:00","18:00")) {
					context.write(new Text(String.format("%s 5pm-6pm", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"18:00","19:00")) {
					context.write(new Text(String.format("%s 6pm-7pm", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"19:00","20:00")) {
					context.write(new Text(String.format("%s 7pm-8pm", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"20:00","21:00")) {
					context.write(new Text(String.format("%s 8pm-9pm", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"21:00","22:00")) {
					context.write(new Text(String.format("%s 9pm-10pm", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"22:00","23:00")) {
					context.write(new Text(String.format("%s 10pm-11pm", dayOfWeek)), new LongWritable(1));
				}else if(isHourInInterval(createdAt.substring(11,16),"23:00","24:00")) {
					context.write(new Text(String.format("%s 11pm-12am", dayOfWeek)), new LongWritable(1));
				}

			}

		}

	}
}