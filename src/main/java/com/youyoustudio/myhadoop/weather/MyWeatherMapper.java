package com.youyoustudio.myhadoop.weather;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

//TextInputFormat.class(默认读取器)
public class MyWeatherMapper extends Mapper<LongWritable, Text,Weather, IntWritable> {

    Weather weather = new Weather();
    IntWritable val = new IntWritable();
    SimpleDateFormat simpleDateFormat =new SimpleDateFormat("yyyy-MM-dd");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //2019-06-03 10:55:22   30c
        try {
            String[] strs = StringUtils.split(value.toString(), "\t");

            Date date = simpleDateFormat.parse(strs[0]);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            weather.setYear(calendar.get(Calendar.YEAR));
            weather.setMonth(calendar.get(Calendar.MONTH)+1);
            weather.setDay(calendar.get(Calendar.DAY_OF_MONTH));
            int tmp = Integer.parseInt( strs[1].substring(0,strs[1].length() -1));
            weather.setWeather(tmp);
            val.set(tmp);
            context.write(weather,val);//输出
        }catch (ParseException e){
            e.printStackTrace();
        }
    }
}
