package org.shirdrn.flume.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimeUtils {

	public static Date getDateBefore(int unit, int amount) {
		Calendar calendar = Calendar.getInstance();
		calendar.add(unit, amount);
		return calendar.getTime();
	}
	
	public static String format(Date date, String format) {
		DateFormat df = new SimpleDateFormat(format);
		return df.format(date);
	}
	
	public static String format(String date, String srcFormat, String dstFormat) {
		DateFormat df = new SimpleDateFormat(srcFormat);
		Date d = null;
		try {
			d = df.parse(date);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		} finally {
			if(d != null) {
				df = new SimpleDateFormat(dstFormat);
			}
		}
		return df.format(d);
	}
	
	public static String format(long date, String format) {
		DateFormat df = new SimpleDateFormat(format);
		return df.format(new Date(date));
	}
	
	public static String[] between(String start, String end, String format) {
		DateFormat df = new SimpleDateFormat(format);
		int before1 = whichDayBefore(start, format);
		int before2 = whichDayBefore(end, format);
		String[] days = new String[Math.abs(before1) - Math.abs(before2) + 1];
		for(int i=before1, j=0; i<=before2; i++, j++) {
			Calendar calendar = Calendar.getInstance();
			calendar.add(Calendar.DAY_OF_MONTH, i);
			String temp = df.format(calendar.getTime());
			days[j] = temp;
		}
		return days;
	}
	
	private static int whichDayBefore(String date, String format) {
		DateFormat df = new SimpleDateFormat(format);
		int which = 0;
		while(true) {
			Calendar calendar = Calendar.getInstance();
			calendar.add(Calendar.DAY_OF_MONTH, which);
			String temp = df.format(calendar.getTime());
			if(temp.equals(date)) {
				return which;
			}
			which--;
		}
	}
}
