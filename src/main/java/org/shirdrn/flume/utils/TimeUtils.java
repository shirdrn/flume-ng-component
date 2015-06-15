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
	
	@SuppressWarnings("deprecation")
	public static long toNextDay(long timestamp) {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DATE, 1);
		Date d = calendar.getTime();
		d.setHours(0);
		d.setMinutes(0);
		d.setSeconds(0);
		return d.getTime() - timestamp;
	}
	
	@SuppressWarnings("deprecation")
	public static long toNextHour(long timestamp) {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.HOUR_OF_DAY, 1);
		Date d = calendar.getTime();
		d.setMinutes(0);
		d.setSeconds(0);
		return d.getTime() - timestamp;
	}
	
	@SuppressWarnings("deprecation")
	public static long toNextMinute(long timestamp) {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.MINUTE, 1);
		Date d = calendar.getTime();
		d.setSeconds(0);
		return d.getTime() - timestamp;
	}
	
	public static void main(String[] args) {
		System.out.println(toNextDay(System.currentTimeMillis()));
		System.out.println(toNextHour(System.currentTimeMillis()));
		System.out.println(toNextMinute(System.currentTimeMillis()));
	}
}
