package org.shirdrn.flume.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class ReflectionUtils {

	private static final ClassLoader DEFAULT_CLASSLOADER = ReflectionUtils.class.getClassLoader();

	public static Object newInstance(String className) {
		Object instance;
		try {
			Class<?> clazz = Class.forName(className, true, DEFAULT_CLASSLOADER);
			instance = clazz.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return instance;
	}


	public static <T> T newInstance(String className, Class<T> baseClass, ClassLoader classLoader) {
		return newInstance(className, baseClass, classLoader, new Object[] {});
	}

	@SuppressWarnings("unchecked")
	public static <T> T newInstance(String className, Class<T> baseClass) {
		return (T) newInstance(className);
	}
	
	public static <T> T newInstance(String className, Class<T> baseClass, Object... parameters) {
		return newInstance(className, baseClass, DEFAULT_CLASSLOADER, parameters);
	}
	
	public static <T> T newInstance(String className, Class<T> baseClass, ClassLoader classLoader, Object... parameters) {
		T instance = null;
		try {
			Class<T> clazz = newClass(className, baseClass, classLoader);
			instance = construct(clazz, parameters);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return instance;
	}
	
	@SuppressWarnings("unchecked")
	private static <T> Class<T> newClass(String className, Class<T> baseClass, ClassLoader classLoader) {
		Class<T> clazz = null;
		try {
			clazz = (Class<T>) Class.forName(className, true, DEFAULT_CLASSLOADER);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return clazz;
	}
	
	@SuppressWarnings("unchecked")
	private static <T> T construct(Class<T> clazz, Object... parameters)
			throws InstantiationException, IllegalAccessException, InvocationTargetException {
		T instance = null;
		Constructor<T>[] constructors = (Constructor<T>[]) clazz.getConstructors();
		for (Constructor<T> c : constructors) {
			if (c.getParameterTypes().length == parameters.length) {
				instance = c.newInstance(parameters);
				break;
			}
		}
		return instance;
	}
	
	
	
	
	
	
	
	
	

	public static <T> T newInstance(Class<T> clazz, Object... parameters) {
		T instance = null;
		try {
			instance = construct(clazz, parameters);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return instance;
	}

	public static <T> T newInstance(Class<T> clazz) {
		T instance = null;
		try {
			instance = clazz.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return instance;
	}




}