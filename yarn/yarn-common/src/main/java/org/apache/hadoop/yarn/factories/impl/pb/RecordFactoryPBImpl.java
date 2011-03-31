package org.apache.hadoop.yarn.factories.impl.pb;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;

public class RecordFactoryPBImpl implements RecordFactory {

  private static final String PB_IMPL_PACKAGE_SUFFIX = "impl.pb";
  private static final String PB_IMPL_CLASS_SUFFIX = "PBImpl";

  private static final RecordFactoryPBImpl self = new RecordFactoryPBImpl();
  private Configuration localConf = new Configuration();
  private Map<Class<?>, Constructor<?>> cache = new HashMap<Class<?>, Constructor<?>>();

  private RecordFactoryPBImpl() {
  }
  
  public static RecordFactory get() {
    return self;
  }
  
  @Override
  public <T> T newRecordInstance(Class<T> clazz) throws YarnException {
    
    Constructor<?> constructor = null;
    if (cache.get(clazz) == null) {
      Class<?> pbClazz = null;
      try {
        pbClazz = localConf.getClassByName(getPBImplClassName(clazz));
      } catch (ClassNotFoundException e) {
        throw new YarnException("Failed to load class: ["
            + getPBImplClassName(clazz) + "]", e);
      }
      try {
        constructor = pbClazz.getConstructor(null);
        constructor.setAccessible(true);
        cache.put(clazz, constructor);
      } catch (NoSuchMethodException e) {
        throw new YarnException("Could not find 0 argument constructor", e);
      }
    } else {
      constructor = cache.get(clazz);
    }
    try {
      Object retObject = constructor.newInstance();
      return (T)retObject;
    } catch (InvocationTargetException e) {
      throw new YarnException(e);
    } catch (IllegalAccessException e) {
      throw new YarnException(e);
    } catch (InstantiationException e) {
      throw new YarnException(e);
    }
  }

  private String getPBImplClassName(Class<?> clazz) {
    String srcPackagePart = getPackageName(clazz);
    String srcClassName = getClassName(clazz);
    String destPackagePart = srcPackagePart + "." + PB_IMPL_PACKAGE_SUFFIX;
    String destClassPart = srcClassName + PB_IMPL_CLASS_SUFFIX;
    return destPackagePart + "." + destClassPart;
  }
  
  private String getClassName(Class<?> clazz) {
    String fqName = clazz.getName();
    return (fqName.substring(fqName.lastIndexOf(".") + 1, fqName.length()));
  }
  
  private String getPackageName(Class<?> clazz) {
    return clazz.getPackage().getName();
  }
}
