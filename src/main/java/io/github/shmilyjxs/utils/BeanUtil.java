package io.github.shmilyjxs.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.lang3.StringUtils;
import org.springframework.cglib.beans.BeanMap;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

public class BeanUtil {

    public static <T> Map<String, Object> beanToSortedMap(T bean) {
        return Optional.ofNullable(bean).map(e -> {
            Map<String, Object> map = new HashMap<>();
            BeanMap.create(e).forEach((key, value) -> map.put((String) key, value));

            Map<String, Object> sortedMap = new LinkedHashMap<>();
            List<String> nameList = Arrays.stream(e.getClass().getDeclaredFields()).map(Field::getName).collect(Collectors.toList());
            map.entrySet().stream().sorted(Comparator.comparingInt(o -> nameList.indexOf(o.getKey()))).forEach(entry -> sortedMap.put(entry.getKey(), entry.getValue()));
            return sortedMap;
        }).orElse(null);
    }

    public static <T> Map<String, Object> beanToMap(T bean) {
        return Optional.ofNullable(bean).map(e -> {
            Map<String, Object> map = new HashMap<>();
            BeanMap.create(e).forEach((key, value) -> map.put((String) key, value));
            return map;
        }).orElse(null);
    }

    public static <T> T mapToBean(Map<String, Object> map, Class<T> type) {
        if (Objects.isNull(map)) {
            return null;
        }
        try {
            T bean = type.newInstance();
            BeanMap.create(bean).putAll(map);
            return bean;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Object> dbToJava(Map<String, Object> dbMap) {
        return Optional.ofNullable(dbMap).map(e -> {
            Map<String, Object> javaMap = new LinkedHashMap<>();
            e.forEach((key, val) -> javaMap.put(dbToJava(key), val));
            return javaMap;
        }).orElse(null);
    }

    public static <T> T dbToJava(Map<String, Object> dbMap, Class<T> type) {
        return Optional.ofNullable(dbMap).map(BeanUtil::dbToJava).map(e -> mapToBean(e, type)).orElse(null);
    }

    public static Map<String, Object> javaToDb(Map<String, Object> javaMap) {
        return Optional.ofNullable(javaMap).map(e -> {
            Map<String, Object> dbMap = new LinkedHashMap<>();
            e.forEach((key, val) -> dbMap.put(javaToDb(key), val));
            return dbMap;
        }).orElse(null);
    }

    public static <T> Map<String, Object> javaToDb(T bean) {
        return Optional.ofNullable(bean).map(BeanUtil::beanToSortedMap).map(BeanUtil::javaToDb).orElse(null);
    }

    public static Map<String, Object> skipBlank(Map<String, Object> map) {
        return Optional.ofNullable(map).map(e -> {
            Map<String, Object> result = new LinkedHashMap<>();
            e.entrySet().stream().filter(entry -> {
                Object value = entry.getValue();
                if (value instanceof CharSequence) {
                    return StringUtils.isNotBlank((CharSequence) value);
                } else {
                    return Objects.nonNull(value);
                }
            }).forEach(entry -> result.put(entry.getKey(), entry.getValue()));
            return result;
        }).orElse(null);
    }

    public static String dbToJava(String column) {
        return Optional.ofNullable(column)
                .map(String::toLowerCase)
                .map(e -> CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, e))
                .orElse(null);
    }

    public static String javaToDb(String property) {
        return javaToDb(property, true);
    }

    public static String javaToDb(String property, boolean toLowerCase) {
        return Optional.ofNullable(property)
                .map(e -> CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, e))
                .map(e -> toLowerCase ? e.toLowerCase() : e.toUpperCase())
                .orElse(null);
    }
}