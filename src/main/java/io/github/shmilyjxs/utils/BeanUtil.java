package io.github.shmilyjxs.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.lang3.StringUtils;
import org.springframework.cglib.beans.BeanCopier;
import org.springframework.cglib.beans.BeanMap;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import javax.persistence.Id;
import javax.persistence.Table;
import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Function;

public class BeanUtil {

    public static <T> Map<String, Object> beanToLinkedMap(T bean) {
        return Optional.ofNullable(bean).map(e -> {
            BeanMap map = BeanMap.create(e);
            Map<String, Object> result = new LinkedHashMap<>(map.size());
            Arrays.stream(e.getClass().getDeclaredFields()).map(Field::getName).forEach(key -> result.put(key, map.get(key)));
            return result;
        }).orElse(null);
    }

    public static <T> Map<String, Object> beanToMap(T bean) {
        return Optional.ofNullable(bean).map(BeanMap::create).orElse(null);
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

    public static <K, V, R> Map<R, V> replaceKey(Map<K, V> map, Function<K, R> keyMapper) {
        return Optional.ofNullable(map).map(e -> {
            Map<R, V> result = new LinkedHashMap<>(e.size());
            e.forEach((k, v) -> result.put(keyMapper.apply(k), v));
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

    public static <F, T> T copy(F from, T to) {
        return Optional.of(from).flatMap(f -> Optional.of(to).map(t -> {
            BeanCopier.create(f.getClass(), t.getClass(), false).copy(f, t, null);
            return t;
        })).get();
    }

    public static <T> Object getFieldValue(T bean, String fieldName) {
        return getFieldValue(bean, ReflectionUtils.findField(bean.getClass(), fieldName));
    }

    public static <T> Object getFieldValue(T bean, Field field) {
        return Optional.of(bean).flatMap(o -> Optional.of(field).map(e -> {
            ReflectionUtils.makeAccessible(e);
            return ReflectionUtils.getField(e, o);
        })).orElse(null);
    }

    public static <T> void setFieldValue(T bean, String fieldName, Object fieldValue) {
        setFieldValue(bean, ReflectionUtils.findField(bean.getClass(), fieldName), fieldValue);
    }

    public static <T> void setFieldValue(T bean, Field field, Object fieldValue) {
        Optional.of(bean).ifPresent(o -> Optional.of(field).ifPresent(e -> {
            ReflectionUtils.makeAccessible(e);
            ReflectionUtils.setField(e, o, fieldValue);
        }));
    }

    public static <T> String getTableName(Class<T> clazz) {
        return Optional.of(clazz).map(e -> AnnotationUtils.findAnnotation(e, Table.class)).map(Table::name).filter(StringUtils::isNotBlank).orElseThrow(NullPointerException::new);
    }

    public static <T> Field idFiled(Class<T> clazz) {
        return Optional.of(clazz).flatMap(e -> Arrays.stream(e.getDeclaredFields()).filter(i -> Objects.nonNull(AnnotationUtils.findAnnotation(i, Id.class))).findAny()).orElseThrow(NullPointerException::new);
    }
}