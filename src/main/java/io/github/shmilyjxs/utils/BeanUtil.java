package io.github.shmilyjxs.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.cglib.beans.BeanCopier;
import org.springframework.cglib.beans.BeanMap;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import javax.persistence.Id;
import javax.persistence.Table;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Function;

public abstract class BeanUtil {

    public static <T> Map<String, Object> beanToMap(T bean) {
        return Optional.ofNullable(bean).map(BeanMap::create).orElse(null);
    }

    public static <T> T mapToBean(Map<String, ?> map, Class<T> type) {
        return Optional.ofNullable(map).map(e -> {
            T bean = BeanUtils.instantiateClass(type);
            BeanMap.create(bean).putAll(e);
            return bean;
        }).orElse(null);
    }

    public static Map<String, Object> skipBlank(Map<String, ?> map) {
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

    public static <F, T> void copy(F from, T to) {
        Optional.of(from).ifPresent(f -> Optional.of(to).ifPresent(t -> BeanCopier.create(f.getClass(), t.getClass(), false).copy(f, t, null)));
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

    public static String getTableName(Class<?> clazz) {
        String tableName = Optional.ofNullable(clazz).map(e -> AnnotationUtils.findAnnotation(e, Table.class)).map(Table::name).filter(StringUtils::isNotBlank).orElse(null);
        return Objects.requireNonNull(tableName);
    }

    public static Field idFiled(Class<?> clazz) {
        Field idFiled = Optional.ofNullable(clazz).map(e -> getFiled(e, Id.class)).orElseGet(() -> findFiled(clazz, Id.class));
        return Objects.requireNonNull(idFiled);
    }

    private static Field getFiled(Class<?> clazz, Class<? extends Annotation> annotationType) {
        return Optional.ofNullable(clazz)
                .map(BeanUtils::getPropertyDescriptors)
                .flatMap(e -> Arrays.stream(e).filter(pd -> Objects.nonNull(AnnotationUtils.findAnnotation(pd.getReadMethod(), annotationType))).map(PropertyDescriptor::getName).findAny())
                .map(e -> ReflectionUtils.findField(clazz, e))
                .orElse(null);
    }

    private static Field findFiled(Class<?> clazz, Class<? extends Annotation> annotationType) {
        if (Objects.equals(clazz, Object.class)) {
            return null;
        }
        return Optional.ofNullable(clazz)
                .flatMap(e -> Arrays.stream(e.getDeclaredFields())
                        .filter(field -> Objects.nonNull(AnnotationUtils.findAnnotation(field, annotationType)))
                        .findAny())
                .orElseGet(() -> Optional.ofNullable(clazz)
                        .map(Class::getSuperclass)
                        .map(e -> findFiled(e, annotationType))
                        .orElse(null));
    }
}