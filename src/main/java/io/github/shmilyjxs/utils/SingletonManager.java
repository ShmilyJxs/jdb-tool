package io.github.shmilyjxs.utils;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public abstract class SingletonManager {

    private static final Object LOCK = new Object();
    private static final Map<Object, Object> SINGLETON_MAP = new ConcurrentHashMap<>();
    private static final Map<Object, Supplier<?>> SUPPLIER_MAP = new ConcurrentHashMap<>();

    public static Object getObj(Object key) {
        Object obj = SINGLETON_MAP.get(Objects.requireNonNull(key));
        if (obj == null) {
            synchronized (LOCK) {
                obj = SINGLETON_MAP.get(key);
                if (obj == null) {
                    Supplier<?> supplier = Objects.requireNonNull(SUPPLIER_MAP.get(key));
                    obj = Objects.requireNonNull(supplier.get());
                    SINGLETON_MAP.put(key, obj);
                }
            }
        }
        return obj;
    }

    public static void registryObj(Object key, Supplier<?> supplier) {
        SUPPLIER_MAP.put(Objects.requireNonNull(key), Objects.requireNonNull(supplier));
        SINGLETON_MAP.remove(key);
    }
}