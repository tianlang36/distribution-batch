package cn.rdtimes.disb.core;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 分片信息类
 * 能被序列化
 * <p>
 * Created by BZ.
 */
public class BSplit implements Serializable {
    private final static long serialVersionUID = -1;

    //这里的值应该都能被序列化
    private final Map<String, Object> kvs = new HashMap<String, Object>(2);

    public BSplit() {}

    public boolean containKey(String key) {
        return kvs.containsKey(key);
    }

    public boolean containValue(Object value) {
        return kvs.containsValue(value);
    }

    public int size() {
        return kvs.size();
    }

    public Set<Map.Entry<String, Object>> entrySet() {
        return kvs.entrySet();
    }

    public Object remove(String key) {
        return kvs.remove(key);
    }

    public Object get(String key) {
        return kvs.get(key);
    }

    public String getString(String key) {
        Object obj = get(key);
        return obj == null ? null : obj.toString();
    }

    public int getInt(String key) {
        return getInt(key, Integer.MAX_VALUE);
    }

    public int getInt(String key, int defaultValue) {
        Object obj = get(key);
        return obj == null ? defaultValue : Integer.parseInt(obj.toString());
    }

    public long getLong(String key) {
        return getLong(key, Long.MAX_VALUE);
    }

    public long getLong(String key, long defaultValue) {
        Object obj = get(key);
        return obj == null ? defaultValue : Long.parseLong(obj.toString());
    }

    public double getDouble(String key) {
        return getDouble(key, Double.MAX_VALUE);
    }

    public double getDouble(String key, double defaultValue) {
        Object obj = get(key);
        return obj == null ? defaultValue : Double.parseDouble(obj.toString());
    }

    public void put(String key, Object value) {
        kvs.put(key, value);
    }

    public void putString(String key, String value) {
        kvs.put(key, value);
    }

    public void putInt(String key, int value) {
        kvs.put(key, value);
    }

    public void putLong(String key, long value) {
        kvs.put(key, value);
    }

    public void putDouble(String key, double value) {
        kvs.put(key, value);
    }

    @Override
    public int hashCode() {
        return kvs.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return kvs.equals(obj);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(32);
        Set<Map.Entry<String, Object>> entries = kvs.entrySet();
        int count = kvs.size();
        int i = 0;
        for (Map.Entry<String, Object> entry : entries) {
            ++i;
            sb.append(entry.getKey()).append(":").append(entry.getValue());
            if (i != count) sb.append("; ");
        }

        return (sb.length() == 0 ? super.toString() : sb.toString());
    }

}
