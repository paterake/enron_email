package com.paterake.enron.util;

public interface ContextWriter<K, V> {
    public void write(K key, V value);
}