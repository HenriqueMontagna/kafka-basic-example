package com.montagna;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class GsonDeserializer<T> implements Deserializer<T> {

    public static final String TYPE_CONFIG = "com.montagna.type_config";

    private final Gson gsonBuilder = new GsonBuilder().create();

    private Class<T> type;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

        String className = String.valueOf(configs.get(TYPE_CONFIG));

        try {
            this.type = (Class<T>) Class.forName(className);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    @Override
    public T deserialize(String s, byte[] bytes) {

        return gsonBuilder.fromJson(new String(bytes), this.type);

    }

}
