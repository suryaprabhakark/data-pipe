package com.github.datapipe.common.utils;

import com.typesafe.config.Config;

import java.util.Optional;

public class ConfigUtils {
    public static <T> Optional<T> getValue(Config config, String path, Class<T> clazz) {
        return config.hasPath(path) ? Optional.of(clazz.cast(config.getValue(path).unwrapped())) : Optional.empty();
    }
}
