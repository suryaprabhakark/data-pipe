package com.github.datapipe.common.exceptions;

public class ConfigurationMissingException extends RuntimeException {
    public ConfigurationMissingException() {
        super();
    }

    public ConfigurationMissingException(String message) {
        super(message);
    }

    public ConfigurationMissingException(String message, Throwable t) {
        super(message, t);
    }

    public ConfigurationMissingException(Throwable t) {
        super(t);
    }
}
