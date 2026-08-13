package dev.blumek.inference.domain.model;

import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public record ModelId(String value) {
    private static final Pattern VALID = Pattern.compile("[a-z0-9._-]+(:[a-z0-9._-]+)?");

    public ModelId {
        requireNonNull(value);
        if (!VALID.matcher(value).matches()) {
            throw new IllegalArgumentException("invalid model id: " + value);
        }
    }
}
