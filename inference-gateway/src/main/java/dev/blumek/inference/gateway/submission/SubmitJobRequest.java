package dev.blumek.inference.gateway.submission;

public record SubmitJobRequest(String model, String prompt, int maxTokens) {}
