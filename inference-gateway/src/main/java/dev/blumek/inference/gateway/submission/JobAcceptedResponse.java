package dev.blumek.inference.gateway.submission;

import java.net.URI;

public record JobAcceptedResponse(String jobId, URI statusUrl) {}
