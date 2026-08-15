package dev.blumek.inference.gateway.submission;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record SubmitJobRequest(@NotBlank(message = "must be provided")
                               @Pattern(regexp = "[a-z0-9._-]+(:[a-z0-9._-]+)?",
                                        message = "must be a lowercase name with an optional :tag, such as llama3:8b")
                               String model,

                               @NotBlank(message = "must be provided")
                               @Size(max = 32_000, message = "must be at most 32000 characters")
                               String prompt,

                               @Min(value = 1, message = "must be at least 1")
                               @Max(value = 8192, message = "must be at most 8192")
                               int maxTokens) {}
