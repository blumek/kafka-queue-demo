package dev.blumek.inference.gateway.submission;

import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Objects.requireNonNullElse;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

@RestControllerAdvice
class ApiExceptionHandler extends ResponseEntityExceptionHandler {
    private static final String VIOLATIONS = "errors";
    private static final String UNREADABLE_VALUE = "is invalid";

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(final MethodArgumentNotValidException exception,
                                                                  final HttpHeaders headers,
                                                                  final HttpStatusCode status,
                                                                  final WebRequest request) {
        final var problem = exception.updateAndGetBody(getMessageSource(), LocaleContextHolder.getLocale());
        problem.setTitle("Invalid submission");
        problem.setProperty(VIOLATIONS, violationsOf(exception));
        return handleExceptionInternal(exception, problem, headers, status, request);
    }

    private static Map<String, List<String>> violationsOf(final MethodArgumentNotValidException exception) {
        return exception.getFieldErrors().stream()
                .collect(groupingBy(FieldError::getField, TreeMap::new, mapping(ApiExceptionHandler::messageOf, toList())));
    }

    private static String messageOf(final FieldError error) {
        return requireNonNullElse(error.getDefaultMessage(), UNREADABLE_VALUE);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<ProblemDetail> handleUnacceptableValue(final IllegalArgumentException exception) {
        logger.warn("rejecting a submission the domain refused", exception);
        return ResponseEntity.badRequest()
                .body(problem(HttpStatus.BAD_REQUEST, "Invalid submission",
                        "The submission carried a value this service cannot accept."));
    }

    @ExceptionHandler(MalformedIdempotencyKeyException.class)
    ResponseEntity<ProblemDetail> handleMalformedIdempotencyKey() {
        return ResponseEntity.badRequest()
                .body(problem(HttpStatus.BAD_REQUEST, "Invalid submission",
                        "The Idempotency-Key header must be a UUID."));
    }

    @ExceptionHandler(PublisherUnavailableException.class)
    ResponseEntity<ProblemDetail> handlePublisherUnavailable(final PublisherUnavailableException exception) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, String.valueOf(exception.retryAfter().toSeconds()))
                .body(problem(HttpStatus.SERVICE_UNAVAILABLE, "Job queue unavailable",
                        "The job was not queued. Submit it again after the interval in the Retry-After header."));
    }

    @ExceptionHandler(PublishRejectedException.class)
    ResponseEntity<ProblemDetail> handlePublishRejected(final PublishRejectedException exception) {
        logger.error("the broker rejected a job outright", exception);
        return ResponseEntity.internalServerError()
                .body(problem(HttpStatus.INTERNAL_SERVER_ERROR, "Job rejected",
                        "The job was rejected and resubmitting it unchanged will not help."));
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<ProblemDetail> handleUnexpectedFailure(final Exception exception) {
        logger.error("unhandled failure while serving a request", exception);
        return ResponseEntity.internalServerError()
                .body(problem(HttpStatus.INTERNAL_SERVER_ERROR, "Internal Server Error",
                        "The request could not be completed."));
    }

    private static ProblemDetail problem(final HttpStatus status, final String title, final String detail) {
        final var problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setTitle(title);
        return problem;
    }
}
