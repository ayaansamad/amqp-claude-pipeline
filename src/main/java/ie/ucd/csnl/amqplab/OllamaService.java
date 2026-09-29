package ie.ucd.csnl.amqplab;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

/** Calls a local Ollama server (POST /api/chat) - free, no API key, no rate limits. */
@Service
public class OllamaService {

    record ChatMessage(String role, String content) {
    }

    record ChatRequest(String model, List<ChatMessage> messages, boolean stream) {
    }

    record ChatResponse(ChatMessage message) {
    }

    private final RestClient client;
    private final String model;

    public OllamaService(@Value("${ai.base-url}") String baseUrl,
                         @Value("${ai.model}") String model,
                         @Value("${ai.timeout-seconds}") long timeoutSeconds) {
        this.model = model;
        HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(Duration.ofSeconds(timeoutSeconds));
        this.client = RestClient.builder().baseUrl(baseUrl).requestFactory(requestFactory).build();
    }

    public String ask(String prompt) {
        ChatResponse response = client.post()
                .uri("/api/chat")
                .body(new ChatRequest(model, List.of(new ChatMessage("user", prompt)), false))
                .retrieve()
                .body(ChatResponse.class);
        if (response == null || response.message() == null || response.message().content() == null) {
            throw new IllegalStateException("Ollama returned an empty response");
        }
        return response.message().content();
    }

    /**
     * Transient failures are worth retrying: the server is down or unreachable, the call timed out
     * (ResourceAccessException), rate limit (429), request timeout (408), or a 5xx error.
     * Everything else (400 bad request, 404 model not pulled, ...) fails the same way every time.
     */
    public static boolean isTransient(Throwable e) {
        if (e instanceof ResourceAccessException) {
            return true;
        }
        if (e instanceof HttpStatusCodeException h) {
            return h.getStatusCode().is5xxServerError()
                    || h.getStatusCode().value() == HttpStatus.TOO_MANY_REQUESTS.value()
                    || h.getStatusCode().value() == HttpStatus.REQUEST_TIMEOUT.value();
        }
        return false;
    }
}
