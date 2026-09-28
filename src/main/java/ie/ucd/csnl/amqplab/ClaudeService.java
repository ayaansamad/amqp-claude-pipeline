package ie.ucd.csnl.amqplab;

import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.anthropic.client.AnthropicClient;
import com.anthropic.client.okhttp.AnthropicOkHttpClient;
import com.anthropic.core.JsonValue;
import com.anthropic.models.beta.messages.BetaMessage;
import com.anthropic.models.beta.messages.BetaStopReason;
import com.anthropic.models.beta.messages.MessageCreateParams;

@Service
public class ClaudeService {

    private final AnthropicClient client = AnthropicOkHttpClient.fromEnv();
    private final String model;
    private final long maxTokens;

    public ClaudeService(@Value("${claude.model}") String model,
                         @Value("${claude.max-tokens}") long maxTokens) {
        this.model = model;
        this.maxTokens = maxTokens;
    }

    public String ask(String prompt) {
        MessageCreateParams params = MessageCreateParams.builder()
                .model(model)
                .maxTokens(maxTokens)
                .addUserMessage(prompt)
                // If a safety classifier refuses the request, let the API retry it on a fallback model.
                .addBeta("server-side-fallback-2026-07-01")
                .putAdditionalBodyProperty("fallbacks", JsonValue.from("default"))
                .build();

        BetaMessage response = client.beta().messages().create(params);

        if (response.stopReason().filter(BetaStopReason.REFUSAL::equals).isPresent()) {
            throw new IllegalStateException("Claude declined the request");
        }
        return response.content().stream()
                .flatMap(block -> block.text().stream())
                .map(textBlock -> textBlock.text())
                .collect(Collectors.joining());
    }
}
