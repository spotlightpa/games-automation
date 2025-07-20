from modules.logging_utils import log

def log_token_usage(usage):
    input_tokens = usage.prompt_tokens
    output_tokens = usage.completion_tokens
    total_tokens = usage.total_tokens

    input_cost = input_tokens * 0.005 / 1000
    output_cost = output_tokens * 0.015 / 1000
    total_cost = input_cost + output_cost

    log(f"🔎 Tokens used — Prompt: {input_tokens}, Completion: {output_tokens}, Total: {total_tokens}")
    log(f"💵 Estimated cost: ${total_cost:.6f}")

    return total_cost
