"""Review summarization contract.

A future function can assume the IAM role from the Terraform llm module and
call Bedrock. This file does not import a cloud SDK and does not call AWS.
"""

MODEL_ID_ENV = "BEDROCK_MODEL_ID"
MAX_REVIEWS = 20

SYSTEM_PROMPT = (
    "Summarize the product reviews in neutral language. "
    "Mention recurring praise and complaints. Do not invent reviews."
)


def summarize_reviews(review_bodies: list[str]) -> str:
    """Return one short summary for a single product's reviews.

    ``review_bodies`` is raw review text, at most ``MAX_REVIEWS`` items.
    The caller selects the model with the ``BEDROCK_MODEL_ID`` environment
    variable. The platform default is amazon.nova-micro-v1:0.
    """
    if len(review_bodies) > MAX_REVIEWS:
        raise ValueError(f"At most {MAX_REVIEWS} reviews per call")
    raise NotImplementedError(
        "Bedrock invocation is a later milestone. See docs/architecture.md."
    )
