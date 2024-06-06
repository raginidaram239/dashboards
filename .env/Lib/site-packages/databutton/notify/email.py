from dataclasses import dataclass
from typing import List, Optional, Union

from dataclasses_json import dataclass_json

from .send import send


@dataclass_json
@dataclass
class Email:
    to: Union[str, List[str]]
    subject: str
    content_text: Optional[str]
    content_html: Optional[str]


def valid_email(recipient: str) -> bool:
    # Note: We could possibly use some email validation library but it's tricky
    parts = recipient.split("@")
    if len(parts) != 2:
        return False
    return bool(parts[0] and parts[1])


def validate_email_to_arg(to: Union[str, List[str]]) -> List[str]:
    if isinstance(to, str):
        to = [to]
    if not isinstance(to, (list, tuple)) and len(to) > 0:
        raise ValueError(
            "Invalid recipient, expecting 'to' to be a string or list of strings."
        )
    invalid_emails = []
    for recipient in to:
        if not valid_email(recipient):
            invalid_emails.append(recipient)
    if invalid_emails:
        raise ValueError("\n".join(["Invalid email address(es):"] + invalid_emails))
    return to


def email(
    to: Union[str, List[str]],
    subject: str,
    content_text: Optional[str] = None,
    content_html: Optional[str] = None,
):
    """Send email notification from databutton.

    At least one of the content arguments must be present.

    A link to the project will be added at the end of the email body.

    If content_text is not provided it will be generated from
    content_html for email clients without html support,
    the result may be less pretty than handcrafted text.
    """
    send(
        Email(
            to=validate_email_to_arg(to),
            subject=subject,
            content_text=content_text,
            content_html=content_html,
        )
    )
