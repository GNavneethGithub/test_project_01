# simple_email.py
from __future__ import annotations
import smtplib
from email.message import EmailMessage
from typing import Iterable, Optional, List, Tuple
import logging
import time
import mimetypes

logger = logging.getLogger(__name__)

Attachment = Tuple[str, bytes, Optional[str]]  # (filename, data, mime_type or None)

def _build_message(
    subject: str,
    sender: str,
    to: Iterable[str],
    *,
    text: Optional[str] = None,
    html: Optional[str] = None,
    cc: Optional[Iterable[str]] = None,
    bcc: Optional[Iterable[str]] = None,
    attachments: Optional[Iterable[Attachment]] = None,
) -> tuple[EmailMessage, list[str]]:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    to_list = list(to)
    cc_list = list(cc) if cc else []
    bcc_list = list(bcc) if bcc else []
    msg["To"] = ", ".join(to_list)
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)

    # Body
    if text:
        msg.set_content(text)
    if html:
        if not text:
            msg.set_content("This message contains HTML. View in an HTML capable client.")
        msg.add_alternative(html, subtype="html")

    # Attachments
    if attachments:
        for filename, data, mime in attachments:
            if mime is None:
                mime, _ = mimetypes.guess_type(filename)
            if not mime:
                mime = "application/octet-stream"
            maintype, subtype = mime.split("/", 1)
            msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=filename)

    recipients = to_list + cc_list + bcc_list
    return msg, recipients

def send_email(
    subject: str,
    sender: str,
    to: Iterable[str],
    *,
    text: Optional[str] = None,
    html: Optional[str] = None,
    cc: Optional[Iterable[str]] = None,
    bcc: Optional[Iterable[str]] = None,
    attachments: Optional[Iterable[Attachment]] = None,
    smtp_server: str = "mailhost",
    port: int = 25,
    starttls: bool = False,
    username: Optional[str] = None,
    password: Optional[str] = None,
    timeout: int = 10,
    retries: int = 1,
    retry_delay_seconds: int = 5,
) -> None:
    """
    Sends an email. Raises the last exception if all retries fail.
    Keep this functional and easy to call from scripts.
    """
    msg, recipients = _build_message(subject, sender, to, text=text, html=html,
                                     cc=cc, bcc=bcc, attachments=attachments)

    last_exc = None
    for attempt in range(1, max(1, retries) + 1):
        try:
            with smtplib.SMTP(smtp_server, port, timeout=timeout) as smtp:
                smtp.ehlo()
                if starttls:
                    smtp.starttls()
                    smtp.ehlo()
                if username and password:
                    smtp.login(username, password)
                smtp.send_message(msg, from_addr=sender, to_addrs=recipients)
            logger.info("Email sent to %s (subject=%s)", recipients, subject)
            return
        except Exception as exc:
            last_exc = exc
            logger.warning("Attempt %d sending email failed: %s", attempt, exc)
            if attempt < retries:
                time.sleep(retry_delay_seconds)
    logger.error("Failed to send email after %d attempts", retries)
    raise last_exc




# import os
# from simple_email import send_email

# send_email(
#     subject="Alert: job failed",                # Subject
#     sender="alerts@example.com",                # From address
#     to=["dev1@example.com","dev2@example.com"], # Recipients
#     text="Main message: job xyz failed at 10:12 UTC. Check logs.",  # Message body (plain text)
#     smtp_server="mail.corp", port=587, starttls=True,
#     username=os.environ.get("SMTP_USER"),
#     password=os.environ.get("SMTP_PASS"),
#     retries=3, retry_delay_seconds=5,
# )





