"""
Sokti PII Masking Utilities
Implements DPDP/GDPR compliant masking for emails, phones, and IP addresses.
"""

import re
from typing import Optional


def mask_email(email: Optional[str]) -> str:
    """Mask email as e***e@domain.com."""
    if not email or "@" not in email:
        return "anonymized@sokti.internal"
    name, domain = email.split("@", 1)
    if len(name) <= 2:
        masked_name = name[0] + "***"
    else:
        masked_name = name[0] + "***" + name[-1]
    return f"{masked_name}@{domain}"


def mask_phone(phone: Optional[str]) -> str:
    """Mask phone as +XX-XXXXX-98765 or similar suffix mask."""
    if not phone:
        return "+XX-XXXXX-XXXXX"
    # Keep last 4 digits
    digits = re.sub(r"\D", "", phone)
    if len(digits) >= 10:
        return f"+91-XXXXX-{digits[-4:]}"
    return "+XX-XXXX-XXXX"


def mask_ip(ip: Optional[str]) -> str:
    """Mask IP as 192.168.x.x."""
    if not ip:
        return "0.0.0.0"
    parts = ip.split(".")
    if len(parts) == 4:
        return f"{parts[0]}.{parts[1]}.x.x"
    return "x.x.x.x"


if __name__ == "__main__":
    print("Email Masked:", mask_email("aviroop.ghosh@streaming.com"))
    print("Phone Masked:", mask_phone("+91-9876543210"))
    print("IP Masked:", mask_ip("192.168.1.105"))
