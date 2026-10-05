# Sokti Data Governance, Privacy & Compliance (DPDP & GDPR)

This document establishes the data governance framework, data subject privacy controls, PII classification, and deletion workflows for the Sokti platform.

---

## 1. Compliance Mandates

Sokti operates under strict compliance with:
1. **India Digital Personal Data Protection Act (DPDP Act 2023):**
   - Principle of Purpose Limitation and Data Minimization.
   - Explicit consent tracking for marketing and recommendation profiling.
   - Right to erasure (Right to be Forgotten) enforceable within 48 hours.
2. **General Data Protection Regulation (GDPR - EU):**
   - Pseudonymization and end-to-end encryption for stored records.
   - Breach notification SLA (< 72 hours).

---

## 2. PII Classification Catalog

All platform data fields are inventoried and classified in `governance/pii_catalog.yaml`:

| Data Classification | Fields | Source Table | Masking / Protection Policy | Retention Period |
|---|---|---|---|---|
| **Direct PII (Confidential)** | `email` | `public.users` | `PARTIAL_EMAIL_MASK` (`a***p@domain.com`) | 2 Years |
| **Direct PII (Confidential)** | `phone` | `public.users` | `PHONE_SUFFIX_MASK` (`+91-XXXXX-98765`) | 2 Years |
| **Pseudonymous Identifier** | `user_id` | `public.users`, `raw_playback_events` | UUID / HMAC-SHA256 | 1 Year active |
| **Device Identifier** | `device_id` | `public.devices` | Salted SHA-256 hash in analytics | 180 Days |
| **Network Identifier** | `ip_address` | Raw HTTP logs | Subnet masking (`192.168.x.x`) | 30 Days |
| **Catalog Metadata (Public)** | `title`, `genres`, `synopsis` | MongoDB / `dim_content` | No masking (Public catalog) | 10 Years |

---

## 3. Data Masking Engine

The platform incorporates automated masking functions (`governance/masking/pii_masker.py`):
```python
mask_email("subscriber@example.com")  # -> "s***r@example.com"
mask_phone("+919876543210")           # -> "+91-XXXXX-3210"
mask_ip("192.168.1.105")              # -> "192.168.x.x"
```
Analytics staging views (`stg_users`) and external consumer APIs enforce this masking so raw email addresses and phone numbers never leak to BI analysts, data scientists, or recommendation models.

---

## 4. Right-to-be-Forgotten / Deletion Workflow

When a data subject requests account erasure, `scripts/delete_user.py` executes a cascading deletion across all platform tiers:

```
                  [User Erasure Request]
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
  PostgreSQL (OLTP)    ClickHouse (OLAP)    Kafka Topics
  - DELETE watchlists  - ALTER TABLE ...    - Send Tombstone
  - DELETE devices       DELETE WHERE         (key=user_id,
  - DELETE profile       user_id = <UUID>      value=None)
  - DELETE users
```

Verification command:
```bash
python scripts/delete_user.py --user-id <UUID>
```
All mutations are logged in audit tables with execution timestamps and operator identities.
