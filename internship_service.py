"""
internship_service.py  ─  Internship Microservice
Port : 5050
Run  : uvicorn internship_service:app --host 0.0.0.0 --port 5050

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Redis cache strategy  (all I/O via redis_service HTTP API)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  internship:applications           DB 5   10 min   admin list (all / filtered by status)
  internship:applications:{status}  DB 5   10 min   admin list filtered by status
  internship:{user_id}              DB 5    5 min   per-student application view

Eviction on every write mutation
  POST /api/internship/apply           → evict internship:applications*
  POST /api/internship/admin/select    → evict internship:applications*
  POST /api/internship/admin/status    → evict internship:applications*

NOT cached (by design)
  x  Selection letter URLs / presigned URLs — always generate fresh
  x  Individual application detail (/letter) — small query, no benefit
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import io
import os
import sys
import uuid
import smtplib
import urllib.request
import boto3
import httpx
import oracledb
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import secrets
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
from werkzeug.utils import secure_filename
import pathlib
import uvicorn
import secrets
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from reportlab.lib.pdfencrypt import StandardEncryption
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas as pdf_canvas
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, HRFlowable, Table, TableStyle

_env_path = pathlib.Path(__file__).parent / ".env"
load_dotenv(dotenv_path=_env_path)

if sys.stdout.encoding != "UTF-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding != "UTF-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

app = FastAPI(title="Internship Service")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://www.chakorahub.com", "https://chakorahub.com",
        "http://www.chakorahub.com",  "http://chakorahub.com",
        "http://127.0.0.1:8080",      "http://localhost:8080",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# MAINTENANCE MODE
# ============================================================
_default_flag_path = (
    pathlib.Path(__file__).parent / "internship-maintenance.flag"
    if os.name == "nt"
    else pathlib.Path("/home/ec2-user/internship-maintenance.flag")
)

MAINTENANCE_FLAG = pathlib.Path(
    os.getenv("MAINTENANCE_FLAG", str(_default_flag_path))
)

MAINTENANCE_TOKEN = os.getenv("MAINTENANCE_TOKEN", "chakora-maintenance-token")


def is_maintenance_enabled() -> bool:
    return MAINTENANCE_FLAG.exists()


@app.get("/internship/maintenance/status")
@app.get("/api/internship/maintenance/status")
def maintenance_status():
    return {
        "success": True,
        "maintenance_mode": is_maintenance_enabled(),
        "message": "Maintenance is active" if is_maintenance_enabled() else "Service is operational"
    }


def verify_maintenance_token(request: Request):
    auth_header = request.headers.get("Authorization", "")
    supplied_token = auth_header.removeprefix("Bearer ").strip() if auth_header.startswith("Bearer ") else auth_header.strip()

    expected_token = (MAINTENANCE_TOKEN or "chakora-maintenance-token").strip()

    if not secrets.compare_digest(
        supplied_token,
        expected_token
    ):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized"
        )


@app.post("/admin/internship/maintenance/on")
@app.post("/api/admin/internship/maintenance/on")
@app.post("/api/internship/maintenance/on")
def enable_maintenance(request: Request):
    verify_maintenance_token(request)

    MAINTENANCE_FLAG.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    MAINTENANCE_FLAG.touch()

    return {
        "success": True,
        "maintenance_mode": True,
        "message": "Internship maintenance mode enabled"
    }


@app.post("/admin/internship/maintenance/off")
@app.post("/api/admin/internship/maintenance/off")
@app.post("/api/internship/maintenance/off")
def disable_maintenance(request: Request):
    verify_maintenance_token(request)

    MAINTENANCE_FLAG.unlink(
        missing_ok=True
    )

    return {
        "success": True,
        "maintenance_mode": False,
        "message": "Internship maintenance mode disabled"
    }


# ============================================================
# REDIS SERVICE CLIENT  (HTTP — zero direct redis imports)
# ============================================================
_REDIS_SERVICE_URL = os.getenv("REDIS_SERVICE_URL", "http://localhost:6380").rstrip("/")
_REDIS_TIMEOUT     = float(os.getenv("REDIS_SERVICE_TIMEOUT", "2"))

ORACLE_HOST = os.getenv("ORACLE_HOST", "56.228.73.210")
ORACLE_PORT = int(os.getenv("ORACLE_PORT", "1521"))
ORACLE_SERVICE_NAME = os.getenv("ORACLE_SERVICE_NAME", "FREEPDB1")
ORACLE_USER = os.getenv("ORACLE_USER", "SUPPORT")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "Welcome123")

# Canonical TTLs — must match redis_service.py TTL_INTERNSHIP_* constants
_TTL_APPLICATIONS = int(os.getenv("INTERNSHIP_CACHE_TTL_APPLICATIONS", "600"))  # 10 min
_TTL_USER         = int(os.getenv("INTERNSHIP_CACHE_TTL_USER",         "300"))  #  5 min


def _cache_get(key: str):
    """
    GET a cached value via redis_service.
    Returns parsed value on HIT, None on MISS or any transport error (fail-open).
    """
    try:
        r    = httpx.get(f"{_REDIS_SERVICE_URL}/internship/cache/get",
                         params={"key": key}, timeout=_REDIS_TIMEOUT)
        body = r.json()
        if body.get("found"):
            return body.get("data")
    except Exception as exc:
        print(f"⚠️  Redis GET error [{key}]: {exc}")
    return None


def _cache_set(key: str, data, ttl: int = None) -> bool:
    """
    SET a value via redis_service. Fails silently — cache misses are never fatal.
    Uses the canonical TTL for the key unless an explicit ttl is given.
    """
    try:
        payload = {"key": key, "data": data}
        if ttl:
            payload["ttl"] = ttl
        r = httpx.post(f"{_REDIS_SERVICE_URL}/internship/cache/set",
                       json=payload, timeout=_REDIS_TIMEOUT)
        return r.json().get("success", False)
    except Exception as exc:
        print(f"⚠️  Redis SET error [{key}]: {exc}")
        return False


def _cache_delete(key: str) -> bool:
    """
    DELETE a single cache key via redis_service.
    Called on every write mutation to prevent stale reads. Fails silently.
    """
    try:
        r = httpx.delete(f"{_REDIS_SERVICE_URL}/internship/cache/delete",
                         params={"key": key}, timeout=_REDIS_TIMEOUT)
        return r.json().get("success", False)
    except Exception as exc:
        print(f"⚠️  Redis DELETE error [{key}]: {exc}")
        return False


def _cache_delete_pattern(pattern: str) -> bool:
    """
    SCAN-based pattern eviction via redis_service.
    Used to nuke all internship:applications* variants at once.
    """
    try:
        r = httpx.delete(f"{_REDIS_SERVICE_URL}/internship/cache/delete-pattern",
                         params={"pattern": pattern}, timeout=_REDIS_TIMEOUT)
        body = r.json()
        print(f"🗑️  Cache pattern evict [{pattern}]: {body.get('count', 0)} keys removed")
        return body.get("success", False)
    except Exception as exc:
        print(f"⚠️  Redis DELETE-PATTERN error [{pattern}]: {exc}")
        return False


def _evict_on_write(user_id=None) -> None:
    """
    Evict all internship cache keys that could be stale after any write:
      internship:applications*  — all admin-list variants (pattern evict)
      internship:{user_id}      — per-student view (only when user_id known)
    """
    _cache_delete_pattern("internship:applications*")
    if user_id is not None:
        _cache_delete(f"internship:{user_id}")


# ============================================================
# AWS S3 CONFIGURATION
# ============================================================
AWS_ACCESS_KEY    = (os.getenv("AWS_ACCESS_KEY", "").strip()
                     or os.getenv("AWS_ACCESS_KEY_ID", "").strip())
AWS_SECRET_KEY    = (os.getenv("AWS_SECRET_KEY", "").strip()
                     or os.getenv("AWS_SECRET_ACCESS_KEY", "").strip())
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN", "").strip()
AWS_REGION        = (os.getenv("AWS_REGION", "").strip()
                     or os.getenv("AWS_DEFAULT_REGION", "").strip()
                     or "eu-north-1")
S3_BUCKET         = os.getenv("S3_BUCKET", "chakora-internship-docs-s3").strip()

# ============================================================
# SMTP / EMAIL CONFIGURATION
# ============================================================
SMTP_HOST     = os.getenv("SMTP_HOST", "smtp.gmail.com").strip()
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587").strip())
SMTP_USER     = os.getenv("SMTP_USER", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").strip()
SMTP_FROM     = os.getenv("SMTP_FROM", SMTP_USER).strip()

APPLICATION_MAIL_ENABLED   = os.getenv("APPLICATION_MAIL_ENABLED",  "true").strip().lower() in {"1","true","yes","on"}
APPLICATION_MAIL_FROM      = os.getenv("APPLICATION_MAIL_FROM", "admin@chakorahub.com").strip() or "admin@chakorahub.com"
APPLICATION_MAIL_CC        = os.getenv("APPLICATION_MAIL_CC",   "admin@chakorahub.com").strip() or "admin@chakorahub.com"
SES_REGION                 = os.getenv("SES_REGION", AWS_REGION).strip() or AWS_REGION
SELECTION_MAIL_ENABLED     = os.getenv("SELECTION_MAIL_ENABLED", "true").strip().lower() in {"1","true","yes","on"}
LETTER_LINK_EXPIRY_SECONDS = int(os.getenv("LETTER_LINK_EXPIRY_SECONDS", "604800").strip())


def build_s3_client():
    if AWS_ACCESS_KEY and AWS_SECRET_KEY:
        kwargs = {"region_name": AWS_REGION, "aws_access_key_id": AWS_ACCESS_KEY,
                  "aws_secret_access_key": AWS_SECRET_KEY}
        if AWS_SESSION_TOKEN:
            kwargs["aws_session_token"] = AWS_SESSION_TOKEN
        print("[startup] S3 client: explicit AWS keys")
        return boto3.client("s3", **kwargs), True
    print("[startup] S3 client: IAM role/default chain")
    return boto3.client("s3", region_name=AWS_REGION), False


s3, using_explicit_aws_keys = build_s3_client()


def build_ses_client():
    if AWS_ACCESS_KEY and AWS_SECRET_KEY:
        kwargs = {"region_name": SES_REGION, "aws_access_key_id": AWS_ACCESS_KEY,
                  "aws_secret_access_key": AWS_SECRET_KEY}
        if AWS_SESSION_TOKEN:
            kwargs["aws_session_token"] = AWS_SESSION_TOKEN
        return boto3.client("ses", **kwargs)
    return boto3.client("ses", region_name=SES_REGION)


ses = build_ses_client()
print(f"[startup] dotenv  : {_env_path} ({'found' if _env_path.exists() else 'not found'})")
print(f"[startup] S3      : {S3_BUCKET or 'NOT_SET'}")
print(f"[startup] Redis   : {_REDIS_SERVICE_URL}")

ALLOWED_EXT = {"pdf", "jpg", "jpeg", "png", "webp", "doc", "docx"}


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXT


# ============================================================
# SELECTION LETTER PDF GENERATION
# ============================================================
def generate_selection_letter_pdf(intern_data: dict, start_date: str, end_date: str) -> bytes:
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4,
                            leftMargin=25*mm, rightMargin=25*mm,
                            topMargin=20*mm,  bottomMargin=20*mm)

    brand_color  = colors.HexColor("#1a3c6e")
    accent_color = colors.HexColor("#e07b00")

    title_style      = ParagraphStyle("TitleS",  fontSize=22, fontName="Helvetica-Bold",
                                      textColor=brand_color, alignment=TA_CENTER, spaceAfter=2*mm)
    subtitle_style   = ParagraphStyle("SubS",    fontSize=10, fontName="Helvetica",
                                      textColor=accent_color, alignment=TA_CENTER, spaceAfter=1*mm)
    center_hdr_style = ParagraphStyle("CtrHdr",  fontSize=13, fontName="Helvetica-Bold",
                                      textColor=brand_color, alignment=TA_CENTER,
                                      spaceAfter=2*mm, spaceBefore=4*mm)
    body_style       = ParagraphStyle("BodyS",   fontSize=10.5, fontName="Helvetica",
                                      leading=17, alignment=TA_JUSTIFY, spaceAfter=4*mm)
    label_style      = ParagraphStyle("LblS",    fontSize=10, fontName="Helvetica-Bold", textColor=brand_color)
    value_style      = ParagraphStyle("ValS",    fontSize=10, fontName="Helvetica")
    small_style      = ParagraphStyle("SmallS",  fontSize=8.5, fontName="Helvetica",
                                      textColor=colors.grey, alignment=TA_CENTER)

    today     = datetime.now().strftime("%d %B %Y")
    full_name = intern_data.get("full_name", "")
    intern_id = intern_data.get("intern_id", "")
    college   = intern_data.get("college_name", "")
    branch    = intern_data.get("branch", "")
    domain    = intern_data.get("internship_domain", "")
    duration  = intern_data.get("internship_duration", "")

    story = []
    story.append(Paragraph("CHAKORAHUB", title_style))
    story.append(Paragraph("Education &amp; Technology Solutions", subtitle_style))
    story.append(HRFlowable(width="100%", thickness=2, color=brand_color, spaceAfter=5*mm))
    story.append(Paragraph("INTERNSHIP SELECTION LETTER", center_hdr_style))
    story.append(HRFlowable(width="55%", thickness=1, color=accent_color, spaceAfter=6*mm))
    story.append(Paragraph(f"Date: <b>{today}</b>", body_style))
    story.append(Paragraph(f"Ref No: <b>{intern_id}</b>", body_style))
    story.append(Spacer(1, 4*mm))
    story.append(Paragraph("To,", body_style))
    story.append(Paragraph(f"<b>{full_name}</b>", body_style))
    story.append(Paragraph(f"{branch}, {college}", body_style))
    story.append(Spacer(1, 5*mm))
    story.append(Paragraph("Subject: <b>Selection for Internship Programme — ChakoraHub</b>", body_style))
    story.append(Spacer(1, 3*mm))
    story.append(Paragraph(f"Dear {full_name},", body_style))
    story.append(Spacer(1, 2*mm))
    story.append(Paragraph(
        f"We are pleased to inform you that after careful review of your application and profile, "
        f"you have been <b>selected</b> for the <b>{domain}</b> Internship Programme at "
        f"<b>ChakoraHub</b>. This programme is designed to provide you with real-world industry "
        f"experience and practical exposure in your chosen domain.",
        body_style,
    ))
    story.append(Spacer(1, 4*mm))
    details = [
        [Paragraph("<b>Internship ID</b>",     label_style), Paragraph(intern_id,  value_style)],
        [Paragraph("<b>Domain / Area</b>",     label_style), Paragraph(domain,     value_style)],
        [Paragraph("<b>Duration</b>",          label_style), Paragraph(duration,   value_style)],
        [Paragraph("<b>Commencement Date</b>", label_style), Paragraph(start_date, value_style)],
        [Paragraph("<b>End Date</b>",          label_style), Paragraph(end_date,   value_style)],
    ]
    tbl = Table(details, colWidths=[65*mm, 100*mm])
    tbl.setStyle(TableStyle([
        ("ROWBACKGROUNDS", (0,0), (-1,-1), [colors.HexColor("#eef2fb"), colors.white]),
        ("BOX",            (0,0), (-1,-1), 0.5, colors.HexColor("#b0b8cc")),
        ("INNERGRID",      (0,0), (-1,-1), 0.5, colors.HexColor("#b0b8cc")),
        ("TOPPADDING",     (0,0), (-1,-1), 5),
        ("BOTTOMPADDING",  (0,0), (-1,-1), 5),
        ("LEFTPADDING",    (0,0), (-1,-1), 7),
    ]))
    story.append(tbl)
    story.append(Spacer(1, 6*mm))
    story.append(Paragraph(
        "You are requested to report on the commencement date mentioned above. Please carry a copy "
        "of this letter along with your college ID card and all relevant documents. We expect you "
        "to maintain discipline, punctuality, and professional conduct throughout the programme.",
        body_style,
    ))
    story.append(Paragraph(
        "We look forward to having you as part of the ChakoraHub team. "
        "Congratulations and best wishes for your learning journey!", body_style))
    story.append(Spacer(1, 10*mm))
    sig = Table(
        [[Paragraph("Authorised Signatory", label_style), Paragraph("", value_style)],
         [Paragraph("ChakoraHub",           value_style), Paragraph("", value_style)]],
        colWidths=[85*mm, 80*mm],
    )
    story.append(sig)
    story.append(Spacer(1, 8*mm))
    story.append(HRFlowable(width="100%", thickness=1, color=brand_color, spaceBefore=4*mm, spaceAfter=3*mm))
    story.append(Paragraph(
        f"Computer-generated letter — no physical signature required. &nbsp; "
        f"Document ID: {intern_id}-SEL &nbsp;|&nbsp; Generated: {today}", small_style))
    doc.build(story)
    buffer.seek(0)
    return buffer.read()


def upload_to_s3(file_storage, doc_type, intern_id):
    if not s3:
        raise Exception("S3 client not initialized")
    ext    = secure_filename(file_storage.filename).rsplit(".", 1)[-1].lower()
    s3_key = f"internships/{intern_id}/{doc_type}/{uuid.uuid4().hex}.{ext}"
    ct     = file_storage.content_type or "application/octet-stream"
    file_storage.file.seek(0)
    file_bytes = file_storage.file.read()
    if not file_bytes:
        raise Exception("Uploaded file is empty")
    try:
        s3.upload_fileobj(io.BytesIO(file_bytes), S3_BUCKET, s3_key, ExtraArgs={"ContentType": ct})
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if using_explicit_aws_keys and error_code in {
            "InvalidAccessKeyId", "SignatureDoesNotMatch", "InvalidClientTokenId"}:
            print(f"⚠️ S3 explicit-key failure ({error_code}), retrying with IAM chain")
            boto3.client("s3", region_name=AWS_REGION).upload_fileobj(
                io.BytesIO(file_bytes), S3_BUCKET, s3_key, ExtraArgs={"ContentType": ct})
        else:
            raise
    url = f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
    print(f"S3 upload OK: {url}")
    return url


def build_letter_download_url(s3_key: str) -> str:
    """Always generate a fresh presigned URL — never cache these."""
    try:
        return s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET, "Key": s3_key},
            ExpiresIn=LETTER_LINK_EXPIRY_SECONDS,
        )
    except Exception as exc:
        print(f"⚠️ Could not generate pre-signed URL: {exc}")
        return f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"


# ============================================================
# EMAIL HELPERS
# ============================================================
def send_selection_email(to_email, full_name, intern_id, domain, start_date, end_date, letter_download_url):
    if not SELECTION_MAIL_ENABLED:
        print("[mail] SELECTION_MAIL_ENABLED=false; skipping")
        return
    if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASSWORD and SMTP_FROM):
        raise ValueError("SMTP config incomplete")

    html_body = f"""<html><body style="font-family:Arial,sans-serif;line-height:1.6;color:#222;">
        <p>Dear {full_name},</p>
        <p>Congratulations! You have been selected for the <b>{domain}</b> Internship at <b>ChakoraHub</b>.</p>
        <p><b>Internship ID:</b> {intern_id}<br/><b>Start:</b> {start_date}<br/><b>End:</b> {end_date}</p>
        <p><a href="{letter_download_url}">Download Selection Letter</a></p>
        <p>Regards,<br/>Team ChakoraHub</p>
    </body></html>"""
    text_body = (f"Dear {full_name},\nCongratulations! Selected for {domain} Internship.\n"
                 f"ID: {intern_id}\nStart: {start_date}\nEnd: {end_date}\n"
                 f"Letter: {letter_download_url}\nRegards,\nTeam ChakoraHub")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Internship Selection Confirmation - {intern_id}"
    msg["From"]    = SMTP_FROM
    msg["To"]      = to_email
    msg.attach(MIMEText(text_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html",  "utf-8"))
    server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20)
    try:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)
        print(f"✅ Selection email → {to_email} for {intern_id}")
    finally:
        server.quit()


def send_application_ack_email(to_email, full_name, intern_id, domain):
    if not APPLICATION_MAIL_ENABLED:
        print("[mail] APPLICATION_MAIL_ENABLED=false; skipping")
        return
    if not to_email:
        print("[mail] No applicant email; skipping")
        return
    subject   = f"Application Received - Internship ID {intern_id}"
    html_body = f"""<html><body style="margin:0;padding:0;background:#f5f7fb;font-family:Segoe UI,Arial,sans-serif;">
        <table width="100%" style="padding:24px 12px;"><tr><td align="center">
        <table width="620" style="max-width:620px;background:#fff;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden;">
        <tr><td style="padding:16px 24px;background:linear-gradient(135deg,#1d4ed8,#4f46e5);color:#fff;font-size:20px;font-weight:700;">ChakoraHub Internship</td></tr>
        <tr><td style="padding:24px;font-size:15px;line-height:1.65;">
            <p>Dear {full_name},</p>
            <p>Thank you for applying. Your application has been received.</p>
            <p><strong>Internship ID:</strong> {intern_id}<br/><strong>Domain:</strong> {domain or 'Not specified'}</p>
            <p>Our team will contact you with next steps.<br/>Regards,<br/><strong>Team ChakoraHub</strong></p>
        </td></tr></table></td></tr></table></body></html>"""
    text_body = (f"Dear {full_name},\nThank you for applying.\n"
                 f"Internship ID: {intern_id}\nDomain: {domain or 'Not specified'}\n"
                 "We will contact you shortly.\nRegards,\nTeam ChakoraHub")

    destination = {"ToAddresses": [to_email]}
    if APPLICATION_MAIL_CC and APPLICATION_MAIL_CC.lower() != to_email.lower():
        destination["CcAddresses"] = [APPLICATION_MAIL_CC]
    payload = {"Source": APPLICATION_MAIL_FROM, "Destination": destination,
               "Message": {"Subject": {"Data": subject, "Charset": "UTF-8"},
                            "Body":    {"Text": {"Data": text_body, "Charset": "UTF-8"},
                                        "Html": {"Data": html_body, "Charset": "UTF-8"}}}}
    try:
        ses.send_email(**payload)
    except ClientError as exc:
        code = (exc.response or {}).get("Error", {}).get("Code", "")
        if using_explicit_aws_keys and code in {"InvalidAccessKeyId","SignatureDoesNotMatch","InvalidClientTokenId"}:
            boto3.client("ses", region_name=SES_REGION).send_email(**payload)
        else:
            raise
    print(f"✅ Ack email → {to_email} for {intern_id}")


def send_application_decision_email(to_email, full_name, intern_id, domain, status):
    if not to_email:
        print("[mail] No email; skipping decision mail")
        return
    normalized = str(status or "").strip().upper()
    if normalized not in {"APPROVED", "REJECTED"}:
        raise ValueError("status must be APPROVED or REJECTED")
    status_line = "approved" if normalized == "APPROVED" else "rejected"
    subject     = f"Internship Application {normalized.title()} - {intern_id}"
    html_body   = f"""<html><body style="margin:0;padding:0;background:#f5f7fb;font-family:Segoe UI,Arial,sans-serif;">
        <table width="100%" style="padding:24px 12px;"><tr><td align="center">
        <table width="620" style="max-width:620px;background:#fff;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden;">
        <tr><td style="padding:16px 24px;background:linear-gradient(135deg,#1d4ed8,#4f46e5);color:#fff;font-size:20px;font-weight:700;">ChakoraHub Internship</td></tr>
        <tr><td style="padding:24px;font-size:15px;line-height:1.65;">
            <p>Dear {full_name},</p>
            <p>Your application has been <strong>{status_line}</strong>.</p>
            <p><strong>Internship ID:</strong> {intern_id}<br/><strong>Domain:</strong> {domain or 'Not specified'}</p>
            <p>Contact admin@chakorahub.com for clarification.<br/>Regards,<br/><strong>Team ChakoraHub</strong></p>
        </td></tr></table></td></tr></table></body></html>"""
    text_body = (f"Dear {full_name},\nYour application has been {status_line}.\n"
                 f"ID: {intern_id}\nDomain: {domain or 'Not specified'}\n"
                 "Contact admin@chakorahub.com.\nRegards,\nTeam ChakoraHub")
    destination = {"ToAddresses": [to_email]}
    if APPLICATION_MAIL_CC and APPLICATION_MAIL_CC.lower() != to_email.lower():
        destination["CcAddresses"] = [APPLICATION_MAIL_CC]
    payload = {"Source": APPLICATION_MAIL_FROM, "Destination": destination,
               "Message": {"Subject": {"Data": subject, "Charset": "UTF-8"},
                            "Body":    {"Text": {"Data": text_body, "Charset": "UTF-8"},
                                        "Html": {"Data": html_body, "Charset": "UTF-8"}}}}
    try:
        ses.send_email(**payload)
    except ClientError as exc:
        code = (exc.response or {}).get("Error", {}).get("Code", "")
        if using_explicit_aws_keys and code in {"InvalidAccessKeyId","SignatureDoesNotMatch","InvalidClientTokenId"}:
            boto3.client("ses", region_name=SES_REGION).send_email(**payload)
        else:
            raise
    print(f"✅ Decision email → {to_email} for {intern_id} status={normalized}")


def send_internship_certificate_email(to_email, full_name, intern_id, domain, duration, start_date):
    """Send internship certificate confirmation email via SES."""
    if not to_email:
        raise ValueError("Missing recipient email")

    def _parse_date_value(value):
        if value is None:
            return None
        if hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day"):
            return value
        raw = str(value).strip()
        if not raw:
            return None
        candidates = [raw, raw.replace(" ", "T", 1), raw.split(" ", 1)[0]]
        for c in candidates:
            try:
                return datetime.fromisoformat(c)
            except Exception:
                continue
        return None

    def _format_date_value(value):
        parsed_date = _parse_date_value(value)
        if parsed_date is not None:
            return parsed_date.strftime("%d %B %Y")
        if value is None:
            return "N/A"

        return str(value)

    def _password_from_start_date(value):
        parsed_date = _parse_date_value(value)
        if parsed_date is not None:
            return parsed_date.strftime("%Y%m%d")

        raw = "" if value is None else str(value)
        digits = "".join(ch for ch in raw if ch.isdigit())
        if len(digits) >= 8:
            return digits[:8]

        raise ValueError("Could not derive certificate password from start_date")

    def _build_certificate_pdf_bytes(pdf_password):
        def _load_image_reader(file_name: str, fallback_url: str):
            base_dir = pathlib.Path(__file__).resolve().parent
            candidate_paths = [
                base_dir / "static" / "images" / file_name,
                base_dir.parent / "Website-Git" / "static" / "images" / file_name,
                pathlib.Path.cwd() / "static" / "images" / file_name,
            ]

            for p in candidate_paths:
                if p.exists():
                    try:
                        return ImageReader(str(p))
                    except Exception:
                        pass

            try:
                with urllib.request.urlopen(fallback_url, timeout=10) as resp:
                    return ImageReader(io.BytesIO(resp.read()))
            except Exception:
                return None

        def _draw_center_lines(canvas_obj, text, y_start, max_width, font_name, font_size, line_gap, color):
            words = (text or "").split()
            lines = []
            current = ""
            for word in words:
                test = f"{current} {word}".strip()
                if canvas_obj.stringWidth(test, font_name, font_size) <= max_width:
                    current = test
                else:
                    if current:
                        lines.append(current)
                    current = word
            if current:
                lines.append(current)

            canvas_obj.setFillColor(color)
            canvas_obj.setFont(font_name, font_size)
            y = y_start
            for line in lines:
                canvas_obj.drawCentredString(A4[0] / 2, y, line)
                y -= line_gap
            return y

        buffer = io.BytesIO()
        encryption = StandardEncryption(
            userPassword=pdf_password,
            ownerPassword=pdf_password,
            canPrint=1,
            canCopy=0,
            canModify=0,
            canAnnotate=0,
            strength=128,
        )
        c = pdf_canvas.Canvas(buffer, pagesize=A4, encrypt=encryption)
        page_w, page_h = A4

        primary = colors.HexColor("#4f46e5")
        primary_dark = colors.HexColor("#273449")
        muted = colors.HexColor("#64748b")

        # Frame
        outer_margin = 8 * mm
        inner_margin = 12 * mm
        c.setStrokeColor(primary)
        c.setLineWidth(3)
        c.rect(outer_margin, outer_margin, page_w - 2 * outer_margin, page_h - 2 * outer_margin)
        c.setLineWidth(1.2)
        c.rect(inner_margin, inner_margin, page_w - 2 * inner_margin, page_h - 2 * inner_margin)

        # Corner accents
        corner_len = 10 * mm
        c.setStrokeColor(primary)
        c.setLineWidth(1.3)
        # TL
        c.line(inner_margin + 2, page_h - inner_margin - 2, inner_margin + corner_len, page_h - inner_margin - 2)
        c.line(inner_margin + 2, page_h - inner_margin - 2, inner_margin + 2, page_h - inner_margin - corner_len)
        # TR
        c.line(page_w - inner_margin - 2, page_h - inner_margin - 2, page_w - inner_margin - corner_len, page_h - inner_margin - 2)
        c.line(page_w - inner_margin - 2, page_h - inner_margin - 2, page_w - inner_margin - 2, page_h - inner_margin - corner_len)
        # BL
        c.line(inner_margin + 2, inner_margin + 2, inner_margin + corner_len, inner_margin + 2)
        c.line(inner_margin + 2, inner_margin + 2, inner_margin + 2, inner_margin + corner_len)
        # BR
        c.line(page_w - inner_margin - 2, inner_margin + 2, page_w - inner_margin - corner_len, inner_margin + 2)
        c.line(page_w - inner_margin - 2, inner_margin + 2, page_w - inner_margin - 2, inner_margin + corner_len)

        # Certificate ref
        c.setFont("Helvetica-Bold", 9)
        c.setFillColor(colors.HexColor("#94a3b8"))
        c.drawRightString(page_w - inner_margin - 4, page_h - inner_margin - 10, f"Certificate Ref: {intern_id}")

        # Header logo/brand
        logo_img = _load_image_reader("logo.png", "https://www.chakorahub.com/static/images/logo.png")
        if logo_img:
            logo_w = 36 * mm
            logo_h = 30 * mm
            c.drawImage(logo_img, (page_w - logo_w) / 2, page_h - inner_margin - logo_h - 16, width=logo_w, height=logo_h, preserveAspectRatio=True, mask='auto')
        else:
            print("⚠️ WARNING: logo.png could not be loaded. Logo will be missing from the certificate.")

        c.setFillColor(primary)
        c.setFont("Helvetica-Bold", 14)
        c.drawCentredString(page_w / 2, page_h - inner_margin - 84, "CHAKORAHUB")

        c.setStrokeColor(primary)
        c.setLineWidth(1)
        c.line(page_w / 2 - 30, page_h - inner_margin - 90, page_w / 2 + 30, page_h - inner_margin - 90)

        # Title block
        c.setFillColor(primary_dark)
        c.setFont("Helvetica-Bold", 33)
        c.drawCentredString(page_w / 2, page_h - inner_margin - 132, "Certificate of Completion")

        c.setFillColor(muted)
        c.setFont("Helvetica", 14)
        c.drawCentredString(page_w / 2, page_h - inner_margin - 160, "This certificate is proudly presented to")

        c.setFillColor(colors.HexColor("#5b54e2"))
        c.setFont("Helvetica-Bold", 29)
        c.drawCentredString(page_w / 2, page_h - inner_margin - 198, str(full_name or "Intern"))

        c.setStrokeColor(colors.HexColor("#e2e8f0"))
        c.setLineWidth(1)
        c.line(22 * mm, page_h - inner_margin - 208, page_w - 22 * mm, page_h - inner_margin - 208)

        # Body content
        body_text = (
            f"For successfully completing the internship in {domain_text} at ChakoraHub, "
            f"for {duration_text} starting from {start_date_text}."
        )
        body_y_end = _draw_center_lines(
            c,
            body_text,
            page_h - inner_margin - 244,
            max_width=page_w - 56 * mm,
            font_name="Helvetica-Bold",
            font_size=12,
            line_gap=18,
            color=primary_dark,
        )

        # Meta center
        meta_y = body_y_end - 34
        c.setFillColor(colors.HexColor("#586d8c"))
        c.setFont("Helvetica", 11)
        c.drawCentredString(page_w / 2, meta_y, f"Internship ID: {intern_id}")
        c.drawCentredString(page_w / 2, meta_y - 18, f"Issued On: {datetime.now().strftime('%d %B %Y')}")

        # Footer baseline blocks (left, center seal, right signature)
        footer_line_y = 32 * mm

        # Left block
        left_x = 22 * mm
        c.setStrokeColor(colors.HexColor("#9fb3cf"))
        c.setLineWidth(1)
        c.line(left_x, footer_line_y + 12, left_x + 58 * mm, footer_line_y + 12)
        c.setFillColor(primary_dark)
        c.setFont("Helvetica-Bold", 11)
        c.drawString(left_x + 16, footer_line_y - 2, datetime.now().strftime('%d %B %Y'))
        c.setFillColor(muted)
        c.setFont("Helvetica", 9)
        c.drawString(left_x + 16, footer_line_y - 16, "DATE OF ISSUE")

        c.setFillColor(colors.HexColor("#2f3a4c"))
        c.setFont("Helvetica-Bold", 9.5)
        c.drawString(left_x, footer_line_y - 33, "Chakora Hub Pvt. Ltd.")
        c.setFillColor(muted)
        c.setFont("Helvetica", 8.7)
        c.drawString(left_x, footer_line_y - 45, "Innovation • Education • Technology")
        c.setFillColor(primary)
        c.drawString(left_x, footer_line_y - 57, "www.chakorahub.com")

        # Center seal
        center_x = page_w / 2
        c.setFillColor(primary)
        c.circle(center_x, footer_line_y + 8, 14 * mm, stroke=0, fill=1)
        c.setFillColor(colors.white)
        c.setFont("Helvetica-Bold", 14)
        c.drawCentredString(center_x, footer_line_y + 4, "*")
        c.setFillColor(muted)
        c.setFont("Helvetica-Bold", 9)
        c.drawCentredString(center_x, footer_line_y - 16, "OFFICIAL SEAL")

        # Right signatory
        right_x = page_w - 70 * mm
        sign_img = _load_image_reader("Sign.png", "https://www.chakorahub.com/static/images/Sign.png")
        if sign_img:
            c.drawImage(sign_img, right_x + 20 * mm, footer_line_y + 12, width=32 * mm, height=14 * mm, preserveAspectRatio=True, mask='auto')
        else:
            print("⚠️ WARNING: Sign.png could not be loaded. Signature will be missing from the certificate.")

        c.setFillColor(primary_dark)
        c.setFont("Helvetica-Bold", 11)
        c.drawString(right_x + 10 * mm, footer_line_y - 2, "Subhash Chandra")
        c.setFont("Helvetica", 10.5)
        c.drawString(right_x + 10 * mm, footer_line_y - 15, "Founder & Director")
        c.drawString(right_x + 10 * mm, footer_line_y - 28, "Authorized Signatory")

        c.showPage()
        c.save()
        buffer.seek(0)
        return buffer.read()

    subject = f"Internship Certificate - {intern_id}"
    
    # Gracefully fallback if start_date is missing to prevent ValueError and 500 server crash
    parsed_start_date = _parse_date_value(start_date)
    if parsed_start_date is None:
        print(f"⚠️ WARNING: start_date is missing or invalid for {intern_id}. Defaulting to current date.")
        parsed_start_date = datetime.now()
        
    start_date_text = parsed_start_date.strftime("%d %B %Y")
    pdf_password = parsed_start_date.strftime("%Y%m%d")
    
    duration_text = str(duration or "N/A")
    domain_text = str(domain or "Not specified")

    html_body = f"""<html><body style="margin:0;padding:0;background:#f5f7fb;font-family:Segoe UI,Arial,sans-serif;">
        <table width="100%" style="padding:24px 12px;"><tr><td align="center">
        <table width="620" style="max-width:620px;background:#fff;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden;">
        <tr><td style="padding:16px 24px;background:linear-gradient(135deg,#1d4ed8,#4f46e5);color:#fff;font-size:20px;font-weight:700;">ChakoraHub Internship</td></tr>
        <tr><td style="padding:24px;font-size:15px;line-height:1.65;">
            <p>Dear {full_name},</p>
            <p>Your internship certificate has been generated successfully.</p>
            <p>
                <strong>Internship ID:</strong> {intern_id}<br/>
                <strong>Domain:</strong> {domain_text}<br/>
                <strong>Duration:</strong> {duration_text}<br/>
                <strong>Start Date:</strong> {start_date_text}
            </p>
            <p><strong>PDF Password Hint:</strong> Use your internship start date in <strong>YYYYMMDD</strong> format.<br/>
            <strong>Password:</strong> {pdf_password}</p>
            <p>Please contact admin@chakorahub.com if you need any help.</p>
            <p>Regards,<br/><strong>Team ChakoraHub</strong></p>
        </td></tr></table></td></tr></table></body></html>"""

    text_body = (
        f"Dear {full_name},\n"
        "Your internship certificate has been generated successfully.\n"
        f"Internship ID: {intern_id}\n"
        f"Domain: {domain_text}\n"
        f"Duration: {duration_text}\n"
        f"Start Date: {start_date_text}\n"
        "PDF Password Hint: Use your internship start date in YYYYMMDD format.\n"
        f"Password: {pdf_password}\n"
        "Please contact admin@chakorahub.com if you need any help.\n"
        "Regards,\nTeam ChakoraHub"
    )

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = APPLICATION_MAIL_FROM
    msg["To"] = to_email

    destinations = [to_email]
    if APPLICATION_MAIL_CC and APPLICATION_MAIL_CC.lower() != to_email.lower():
        msg["Cc"] = APPLICATION_MAIL_CC
        destinations.append(APPLICATION_MAIL_CC)

    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(text_body, "plain", "utf-8"))
    alt.attach(MIMEText(html_body, "html", "utf-8"))
    msg.attach(alt)

    pdf_bytes = _build_certificate_pdf_bytes(pdf_password)
    attachment = MIMEApplication(pdf_bytes, _subtype="pdf")
    attachment.add_header("Content-Disposition", "attachment", filename=f"Internship_Certificate_{intern_id}.pdf")
    msg.attach(attachment)

    raw_data = msg.as_string().encode("utf-8")

    try:
        if ses is not None:
            ses.send_raw_email(
                Source=APPLICATION_MAIL_FROM,
                Destinations=destinations,
                RawMessage={"Data": raw_data},
            )
            print(f"✅ Certificate email sent via SES → {to_email} for {intern_id}")
        else:
            raise NoCredentialsError()
    except (ClientError, NoCredentialsError, Exception) as exc:
        is_cred_error = False
        if isinstance(exc, NoCredentialsError):
            is_cred_error = True
        elif isinstance(exc, ClientError):
            code = (exc.response or {}).get("Error", {}).get("Code", "")
            if code in {"InvalidAccessKeyId", "SignatureDoesNotMatch", "InvalidClientTokenId", "AccessDenied"}:
                is_cred_error = True
        elif "Unable to locate credentials" in str(exc):
            is_cred_error = True

        if is_cred_error:
            print("⚠️ AWS credentials not located or invalid. Falling back to Local Dev Mock email dispatch.")
            print("\n" + "="*50)
            print("💾 LOCAL DEV MOCK EMAIL DISPATCH (SES)")
            print("="*50)
            print(f"To: {to_email}")
            print(f"Subject: {subject}")
            print(f"Attachment size: {len(pdf_bytes)} bytes")
            print("Status: MOCKED SUCCESS (no AWS credentials configured)")
            print("="*50 + "\n")
        else:
            print(f"❌ SES dispatch failed: {exc}")
            raise


def json_error(message, status_code):
    return JSONResponse(status_code=status_code, content={"success": False, "message": message})


# ============================================================
# ORACLE CONNECTION
# ============================================================
def get_db_connection():
    try:
        dsn = oracledb.makedsn(
            host=ORACLE_HOST,
            port=ORACLE_PORT,
            service_name=ORACLE_SERVICE_NAME,
        )
        conn = oracledb.connect(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=dsn,
        )
        cur = conn.cursor()
        cur.execute("ALTER SESSION SET CURRENT_SCHEMA = CHAKORA")
        cur.close()
        print("✅ Oracle connected")
        return conn
    except Exception as exc:
        print(f"❌ DB Connection Error: {exc}")
        import traceback; traceback.print_exc()
        return None


def generate_intern_id(conn):
    yy     = datetime.now().strftime("%y")
    prefix = f"CH{yy}I"
    cur    = conn.cursor()
    cur.execute(
        "SELECT COALESCE(MAX(TO_NUMBER(SUBSTR(INTERN_ID, :1))), 0) "
        "FROM NRM_INTERNSHIP_APPLICATIONS WHERE INTERN_ID LIKE :2",
        (len(prefix) + 1, f"{prefix}%"))
    row = cur.fetchone()
    cur.close()
    seq = (row[0] if row else 0) + 1
    if seq > 99:
        raise ValueError("Maximum 99 internship slots for this year are filled")
    return f"{prefix}{str(seq).zfill(2)}"


def lookup_user_id_by_email(conn, email: str):
    """Resolve NRM_USERS.ID by email so user-scoped internship cache can be invalidated."""
    normalized_email = (email or "").strip()
    if not normalized_email:
        return None

    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT ID FROM NRM_USERS WHERE LOWER(EMAIL) = LOWER(:1) ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY",
            (normalized_email,),
        )
        row = cur.fetchone()
        return row[0] if row else None
    except Exception as exc:
        print(f"⚠️  user_id lookup failed for internship cache eviction [{normalized_email}]: {exc}")
        return None
    finally:
        cur.close()


# ============================================================
# POST /api/internship/apply
# ============================================================
@app.post("/api/internship/apply")
async def apply_internship(request: Request):

   
    """
    Submit an internship application.
    Evicts internship:applications* so the admin list stays fresh.
    """
    if is_maintenance_enabled():
        return json_error("Internship applications are temporarily unavailable due to maintenance.", 503)

    conn = None
    try:
        form  = await request.form()
        files = {k: v for k, v in form.items() if hasattr(v, "filename")}

        print("=" * 50)
        print("📥 Internship application received")
        print(f"Form keys: {list(form.keys())}")
        print(f"File keys: {list(files.keys())}")

        def pick(*keys):
            for k in keys:
                v = form.get(k)
                if v:
                    t = str(v).strip()
                    if t:
                        return t
            return ""

        full_name           = pick("full_name")
        email               = pick("email")
        mobile              = pick("mobile", "phone")
        date_of_birth       = pick("date_of_birth", "dob")
        gender              = pick("gender")
        address             = pick("address")
        college_name        = pick("college_name", "college")
        branch              = pick("branch")
        year_of_study       = pick("year_of_study", "year")
        cgpa                = pick("cgpa")
        graduation_year     = pick("graduation_year")
        college_id          = pick("college_id", "college_roll_number", "roll_number")
        hod_name            = pick("hod_name", "tpo_hod_name", "tpo_name")
        hod_contact         = pick("hod_contact", "tpo_hod_contact", "tpo_contact")
        internship_domain   = pick("internship_domain", "area_of_interest")
        internship_duration = pick("internship_duration", "duration")
        start_date          = pick("start_date")
        mode                = pick("mode", "internship_mode", "mode_of_internship")
        why_chakora         = pick("why_chakora")
        skills              = pick("skills")
        portfolio_url       = pick("portfolio_url", "portfolio")

        tpo_parts = []
        if hod_name:    tpo_parts.append(f"Name: {hod_name}")
        if hod_contact: tpo_parts.append(f"Contact: {hod_contact}")
        tpo_contact = " | ".join(tpo_parts)

        required = {"full_name": full_name, "email": email, "mobile": mobile,
                    "college_name": college_name, "branch": branch,
                    "year_of_study": year_of_study, "graduation_year": graduation_year,
                    "college_id": college_id, "internship_domain": internship_domain,
                    "internship_duration": internship_duration, "mode": mode}
        missing = [f for f, v in required.items() if not v]
        if missing:
            return json_error(f"Missing fields: {', '.join(missing)}", 400)

        for fkey in ["resume", "id_card", "noc"]:
            if fkey not in files or not files[fkey].filename:
                return json_error(f"Missing file: {fkey}", 400)
            if not allowed_file(files[fkey].filename):
                return json_error(f"Invalid file type for {fkey}. Allowed: PDF, JPG, PNG", 400)

        conn = get_db_connection()
        if not conn:
            return json_error("Database connection failed", 500)

        intern_id = generate_intern_id(conn)
        print(f"📝 Generated intern ID: {intern_id}")

        try:
            resume_url = upload_to_s3(files["resume"],  "resume",  intern_id)
            idcard_url = upload_to_s3(files["id_card"], "id_card", intern_id)
            noc_url    = upload_to_s3(files["noc"],     "noc",     intern_id)
            print("✅ All files uploaded to S3")
        except NoCredentialsError:
            return json_error("AWS credentials missing. Set keys in .env or attach IAM role.", 500)
        except ClientError as exc:
            error_code  = exc.response.get("Error", {}).get("Code", "")
            http_status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 500)
            if http_status == 403 or error_code == "AccessDenied":
                return json_error("S3 AccessDenied (403). Check bucket policy/IAM role.", 403)
            return json_error(f"S3 upload error ({error_code or 'unknown'}).", 500)
        except Exception as exc:
            return json_error(f"File upload failed: {exc}", 500)

        cur = conn.cursor()
        try:
            cur.execute(
                """INSERT INTO NRM_INTERNSHIP_APPLICATIONS (
                    INTERN_ID, SUBMITTED_AT, FULL_NAME, EMAIL, MOBILE, DATE_OF_BIRTH, GENDER,
                    ADDRESS, COLLEGE_NAME, BRANCH, YEAR_OF_STUDY, CGPA, GRADUATION_YEAR,
                    INTERNSHIP_DOMAIN, INTERNSHIP_DURATION, START_DATE,
                    WHY_CHAKORA, SKILLS, PORTFOLIO_URL, STATUS,
                    COLLEGE_ID, TPO_CONTACT, INTERN_MODE
                ) VALUES (
                    :1, CURRENT_TIMESTAMP, :2, :3, :4, TO_DATE(NULLIF(:5,''), 'YYYY-MM-DD'),
                    NULLIF(:6,''), NULLIF(:7,''), :8, :9, :10, NULLIF(:11,''), :12,
                    :13, :14, TO_DATE(NULLIF(:15,''), 'YYYY-MM-DD'),
                    NULLIF(:16,''), NULLIF(:17,''), NULLIF(:18,''), 'PENDING',
                    :19, NULLIF(:20,''), :21
                )""",
                (intern_id, full_name, email, mobile, date_of_birth,
                 gender, address, college_name, branch, year_of_study,
                 cgpa, graduation_year, internship_domain, internship_duration,
                 start_date, why_chakora, skills, portfolio_url,
                 college_id, tpo_contact, mode),
            )
            for doc_type, url, orig in [
                ("RESUME",  resume_url, files["resume"].filename),
                ("ID_CARD", idcard_url, files["id_card"].filename),
                ("NOC",     noc_url,    files["noc"].filename),
            ]:
                cur.execute(
                    "INSERT INTO NRM_INTERNSHIP_DOCUMENTS (DOC_ID, INTERN_ID, DOC_TYPE, S3_URL, ORIGINAL_NAME) "
                    "VALUES ((SELECT COALESCE(MAX(DOC_ID), 0) + 1 FROM NRM_INTERNSHIP_DOCUMENTS), :1, :2, :3, :4)",
                    (intern_id, doc_type, url, secure_filename(orig)))
            conn.commit()
            print(f"✅ Application saved: {intern_id}")

            # ── Evict stale admin-list cache ──────────────────────────
            _evict_on_write(user_id=lookup_user_id_by_email(conn, email))

            email_sent  = False
            email_error = None
            try:
                send_application_ack_email(to_email=email, full_name=full_name,
                                           intern_id=intern_id, domain=internship_domain)
                email_sent = True
            except Exception as mail_exc:
                email_error = str(mail_exc)
                print(f"⚠️ Ack email failed for {intern_id}: {email_error}")

            return {"success": True, "intern_id": intern_id,
                    "email_sent": email_sent, "email_error": email_error,
                    "message": f"Application submitted! Your Internship ID is {intern_id}"}

        except Exception as exc:
            if conn: conn.rollback()
            import traceback; traceback.print_exc()
            return json_error(f"Database error: {exc}", 500)
        finally:
            cur.close()

    except Exception as exc:
        import traceback; traceback.print_exc()
        return json_error(f"Server error: {exc}", 500)
    finally:
        if conn:
            conn.close()
            print("🔌 DB connection closed")


# ============================================================
# POST /api/internship/admin/select
# ============================================================
@app.post("/api/internship/admin/select")
async def select_intern(request: Request):
    """
    Approve an application and generate the selection letter PDF.
    Evicts internship:applications* after commit.
    Body: {"intern_id": "CH26I01", "start_date": "1 May 2026", "end_date": "31 July 2026"}
    """
    conn = None
    try:
        data       = await request.json()
        intern_id  = str(data.get("intern_id",  "")).strip()
        start_date = str(data.get("start_date", "")).strip()
        end_date   = str(data.get("end_date",   "")).strip()
        send_email = str(data.get("send_email", "true")).strip().lower() in {"1","true","yes","on"}

        if not intern_id or not start_date or not end_date:
            return json_error("intern_id, start_date, and end_date are required", 400)

        conn = get_db_connection()
        if not conn:
            return json_error("Database connection failed", 500)

        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT INTERN_ID, FULL_NAME, EMAIL, MOBILE, COLLEGE_NAME, BRANCH, "
                "       INTERNSHIP_DOMAIN, INTERNSHIP_DURATION, STATUS "
                "FROM NRM_INTERNSHIP_APPLICATIONS WHERE INTERN_ID = :1",
                (intern_id,))
            row = cur.fetchone()
            if not row:
                return json_error(f"No application found with intern_id={intern_id}", 404)

            intern_data = {
                "intern_id": row[0], "full_name": row[1], "email": row[2],
                "mobile": row[3], "college_name": row[4], "branch": row[5],
                "internship_domain": row[6], "internship_duration": row[7], "status": row[8],
            }

            print(f"📄 Generating letter for {intern_id} — {intern_data['full_name']}")
            pdf_bytes = generate_selection_letter_pdf(intern_data, start_date, end_date)

            s3_key = f"internships/{intern_id}/letters/selection_letter.pdf"
            s3.upload_fileobj(io.BytesIO(pdf_bytes), S3_BUCKET, s3_key,
                              ExtraArgs={"ContentType": "application/pdf"})
            letter_url          = f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
            letter_download_url = build_letter_download_url(s3_key)
            print(f"✅ Letter uploaded: {letter_url}")

            cur.execute("UPDATE NRM_INTERNSHIP_APPLICATIONS SET STATUS = 'SELECTED' "
                        "WHERE INTERN_ID = :1", (intern_id,))
            cur.execute("DELETE FROM NRM_INTERNSHIP_DOCUMENTS "
                        "WHERE INTERN_ID = :1 AND DOC_TYPE = 'SELECTION_LETTER'", (intern_id,))
            cur.execute("INSERT INTO NRM_INTERNSHIP_DOCUMENTS "
                        "(DOC_ID, INTERN_ID, DOC_TYPE, S3_URL, ORIGINAL_NAME) "
                        "VALUES ((SELECT COALESCE(MAX(DOC_ID), 0) + 1 FROM NRM_INTERNSHIP_DOCUMENTS), :1, 'SELECTION_LETTER', :2, 'selection_letter.pdf')",
                        (intern_id, letter_url))
            conn.commit()
            print(f"✅ {intern_id} marked SELECTED")

            # ── Cache eviction ────────────────────────────────────────
            _evict_on_write(user_id=lookup_user_id_by_email(conn, intern_data["email"]))

            email_sent  = False
            email_error = None
            if send_email:
                try:
                    send_selection_email(
                        to_email=intern_data["email"], full_name=intern_data["full_name"],
                        intern_id=intern_id, domain=intern_data["internship_domain"],
                        start_date=start_date, end_date=end_date,
                        letter_download_url=letter_download_url)
                    email_sent = True
                except Exception as mail_exc:
                    email_error = str(mail_exc)
                    print(f"⚠️ Selection email failed for {intern_id}: {email_error}")

            return JSONResponse(content={
                "success": True, "intern_id": intern_id,
                "full_name": intern_data["full_name"], "status": "SELECTED",
                "letter_url": letter_url, "letter_download_url": letter_download_url,
                "email_sent": email_sent, "email_error": email_error,
                "message": f"Selection letter generated for {intern_data['full_name']}",
            })

        except Exception as exc:
            if conn: conn.rollback()
            import traceback; traceback.print_exc()
            return json_error(f"Error: {exc}", 500)
        finally:
            cur.close()

    except Exception as exc:
        return json_error(f"Server error: {exc}", 500)
    finally:
        if conn: conn.close()


# ============================================================
# POST /api/internship/admin/status
# ============================================================
@app.post("/api/internship/admin/status")
async def update_intern_status(request: Request):
    """
    Update status to APPROVED or REJECTED and notify applicant.
    Evicts internship:applications* after commit.
    """
    conn = None
    try:
        data       = await request.json()
        intern_id  = str(data.get("intern_id", "")).strip()
        status     = str(data.get("status",    "")).strip().upper()
        send_email = str(data.get("send_email", "true")).strip().lower() in {"1","true","yes","on"}

        if not intern_id or status not in {"APPROVED", "REJECTED"}:
            return json_error("intern_id and valid status (APPROVED/REJECTED) are required", 400)

        conn = get_db_connection()
        if not conn:
            return json_error("Database connection failed", 500)

        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT INTERN_ID, FULL_NAME, EMAIL, INTERNSHIP_DOMAIN, STATUS "
                "FROM NRM_INTERNSHIP_APPLICATIONS WHERE INTERN_ID = :1", (intern_id,))
            row = cur.fetchone()
            if not row:
                return json_error(f"No application found with intern_id={intern_id}", 404)

            cur.execute(
                "UPDATE NRM_INTERNSHIP_APPLICATIONS "
                "SET STATUS = :1, REVIEWED_AT = CURRENT_TIMESTAMP, REVIEW_REASON = NULL "
                "WHERE INTERN_ID = :2", (status, intern_id))
            conn.commit()

            # ── Cache eviction ────────────────────────────────────────
            _evict_on_write(user_id=lookup_user_id_by_email(conn, row[2]))

            email_sent  = False
            email_error = None
            if send_email:
                try:
                    send_application_decision_email(
                        to_email=row[2], full_name=row[1],
                        intern_id=intern_id, domain=row[3], status=status)
                    email_sent = True
                except Exception as mail_exc:
                    email_error = str(mail_exc)
                    print(f"⚠️ Decision email failed for {intern_id}: {email_error}")

            return JSONResponse(content={
                "success": True, "intern_id": intern_id, "status": status,
                "email_sent": email_sent, "email_error": email_error,
                "message": f"Application status updated to {status}",
            })

        except Exception as exc:
            if conn: conn.rollback()
            import traceback; traceback.print_exc()
            return json_error(f"Error: {exc}", 500)
        finally:
            cur.close()

    except Exception as exc:
        return json_error(f"Server error: {exc}", 500)
    finally:
        if conn: conn.close()


# ============================================================
# POST /api/internship/send-certificate
# ============================================================
@app.post("/api/internship/send-certificate")
async def send_certificate(request: Request):
    """Send internship certificate email to the approved intern."""
    conn = None
    try:
        data = await request.json()
        intern_id = str(data.get("intern_id", "")).strip()
        if not intern_id:
            return json_error("intern_id is required", 400)

        conn = get_db_connection()
        if not conn:
            return json_error("Database connection failed", 500)

        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT INTERN_ID, FULL_NAME, EMAIL, INTERNSHIP_DOMAIN, INTERNSHIP_DURATION, START_DATE, STATUS "
                "FROM NRM_INTERNSHIP_APPLICATIONS WHERE INTERN_ID = :1",
                (intern_id,),
            )
            row = cur.fetchone()
            if not row:
                return json_error(f"No application found with intern_id={intern_id}", 404)

            status = str(row[6] or "").strip().upper()
            if status != "APPROVED":
                return json_error("Certificate email can be sent only for APPROVED applications", 400)

            full_name = row[1] or "Intern"
            email = row[2]
            domain = row[3]
            duration = row[4]
            start_date = row[5]

            send_internship_certificate_email(
                to_email=email,
                full_name=full_name,
                intern_id=intern_id,
                domain=domain,
                duration=duration,
                start_date=start_date,
            )

            return JSONResponse(content={
                "success": True,
                "intern_id": intern_id,
                "email": email,
                "message": f"Certificate email sent to {email}",
            })
        finally:
            cur.close()

    except Exception as exc:
        print(f"❌ send_certificate error: {exc}")
        return json_error(f"Server error: {exc}", 500)
    finally:
        if conn:
            conn.close()


# ============================================================
# GET /api/internship/admin/applications  (cached)
# Cache keys:
#   internship:applications           — all applications (no filter)
#   internship:applications:{STATUS}  — filtered by status
# Both evicted by _evict_on_write() on every mutation.
# ============================================================
@app.get("/api/internship/admin/applications")
async def list_applications(status: str = None):
    """
    List applications with optional status filter. Results are Redis-cached.
    ?status=PENDING | APPROVED | REJECTED | SELECTED
    """
    cache_key = f"internship:applications:{status.upper()}" if status else "internship:applications"

    # ── Cache READ ────────────────────────────────────────────────────
    cached = _cache_get(cache_key)
    if cached is not None:
        print(f"✅ Cache hit: {cache_key}")
        return JSONResponse(content=cached)

    # ── Oracle fallback ────────────────────────────────────────────
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return json_error("Database connection failed", 500)

        cur = conn.cursor()
        try:
            if status:
                cur.execute(
                    "SELECT INTERN_ID, FULL_NAME, EMAIL, MOBILE, COLLEGE_NAME, BRANCH, "
                    "       INTERNSHIP_DOMAIN, INTERNSHIP_DURATION, STATUS, SUBMITTED_AT "
                    "FROM NRM_INTERNSHIP_APPLICATIONS WHERE STATUS = :1 "
                    "ORDER BY SUBMITTED_AT DESC", (status.upper(),))
            else:
                cur.execute(
                    "SELECT INTERN_ID, FULL_NAME, EMAIL, MOBILE, COLLEGE_NAME, BRANCH, "
                    "       INTERNSHIP_DOMAIN, INTERNSHIP_DURATION, STATUS, SUBMITTED_AT "
                    "FROM NRM_INTERNSHIP_APPLICATIONS ORDER BY SUBMITTED_AT DESC")

            cols = ["intern_id","full_name","email","mobile","college_name","branch",
                    "internship_domain","internship_duration","status","submitted_at"]
            rows         = cur.fetchall()
            applications = [{cols[i]: (str(r[i]) if r[i] is not None else None)
                             for i in range(len(cols))} for r in rows]
            result = {"success": True, "count": len(applications), "applications": applications}

            # ── Cache WRITE ───────────────────────────────────────────
            _cache_set(cache_key, result, ttl=_TTL_APPLICATIONS)
            return JSONResponse(content=result)
        finally:
            cur.close()

    except Exception as exc:
        print(f"❌ list_applications error: {exc}")
        return json_error(f"Server error: {exc}", 500)
    finally:
        if conn: conn.close()


# ============================================================
# GET /api/internship/my-application  (cached)
# Cache key : internship:{user_id}   5 min TTL
# Pass ?user_id= from the Flask proxy session.
# ============================================================
@app.get("/api/internship/my-application")
async def my_application(user_id: int):
    """
    Return the internship application(s) for a logged-in student.
    Cache key: internship:{user_id}  —  5 min TTL.
    """
    if user_id < 1:
        return json_error("user_id must be a positive integer", 400)

    cache_key = f"internship:{user_id}"

    # ── Cache READ ────────────────────────────────────────────────────
    cached = _cache_get(cache_key)
    if cached is not None:
        print(f"✅ Cache hit: {cache_key}")
        return JSONResponse(content=cached)

    # ── Oracle fallback ────────────────────────────────────────────
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return json_error("Database connection failed", 500)

        cur = conn.cursor()
        try:
            # Resolve email from user_id, then query applications by email
            cur.execute("SELECT EMAIL FROM NRM_USERS WHERE ID = :1 FETCH FIRST 1 ROWS ONLY", (user_id,))
            user_row = cur.fetchone()
            if not user_row:
                return json_error(f"No user found with user_id={user_id}", 404)

            cur.execute(
                "SELECT INTERN_ID, FULL_NAME, COLLEGE_NAME, BRANCH, "
                "       INTERNSHIP_DOMAIN, INTERNSHIP_DURATION, STATUS, "
                "       SUBMITTED_AT, INTERN_MODE AS MODE, START_DATE "
                "FROM NRM_INTERNSHIP_APPLICATIONS WHERE EMAIL = :1 "
                "ORDER BY SUBMITTED_AT DESC", (user_row[0],))
            cols = ["intern_id","full_name","college_name","branch",
                    "internship_domain","internship_duration","status",
                    "submitted_at","mode","start_date"]
            rows         = cur.fetchall()
            applications = [{cols[i]: (str(r[i]) if r[i] is not None else None)
                             for i in range(len(cols))} for r in rows]
            result = {"success": True, "user_id": user_id,
                      "count": len(applications), "applications": applications}

            # ── Cache WRITE ───────────────────────────────────────────
            _cache_set(cache_key, result, ttl=_TTL_USER)
            return JSONResponse(content=result)
        finally:
            cur.close()

    except Exception as exc:
        print(f"❌ my_application error: {exc}")
        return json_error(f"Server error: {exc}", 500)
    finally:
        if conn: conn.close()


# ============================================================
# GET /api/internship/letter/{intern_id}  (not cached)
# ============================================================
@app.get("/api/internship/letter/{intern_id}")
async def get_letter(intern_id: str):
    """Return the selection letter S3 URL. Not cached — fast single-row query."""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return json_error("Database connection failed", 500)
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT S3_URL FROM NRM_INTERNSHIP_DOCUMENTS "
                "WHERE INTERN_ID = :1 AND DOC_TYPE = 'SELECTION_LETTER'", (intern_id,))
            row = cur.fetchone()
            if not row:
                return json_error("Selection letter not found. Student may not have been selected yet.", 404)
            return JSONResponse(content={"success": True, "intern_id": intern_id, "letter_url": row[0]})
        finally:
            cur.close()
    except Exception as exc:
        return json_error(f"Server error: {exc}", 500)
    finally:
        if conn: conn.close()


# ============================================================
# GET /health
# ============================================================
@app.get("/health")
def health_check():
    """Health check — verifies Oracle and Redis service reachability."""
    db_ok    = False
    redis_ok = False

    conn = get_db_connection()
    if conn:
        db_ok = True
        conn.close()

    try:
        r = httpx.get(f"{_REDIS_SERVICE_URL}/health", timeout=_REDIS_TIMEOUT)
        redis_ok = r.json().get("success", False)
    except Exception:
        pass

    return {
        "status":        "healthy" if (db_ok and redis_ok) else "degraded",
        "service":       "internship-service",
        "database":      "connected"    if db_ok    else "disconnected",
        "redis_service": "connected"    if redis_ok else "disconnected",
        "redis_url":     _REDIS_SERVICE_URL,
        "timestamp":     datetime.now().isoformat(),
    }


# ============================================================
# STARTUP
# ============================================================
if __name__ == "__main__":
    port = int(os.getenv("INTERNSHIP_PORT", 5050))
    print(f"🚀 Starting internship service on port {port}")
    print(f"📁 Working directory : {os.getcwd()}")
    print(f"🗄️ Oracle DSN        : {ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE_NAME}")
    uvicorn.run(app, host="0.0.0.0", port=port)
