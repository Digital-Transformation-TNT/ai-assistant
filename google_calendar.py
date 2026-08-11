"""
============================================================
 GOOGLE CALENDAR - tự đặt lịch ngay trong app Railway
============================================================
Không cần dự án Vercel riêng, không cần thư viện Google nặng.
Chỉ dùng `requests` (bạn đã có sẵn trong requirements.txt).

Cách hoạt động:
  1. Sếp vào  https://<app>.up.railway.app/auth-google  MỘT LẦN
     -> đăng nhập Google -> Google trả về refresh_token
  2. refresh_token được lưu vào MongoDB (collection google_tokens)
  3. Từ đó về sau bot tự lấy access_token mới mỗi khi cần -> tạo lịch mãi mãi

Cần env var trên Railway:
  GOOGLE_CLIENT_ID
  GOOGLE_CLIENT_SECRET
  GOOGLE_REDIRECT_URI   = https://<app>.up.railway.app/oauth2callback
  GOOGLE_CAL_USER       = sep        (tuỳ ý, chỉ là khoá lưu token)
  GOOGLE_CALENDAR_ID    = primary    (mặc định lịch chính của sếp)
============================================================
"""
import os
import time
import requests
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI = os.environ.get("GOOGLE_REDIRECT_URI")
GOOGLE_CAL_USER = os.environ.get("GOOGLE_CAL_USER", "sep")
CALENDAR_ID = os.environ.get("GOOGLE_CALENDAR_ID", "primary")

SCOPE = "https://www.googleapis.com/auth/calendar"
TOKEN_URL = "https://oauth2.googleapis.com/token"
API = "https://www.googleapis.com/calendar/v3"
VN_TZ = timezone(timedelta(hours=7))

_tokens_col = None          # collection MongoDB
_cache = {}                 # {user: (access_token, het_han_ts)} - đỡ gọi refresh liên tục


def init(db):
    """Gọi 1 lần trong assistant.py:  gcal.init(db)"""
    global _tokens_col
    _tokens_col = db["google_tokens"]


def da_cau_hinh():
    return bool(GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REDIRECT_URI)


def da_ket_noi(user=None):
    user = user or GOOGLE_CAL_USER
    if _tokens_col is None:
        return False
    return bool(_tokens_col.find_one({"_id": user}))


# ==================== BƯỚC 1: LINK ĐĂNG NHẬP GOOGLE ====================
def auth_url(user=None):
    user = user or GOOGLE_CAL_USER
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope": SCOPE,
        "access_type": "offline",   # BẮT BUỘC: để Google trả refresh_token
        "prompt": "consent",        # BẮT BUỘC: ép Google cấp lại refresh_token
        "state": user,
    }
    return "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)


# ==================== BƯỚC 2: ĐỔI CODE -> REFRESH TOKEN ====================
def doi_code_lay_token(code, user=None):
    user = user or GOOGLE_CAL_USER
    r = requests.post(TOKEN_URL, data={
        "code": code,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "grant_type": "authorization_code",
    }, timeout=15)
    data = r.json()
    if not data.get("refresh_token"):
        raise RuntimeError(f"Google không trả refresh_token: {data}")

    _tokens_col.update_one(
        {"_id": user},
        {"$set": {
            "refresh_token": data["refresh_token"],
            "updated_at": datetime.now(timezone.utc),
        }},
        upsert=True,
    )
    _cache[user] = (data.get("access_token"), time.time() + int(data.get("expires_in", 3600)) - 60)
    return user


# ==================== BƯỚC 3: LẤY ACCESS TOKEN (tự làm mới) ====================
def _access_token(user=None):
    user = user or GOOGLE_CAL_USER
    tok, het_han = _cache.get(user, (None, 0))
    if tok and time.time() < het_han:
        return tok

    row = _tokens_col.find_one({"_id": user}) if _tokens_col is not None else None
    if not row or not row.get("refresh_token"):
        raise RuntimeError(f"Chưa kết nối Google cho user '{user}'. Vào /auth-google một lần.")

    r = requests.post(TOKEN_URL, data={
        "refresh_token": row["refresh_token"],
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "grant_type": "refresh_token",
    }, timeout=15)
    data = r.json()
    if not data.get("access_token"):
        raise RuntimeError(f"Không làm mới được token Google: {data}")

    tok = data["access_token"]
    _cache[user] = (tok, time.time() + int(data.get("expires_in", 3600)) - 60)
    return tok


def _headers(user=None):
    return {"Authorization": f"Bearer {_access_token(user)}",
            "Content-Type": "application/json"}


def _iso(x):
    """Chấp nhận ISO8601 hoặc timestamp -> chuỗi ISO có offset +07:00."""
    if isinstance(x, (int, float)):
        return datetime.fromtimestamp(x, VN_TZ).isoformat()
    dt = datetime.fromisoformat(str(x))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=VN_TZ)
    return dt.isoformat()


# ==================== CHECK TRÙNG LỊCH ====================
def bi_trung_lich(bat_dau_iso, ket_thuc_iso, user=None):
    """True nếu sếp đã có việc khác trong khung giờ đó."""
    try:
        r = requests.post(
            f"{API}/freeBusy",
            headers=_headers(user),
            json={
                "timeMin": _iso(bat_dau_iso),
                "timeMax": _iso(ket_thuc_iso),
                "timeZone": "Asia/Ho_Chi_Minh",
                "items": [{"id": CALENDAR_ID}],
            }, timeout=15,
        )
        busy = r.json().get("calendars", {}).get(CALENDAR_ID, {}).get("busy", [])
        return len(busy) > 0, busy
    except Exception as e:
        print(f"⚠️ Lỗi check trùng lịch: {e}")
        return False, []


# ==================== TẠO SỰ KIỆN ====================
def tao_su_kien(summary, bat_dau_iso, ket_thuc_iso, description="",
                attendee_email=None, user=None, check_trung=True, tao_meet=False):
    """
    Tạo sự kiện trên Google Calendar của sếp, mời attendee_email (Google tự gửi mail).
    Trả về dict: {"success": True, "event_id":..., "link":...}
                 {"duplicate": True, "message":...}
                 {"error": "..."}
    """
    if not da_cau_hinh():
        return {"error": "Chưa cấu hình GOOGLE_CLIENT_ID / SECRET / REDIRECT_URI"}

    try:
        bat_dau = _iso(bat_dau_iso)
        ket_thuc = _iso(ket_thuc_iso)

        if check_trung:
            trung, busy = bi_trung_lich(bat_dau, ket_thuc, user)
            if trung:
                return {"duplicate": True,
                        "message": f"Sếp đã có lịch khác trong khung giờ này: {busy}"}

        body = {
            "summary": summary,
            "description": description,
            "start": {"dateTime": bat_dau, "timeZone": "Asia/Ho_Chi_Minh"},
            "end": {"dateTime": ket_thuc, "timeZone": "Asia/Ho_Chi_Minh"},
            "reminders": {"useDefault": False, "overrides": [
                {"method": "popup", "minutes": 15},
                {"method": "email", "minutes": 60},
            ]},
        }
        if attendee_email:
            body["attendees"] = [{"email": attendee_email}]

        params = {"sendUpdates": "all"}   # gửi mail mời cho attendee
        if tao_meet:
            params["conferenceDataVersion"] = 1
            body["conferenceData"] = {"createRequest": {
                "requestId": f"meet-{int(time.time())}",
                "conferenceSolutionKey": {"type": "hangoutsMeet"},
            }}

        r = requests.post(
            f"{API}/calendars/{CALENDAR_ID}/events",
            headers=_headers(user), params=params, json=body, timeout=20,
        )
        data = r.json()
        if r.status_code >= 300 or data.get("error"):
            return {"error": data}

        return {
            "success": True,
            "event_id": data.get("id"),
            "link": data.get("htmlLink"),
            "meet": (data.get("conferenceData", {}) or {}).get("entryPoints", [{}])[0].get("uri"),
        }
    except Exception as e:
        return {"error": str(e)}


# ==================== ĐỔI GIỜ / HUỶ SỰ KIỆN ====================
def doi_gio_su_kien(event_id, bat_dau_iso, ket_thuc_iso, user=None):
    try:
        r = requests.patch(
            f"{API}/calendars/{CALENDAR_ID}/events/{event_id}",
            headers=_headers(user), params={"sendUpdates": "all"},
            json={
                "start": {"dateTime": _iso(bat_dau_iso), "timeZone": "Asia/Ho_Chi_Minh"},
                "end": {"dateTime": _iso(ket_thuc_iso), "timeZone": "Asia/Ho_Chi_Minh"},
            }, timeout=20,
        )
        data = r.json()
        return {"success": True, "link": data.get("htmlLink")} if r.status_code < 300 else {"error": data}
    except Exception as e:
        return {"error": str(e)}


def huy_su_kien(event_id, user=None):
    try:
        r = requests.delete(
            f"{API}/calendars/{CALENDAR_ID}/events/{event_id}",
            headers=_headers(user), params={"sendUpdates": "all"}, timeout=20,
        )
        return {"success": r.status_code in (200, 204)}
    except Exception as e:
        return {"error": str(e)}


# ==================== ĐỌC DANH SÁCH SỰ KIỆN (để trả lời sếp hỏi lịch) ====================
def liet_ke_su_kien(tu_iso=None, den_iso=None, gioi_han=50, user=None):
    """Đọc các sự kiện trên Google Calendar của sếp trong khoảng [tu_iso, den_iso].
    Trả về list dict {summary, start, end, location}. Luôn trả list (rỗng nếu lỗi/chưa
    kết nối) — KHÔNG raise, để chỗ gọi không bao giờ vỡ."""
    if not (da_cau_hinh() and da_ket_noi(user)):
        return []
    try:
        params = {
            "singleEvents": "true", "orderBy": "startTime",
            "maxResults": str(gioi_han), "timeZone": "Asia/Ho_Chi_Minh",
        }
        if tu_iso:
            params["timeMin"] = _iso(tu_iso)
        if den_iso:
            params["timeMax"] = _iso(den_iso)
        r = requests.get(
            f"{API}/calendars/{CALENDAR_ID}/events",
            headers=_headers(user), params=params, timeout=15,
        )
        data = r.json()
        if data.get("error"):
            print(f"⚠️ Lỗi đọc lịch Google: {data.get('error')}")
            return []
        out = []
        for e in data.get("items", []):
            if e.get("status") == "cancelled":
                continue
            s = e.get("start", {}) or {}
            en = e.get("end", {}) or {}
            out.append({
                "id": e.get("id"),
                "summary": e.get("summary", "(không tiêu đề)"),
                "start": s.get("dateTime") or s.get("date"),
                "end": en.get("dateTime") or en.get("date"),
                "location": e.get("location", ""),
                "ca_ngay": "date" in s,   # sự kiện cả ngày (không có giờ cụ thể)
            })
        return out
    except Exception as e:
        print(f"⚠️ Lỗi liet_ke_su_kien: {e}")
        return []
