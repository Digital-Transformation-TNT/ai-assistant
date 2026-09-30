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
class LoiXacThucGoogle(RuntimeError):
    """Token Google hỏng/hết hạn/bị thu hồi -> sếp phải vào /auth-google cấp quyền lại.
    Hay gặp nhất: app OAuth để chế độ 'Testing' -> refresh_token chết sau 7 ngày."""


# Google trả các mã này khi refresh_token không dùng được nữa (phải cấp quyền lại)
_LOI_TOKEN_CHET = ("invalid_grant", "unauthorized_client", "invalid_client")


def _access_token(user=None):
    user = user or GOOGLE_CAL_USER
    tok, het_han = _cache.get(user, (None, 0))
    if tok and time.time() < het_han:
        return tok

    row = _tokens_col.find_one({"_id": user}) if _tokens_col is not None else None
    if not row or not row.get("refresh_token"):
        raise LoiXacThucGoogle(f"Chưa kết nối Google cho user '{user}'. Vào /auth-google một lần.")

    r = requests.post(TOKEN_URL, data={
        "refresh_token": row["refresh_token"],
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "grant_type": "refresh_token",
    }, timeout=15)
    data = r.json()
    if not data.get("access_token"):
        if data.get("error") in _LOI_TOKEN_CHET:
            raise LoiXacThucGoogle(f"Token Google hết hạn/bị thu hồi ({data.get('error')}). "
                                   "Vào /auth-google cấp quyền lại.")
        raise RuntimeError(f"Không làm mới được token Google: {data}")

    tok = data["access_token"]
    _cache[user] = (tok, time.time() + int(data.get("expires_in", 3600)) - 60)
    return tok


def kiem_tra_ket_noi(user=None):
    """Thử lấy access token THẬT (không chỉ xem có lưu token trong Mongo không).
    Trả (True, None) nếu dùng được, (False, 'lý do') nếu không."""
    if not da_cau_hinh():
        return False, "Chưa cấu hình GOOGLE_CLIENT_ID / SECRET / REDIRECT_URI"
    try:
        _access_token(user)
        return True, None
    except Exception as e:
        return False, str(e)


def _headers(user=None):
    return {"Authorization": f"Bearer {_access_token(user)}",
            "Content-Type": "application/json"}


def _goi(method, path, user=None, params=None, json_body=None, timeout=20):
    """Gọi Google Calendar API. Trả (status_code, data). Tự thử lại 1 lần nếu 401."""
    for lan in range(2):
        r = requests.request(method, f"{API}{path}", headers=_headers(user),
                             params=params, json=json_body, timeout=timeout)
        if r.status_code == 401 and lan == 0:
            _cache.pop(user or GOOGLE_CAL_USER, None)   # access token cũ hỏng -> lấy lại
            continue
        break
    try:
        data = r.json() if r.content else {}
    except ValueError:
        data = {"raw": r.text[:300]}
    return r.status_code, data


def _loi(e):
    """Đổi exception thành dict lỗi thống nhất (auth=True nếu cần cấp quyền lại)."""
    return {"error": str(e), "auth": isinstance(e, LoiXacThucGoogle)}


def _iso(x):
    """Chấp nhận ISO8601 hoặc timestamp -> chuỗi ISO có offset +07:00."""
    if isinstance(x, (int, float)):
        return datetime.fromtimestamp(x, VN_TZ).isoformat()
    dt = datetime.fromisoformat(str(x).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=VN_TZ)
    return dt.isoformat()


# ==================== CHECK TRÙNG LỊCH ====================
def bi_trung_lich(bat_dau_iso, ket_thuc_iso, user=None):
    """True nếu sếp đã có việc khác trong khung giờ đó."""
    try:
        st, data = _goi("POST", "/freeBusy", user, json_body={
            "timeMin": _iso(bat_dau_iso),
            "timeMax": _iso(ket_thuc_iso),
            "timeZone": "Asia/Ho_Chi_Minh",
            "items": [{"id": CALENDAR_ID}],
        }, timeout=15)
        busy = data.get("calendars", {}).get(CALENDAR_ID, {}).get("busy", [])
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
                 {"error": "...", "auth": bool}
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
                "requestId": f"meet-{time.time_ns()}",
                "conferenceSolutionKey": {"type": "hangoutsMeet"},
            }}

        st, data = _goi("POST", f"/calendars/{CALENDAR_ID}/events", user,
                        params=params, json_body=body)
        if st >= 300 or data.get("error"):
            return {"error": data}

        return {
            "success": True,
            "event_id": data.get("id"),
            "link": data.get("htmlLink"),
            "meet": (data.get("conferenceData", {}) or {}).get("entryPoints", [{}])[0].get("uri"),
        }
    except Exception as e:
        return _loi(e)


# ==================== ĐỔI GIỜ / HUỶ SỰ KIỆN ====================
def doi_gio_su_kien(event_id, bat_dau_iso, ket_thuc_iso, user=None, send_updates="all"):
    """Trả {"success": True, "link"} | {"error", "not_found": bool, "auth": bool}.
    Chỗ gọi PHẢI kiểm tra kết quả (trước đây bỏ qua -> lỗi mà vẫn báo sếp 'đã dời')."""
    try:
        st, data = _goi("PATCH", f"/calendars/{CALENDAR_ID}/events/{event_id}", user,
                        params={"sendUpdates": send_updates},
                        json_body={
                            "start": {"dateTime": _iso(bat_dau_iso), "timeZone": "Asia/Ho_Chi_Minh"},
                            "end": {"dateTime": _iso(ket_thuc_iso), "timeZone": "Asia/Ho_Chi_Minh"},
                        })
        if st < 300:
            return {"success": True, "link": data.get("htmlLink")}
        return {"error": data, "not_found": st in (404, 410)}
    except Exception as e:
        return _loi(e)


def huy_su_kien(event_id, user=None, send_updates="all"):
    """Xoá sự kiện. Sự kiện đã không còn (404/410) cũng coi là xoá xong."""
    try:
        st, data = _goi("DELETE", f"/calendars/{CALENDAR_ID}/events/{event_id}", user,
                        params={"sendUpdates": send_updates})
        if st in (200, 204, 404, 410):
            return {"success": True, "da_mat_tu_truoc": st in (404, 410)}
        return {"error": data}
    except Exception as e:
        return _loi(e)


# ==================== DÙNG CHO ĐỒNG BỘ LARK -> GOOGLE ====================
def tao_su_kien_tho(body, user=None, send_updates="none"):
    """Tạo sự kiện từ body Google đầy đủ (không check trùng, không Meet)."""
    try:
        st, data = _goi("POST", f"/calendars/{CALENDAR_ID}/events", user,
                        params={"sendUpdates": send_updates}, json_body=body)
        if st < 300:
            return {"success": True, "event_id": data.get("id"), "link": data.get("htmlLink")}
        return {"error": data}
    except Exception as e:
        return _loi(e)


def cap_nhat_su_kien(event_id, body, user=None, send_updates="none"):
    """PATCH các trường trong body. Trả thêm not_found=True nếu sự kiện đã bị xoá."""
    try:
        st, data = _goi("PATCH", f"/calendars/{CALENDAR_ID}/events/{event_id}", user,
                        params={"sendUpdates": send_updates}, json_body=body)
        if st < 300 and data.get("status") != "cancelled":
            return {"success": True, "event_id": data.get("id")}
        return {"error": data, "not_found": st in (404, 410) or data.get("status") == "cancelled"}
    except Exception as e:
        return _loi(e)


def tim_theo_thuoc_tinh(khoa, gia_tri, user=None):
    """Tìm event Google có extendedProperties.private[khoa] == gia_tri. Trả id hoặc None."""
    st, data = _goi("GET", f"/calendars/{CALENDAR_ID}/events", user, params={
        "privateExtendedProperty": f"{khoa}={gia_tri}", "maxResults": "5",
    })
    if st >= 300:
        raise RuntimeError(f"Lỗi tìm sự kiện Google: {data}")
    for e in data.get("items", []):
        if e.get("status") != "cancelled":
            return e.get("id")
    return None


def tim_su_kien_trung(summary, bat_dau_iso, ket_thuc_iso, user=None):
    """Tìm event Google (không lặp) CÙNG tiêu đề + CÙNG giờ bắt đầu. Trả id hoặc None.
    Dùng khi đồng bộ lần đầu để không nhân đôi lịch sếp đã có sẵn ở cả 2 bên."""
    bd = datetime.fromisoformat(_iso(bat_dau_iso))
    st, data = _goi("GET", f"/calendars/{CALENDAR_ID}/events", user, params={
        "singleEvents": "true", "maxResults": "50",
        "timeMin": _iso(bat_dau_iso), "timeMax": _iso(ket_thuc_iso),
    })
    if st >= 300:
        raise RuntimeError(f"Lỗi tìm sự kiện Google: {data}")
    ten = (summary or "").strip().lower()
    for e in data.get("items", []):
        if e.get("status") == "cancelled" or e.get("recurringEventId"):
            continue
        s = (e.get("start") or {}).get("dateTime")
        if not s or (e.get("summary") or "").strip().lower() != ten:
            continue
        if datetime.fromisoformat(s.replace("Z", "+00:00")) == bd:
            return e.get("id")
    return None


# ==================== ĐỌC DANH SÁCH SỰ KIỆN (để trả lời sếp hỏi lịch) ====================
def liet_ke_su_kien(tu_iso=None, den_iso=None, gioi_han=50, user=None, bao_loi=False):
    """Đọc các sự kiện trên Google Calendar của sếp trong khoảng [tu_iso, den_iso].
    Trả về list dict {id, summary, start, end, location, ca_ngay, tu_lark}.
    - bao_loi=False: lỗi/chưa kết nối -> trả list rỗng (chỗ gọi không bao giờ vỡ).
    - bao_loi=True : lỗi -> raise, để chỗ gọi báo thật 'lỗi đọc lịch' thay vì nói
      nhầm là 'sếp không có lịch nào'."""
    if not (da_cau_hinh() and da_ket_noi(user)):
        if bao_loi:
            raise LoiXacThucGoogle("Chưa kết nối Google Calendar. Vào /auth-google một lần.")
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
        st, data = _goi("GET", f"/calendars/{CALENDAR_ID}/events", user, params=params, timeout=15)
        if st >= 300 or data.get("error"):
            raise RuntimeError(f"Lỗi đọc lịch Google: {data.get('error', data)}")
        out = []
        for e in data.get("items", []):
            if e.get("status") == "cancelled":
                continue
            s = e.get("start", {}) or {}
            en = e.get("end", {}) or {}
            rieng = ((e.get("extendedProperties") or {}).get("private") or {})
            out.append({
                "id": e.get("id"),
                "summary": e.get("summary", "(không tiêu đề)"),
                "start": s.get("dateTime") or s.get("date"),
                "end": en.get("dateTime") or en.get("date"),
                "location": e.get("location", ""),
                "ca_ngay": "date" in s,   # sự kiện cả ngày (không có giờ cụ thể)
                "tu_lark": bool(rieng.get("lark_event_id")),   # được đồng bộ từ Lark Calendar
            })
        return out
    except Exception as e:
        print(f"⚠️ Lỗi liet_ke_su_kien: {e}")
        if bao_loi:
            raise
        return []
