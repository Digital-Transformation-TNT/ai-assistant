"""
============================================================
 LARK CALENDAR -> GOOGLE CALENDAR (đồng bộ 1 chiều)
============================================================
Vì sao cần: trước đây bot chỉ nghe TIN NHẮN Lark. Sếp sửa lịch thẳng trên giao diện
Lark Calendar thì bot không hề biết -> Google Calendar không đổi theo.

Cách hoạt động:
  1. Sếp mở  <PUBLIC_URL>/auth-lark  MỘT LẦN -> đăng nhập Lark, cho phép đọc lịch
     -> lưu user_access_token + refresh_token vào MongoDB (collection lark_tokens)
  2. Bot đăng ký nhận sự kiện "lịch thay đổi" (calendar.calendar.event.changed_v4)
     -> Lark bắn về /lark-webhook -> bot đồng bộ NGAY.
  3. Lưới an toàn: mỗi lần /nhac (cron) / luồng nền chạy -> đồng bộ thêm 1 lần
     (phòng khi webhook lỡ, hoặc chưa bật event subscription).
  4. Đồng bộ tăng dần bằng sync_token của Lark: chỉ lấy các sự kiện ĐÃ THAY ĐỔI.
     Mỗi sự kiện Lark được nối với 1 sự kiện Google (collection lark_gg_links):
       - mới      -> tạo trên Google
       - sửa      -> cập nhật Google (giờ, tiêu đề, mô tả, địa điểm, lặp lại)
       - xoá/huỷ  -> xoá trên Google
     Sự kiện Google tạo ra có extendedProperties.private.lark_event_id, nên dù Mongo
     mất liên kết thì lần sau vẫn tìm lại được, không tạo trùng.

Chỉ 1 CHIỀU (Lark -> Google): bot không ghi gì lên Lark, nên không có vòng lặp.

Cần env:
  LARK_APP_ID / LARK_APP_SECRET   (đã có)
  LARK_REDIRECT_URI = <PUBLIC_URL>/lark-oauth-callback
  (tuỳ chọn) LARK_OAUTH_SCOPE, LARK_AUTH_URL, LARK_BASE_URL, LARK_CAL_USER
Cấu hình trên Lark Developer Console (app của bot):
  - Security Settings -> Redirect URLs: thêm đúng LARK_REDIRECT_URI
  - Permissions & Scopes: quyền đọc lịch/sự kiện của user (calendar:calendar:readonly)
    + offline_access (để có refresh_token), rồi publish lại version app
  - Event Subscriptions: thêm sự kiện "calendar.calendar.event.changed_v4"
============================================================
"""
import os
import re
import json
import time
import hashlib
from datetime import datetime, timezone, timedelta, date
from urllib.parse import urlencode

import requests
from pymongo.errors import DuplicateKeyError

import google_calendar as gcal

LARK_BASE_URL = os.environ.get("LARK_BASE_URL", "https://open.larksuite.com").rstrip("/")
LARK_APP_ID = os.environ.get("LARK_APP_ID")
LARK_APP_SECRET = os.environ.get("LARK_APP_SECRET")
LARK_REDIRECT_URI = os.environ.get("LARK_REDIRECT_URI")
LARK_AUTH_URL = os.environ.get(
    "LARK_AUTH_URL", "https://accounts.larksuite.com/open-apis/authen/v1/authorize")
LARK_OAUTH_SCOPE = os.environ.get("LARK_OAUTH_SCOPE", "calendar:calendar:readonly offline_access")
LARK_CAL_USER = os.environ.get("LARK_CAL_USER", "sep")

VN_TZ = timezone(timedelta(hours=7))
DAU_HIEU = "[Đồng bộ từ Lark Calendar]"
# Mã lỗi Lark khi user_access_token hết hạn/không hợp lệ -> làm mới rồi thử lại
_MA_TOKEN_HONG = {99991661, 99991663, 99991668, 99991677, 20005}

_tokens_col = None   # lark_tokens : token OAuth của sếp + calendar_id
_sync_col = None     # lark_sync   : sync_token, khoá chạy, lần chạy cuối
_links_col = None    # lark_gg_links: lark_event_id -> gg_event_id


class LoiXacThucLark(RuntimeError):
    """Token Lark của sếp hỏng/hết hạn -> sếp phải vào /auth-lark cấp quyền lại."""


class LoiApiLark(RuntimeError):
    pass


def init(db):
    global _tokens_col, _sync_col, _links_col
    _tokens_col = db["lark_tokens"]
    _sync_col = db["lark_sync"]
    _links_col = db["lark_gg_links"]


def da_cau_hinh():
    return bool(LARK_APP_ID and LARK_APP_SECRET and LARK_REDIRECT_URI)


def da_ket_noi(user=None):
    user = user or LARK_CAL_USER
    if _tokens_col is None:
        return False
    row = _tokens_col.find_one({"_id": user})
    return bool(row and (row.get("refresh_token") or row.get("access_token")))


# ==================== OAUTH (sếp cấp quyền 1 lần) ====================
def auth_url(user=None):
    params = {
        "client_id": LARK_APP_ID,
        "redirect_uri": LARK_REDIRECT_URI,
        "response_type": "code",
        "scope": LARK_OAUTH_SCOPE,
        "state": user or LARK_CAL_USER,
    }
    return f"{LARK_AUTH_URL}?{urlencode(params)}"


def _goi_token(payload):
    r = requests.post(f"{LARK_BASE_URL}/open-apis/authen/v2/oauth/token", json=payload, timeout=15)
    try:
        data = r.json()
    except ValueError:
        data = {"raw": r.text[:300]}
    if r.status_code >= 300 or data.get("code") not in (0, None) or not data.get("access_token"):
        raise LoiXacThucLark(f"Lark từ chối cấp token: {data}")
    return data


def _luu_token(user, data):
    now = time.time()
    upd = {
        "access_token": data["access_token"],
        "access_exp": now + int(data.get("expires_in", 7200)) - 120,
        "updated_at": datetime.now(timezone.utc),
    }
    # Lark XOAY refresh_token: mỗi lần làm mới trả cái mới, cái cũ hết dùng -> luôn lưu lại
    if data.get("refresh_token"):
        upd["refresh_token"] = data["refresh_token"]
        upd["refresh_exp"] = now + int(data.get("refresh_token_expires_in") or 0)
    _tokens_col.update_one({"_id": user}, {"$set": upd}, upsert=True)


def doi_code_lay_token(code, user=None):
    user = user or LARK_CAL_USER
    data = _goi_token({
        "grant_type": "authorization_code",
        "client_id": LARK_APP_ID,
        "client_secret": LARK_APP_SECRET,
        "code": code,
        "redirect_uri": LARK_REDIRECT_URI,
    })
    _luu_token(user, data)
    # Cấp quyền lại -> tra lại lịch chính + đăng ký webhook lại
    _tokens_col.update_one({"_id": user}, {"$unset": {"calendar_id": "", "da_dang_ky_webhook": ""}})
    if not data.get("refresh_token"):
        print("⚠️ Lark không trả refresh_token -> kiểm tra quyền offline_access của app")
    return user


def _access_token(user=None, ep_lam_moi=False):
    user = user or LARK_CAL_USER
    row = _tokens_col.find_one({"_id": user}) if _tokens_col is not None else None
    if not row:
        raise LoiXacThucLark("Chưa kết nối Lark Calendar. Vào /auth-lark một lần.")
    if not ep_lam_moi and row.get("access_token") and time.time() < row.get("access_exp", 0):
        return row["access_token"]
    if not row.get("refresh_token"):
        raise LoiXacThucLark("Token Lark hết hạn và không có refresh_token "
                             "(app thiếu quyền offline_access?). Vào /auth-lark cấp quyền lại.")
    data = _goi_token({
        "grant_type": "refresh_token",
        "client_id": LARK_APP_ID,
        "client_secret": LARK_APP_SECRET,
        "refresh_token": row["refresh_token"],
    })
    _luu_token(user, data)
    return data["access_token"]


def kiem_tra_ket_noi(user=None):
    if not da_cau_hinh():
        return False, "Chưa cấu hình LARK_APP_ID / LARK_APP_SECRET / LARK_REDIRECT_URI"
    try:
        _access_token(user)
        return True, None
    except Exception as e:
        return False, str(e)


def _api(method, path, user=None, params=None, json_body=None):
    """Gọi Open API Lark bằng token CỦA SẾP. Tự làm mới token 1 lần nếu hỏng."""
    for lan in range(2):
        tok = _access_token(user, ep_lam_moi=(lan == 1))
        r = requests.request(
            method, f"{LARK_BASE_URL}{path}",
            headers={"Authorization": f"Bearer {tok}",
                     "Content-Type": "application/json; charset=utf-8"},
            params=params, json=json_body, timeout=20,
        )
        try:
            data = r.json()
        except ValueError:
            data = {"code": -1, "msg": r.text[:300]}
        if data.get("code") in _MA_TOKEN_HONG and lan == 0:
            continue
        return data
    return data


# ==================== LỊCH CHÍNH + ĐĂNG KÝ WEBHOOK ====================
def lich_chinh(user=None):
    user = user or LARK_CAL_USER
    row = _tokens_col.find_one({"_id": user}) or {}
    if row.get("calendar_id"):
        return row["calendar_id"]
    d = _api("POST", "/open-apis/calendar/v4/calendars/primary", user)
    if d.get("code") != 0:
        raise LoiApiLark(f"Không lấy được lịch chính Lark: {d}")
    cals = (d.get("data") or {}).get("calendars") or []
    if not cals:
        raise LoiApiLark(f"Lark không trả lịch chính: {d}")
    cid = cals[0]["calendar"]["calendar_id"]
    _tokens_col.update_one({"_id": user}, {"$set": {"calendar_id": cid}})
    return cid


def dang_ky_nhan_thay_doi(user=None):
    """Đăng ký để Lark bắn calendar.calendar.event.changed_v4 về /lark-webhook
    mỗi khi lịch của sếp thay đổi (cần bật event này trong Developer Console)."""
    user = user or LARK_CAL_USER
    cid = lich_chinh(user)
    d = _api("POST", f"/open-apis/calendar/v4/calendars/{cid}/events/subscription", user)
    ok = d.get("code") == 0
    _tokens_col.update_one({"_id": user}, {"$set": {
        "da_dang_ky_webhook": ok,
        "loi_dang_ky_webhook": None if ok else str(d)[:500],
    }})
    print(("✅ Đã đăng ký webhook thay đổi lịch Lark" if ok
           else f"⚠️ Chưa đăng ký được webhook lịch Lark (vẫn có cron dự phòng): {d}"))
    return ok, d


# ==================== LẤY CÁC SỰ KIỆN THAY ĐỔI ====================
def _lay_thay_doi(user, cid, sync_token):
    """Trả (items, sync_token_moi). Không có sync_token -> quét từ hôm qua trở đi."""
    items, page_token, sync_moi = [], None, None
    for _ in range(50):   # tối đa 50 trang, phòng vòng lặp vô hạn
        params = {"page_size": 500}
        if sync_token:
            params["sync_token"] = sync_token
        elif not page_token:
            params["anchor_time"] = str(int(time.time()) - 86400)
        if page_token:
            params["page_token"] = page_token
        d = _api("GET", f"/open-apis/calendar/v4/calendars/{cid}/events", user, params=params)
        if d.get("code") != 0:
            raise LoiApiLark(f"Lỗi đọc sự kiện Lark: {d}")
        data = d.get("data") or {}
        items += data.get("items") or []
        sync_moi = data.get("sync_token") or sync_moi
        page_token = data.get("page_token")
        if not data.get("has_more") or not page_token:
            break
    return items, sync_moi


# ==================== CHUYỂN SỰ KIỆN LARK -> BODY GOOGLE ====================
def _thoi_gian(t):
    if not t:
        return None
    if t.get("date"):
        return {"date": t["date"]}
    ts = int(t.get("timestamp"))
    return {"dateTime": datetime.fromtimestamp(ts, VN_TZ).isoformat(),
            "timeZone": t.get("timezone") or "Asia/Ho_Chi_Minh"}


def _body_google(ev):
    start = _thoi_gian(ev.get("start_time"))
    end = _thoi_gian(ev.get("end_time")) or dict(start)
    if "date" in start and "date" in end and end["date"] <= start["date"]:
        # Google cần ngày kết thúc (không tính) > ngày bắt đầu
        end = {"date": (date.fromisoformat(start["date"]) + timedelta(days=1)).isoformat()}
    mo_ta = (ev.get("description") or "").strip()
    dau = DAU_HIEU + (f" {ev['app_link']}" if ev.get("app_link") else "")
    body = {
        "summary": ev.get("summary") or "(không tiêu đề)",
        "description": f"{mo_ta}\n\n{dau}" if mo_ta else dau,
        "location": ((ev.get("location") or {}).get("name") or ""),
        "start": start,
        "end": end,
        "extendedProperties": {"private": {"lark_event_id": ev["event_id"]}},
    }
    rec = (ev.get("recurrence") or "").strip()
    if rec:
        body["recurrence"] = [rec if rec.upper().startswith("RRULE:") else f"RRULE:{rec}"]
    return body


def _bam(body):
    return hashlib.md5(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def _da_qua(ev):
    """Sự kiện KHÔNG lặp đã kết thúc hơn 1 ngày -> khỏi tạo mới trên Google."""
    t = ev.get("end_time") or ev.get("start_time") or {}
    try:
        if t.get("date"):
            return date.fromisoformat(t["date"]) < (datetime.now(VN_TZ) - timedelta(days=1)).date()
        return int(t["timestamp"]) < time.time() - 86400
    except Exception:
        return False


def _kiem_tra(kq):
    """Lỗi token Google -> dừng cả lượt đồng bộ (báo sếp cấp quyền lại)."""
    if kq.get("auth"):
        raise gcal.LoiXacThucGoogle(kq.get("error"))
    return kq


def _luu_link(lark_id, gg_id, body, **them):
    _links_col.update_one({"_id": lark_id}, {"$set": {
        "gg_event_id": gg_id, "hash": _bam(body),
        "la_lap": bool(body.get("recurrence")),
        "ca_ngay": "date" in body["start"],
        "updated_at": datetime.now(timezone.utc), **them,
    }}, upsert=True)


# ==================== ÁP DỤNG 1 SỰ KIỆN ====================
def _ap_dung(ev):
    """Trả 'tao' | 'sua' | 'xoa' | 'giu_nguyen'."""
    lark_id = ev.get("event_id")
    if not lark_id:
        return "giu_nguyen"
    huy = ev.get("status") == "cancelled" or ev.get("self_rsvp_status") == "decline"
    if ev.get("is_exception") and ev.get("recurring_event_id"):
        return _ap_dung_ngoai_le(ev, huy)

    link = _links_col.find_one({"_id": lark_id})
    if huy:
        gg_id = (link or {}).get("gg_event_id") or gcal.tim_theo_thuoc_tinh("lark_event_id", lark_id)
        if not gg_id:
            return "giu_nguyen"
        kq = _kiem_tra(gcal.huy_su_kien(gg_id, send_updates="none"))
        if not kq.get("success"):
            raise RuntimeError(f"xoá trên Google lỗi: {kq.get('error')}")
        _links_col.delete_one({"_id": lark_id})
        return "xoa"

    body = _body_google(ev)
    if link:
        if link.get("hash") == _bam(body):
            return "giu_nguyen"
        patch = dict(body)
        if link.get("la_lap") and not body.get("recurrence"):
            patch["recurrence"] = []   # Lark bỏ lặp lại -> Google cũng bỏ
        kq = _kiem_tra(gcal.cap_nhat_su_kien(link["gg_event_id"], patch))
        if kq.get("success"):
            _luu_link(lark_id, link["gg_event_id"], body)
            return "sua"
        if not kq.get("not_found"):
            raise RuntimeError(f"cập nhật Google lỗi: {kq.get('error')}")
        # Sự kiện Google đã bị xoá tay -> tạo lại bên dưới

    if not body.get("recurrence") and _da_qua(ev):
        return "giu_nguyen"

    # Chống trùng: đã có event mang lark_event_id này, hoặc sếp đã có sẵn lịch
    # cùng tên + cùng giờ trên Google -> NỐI vào thay vì tạo mới.
    gg_id = gcal.tim_theo_thuoc_tinh("lark_event_id", lark_id)
    if gg_id:
        kq = _kiem_tra(gcal.cap_nhat_su_kien(gg_id, body))
        if kq.get("success"):
            _luu_link(lark_id, gg_id, body)
            return "sua"
    elif not body.get("recurrence") and "dateTime" in body["start"]:
        gg_id = gcal.tim_su_kien_trung(body["summary"], body["start"]["dateTime"],
                                       body["end"]["dateTime"])
        if gg_id:
            # Chỉ gắn nhãn, KHÔNG ghi đè nội dung (có thể là lịch bot tạo, có Meet/khách mời)
            kq = _kiem_tra(gcal.cap_nhat_su_kien(gg_id, {"extendedProperties": body["extendedProperties"]}))
            if kq.get("success"):
                _luu_link(lark_id, gg_id, body, noi_vao_co_san=True)
                return "sua"

    kq = _kiem_tra(gcal.tao_su_kien_tho(body))
    if not kq.get("success"):
        raise RuntimeError(f"tạo trên Google lỗi: {kq.get('error')}")
    _luu_link(lark_id, kq["event_id"], body)
    return "tao"


def _ap_dung_ngoai_le(ev, huy):
    """Sửa/huỷ RIÊNG 1 lần của lịch lặp lại. event_id Lark của lần đó có dạng
    <uid>_<timestamp giờ gốc>; trên Google, lần tương ứng có id <id_gốc>_<YYYYMMDDTHHMMSSZ>."""
    lark_id = ev["event_id"]
    uid, _, duoi = lark_id.rpartition("_")
    if not (uid and duoi.isdigit() and duoi != "0"):
        print(f"⚠️ Không đọc được giờ gốc của lần lặp {lark_id}, bỏ qua")
        return "giu_nguyen"
    goc = _links_col.find_one({"_id": ev["recurring_event_id"]}) or \
        _links_col.find_one({"_id": {"$regex": f"^{re.escape(uid)}"}, "la_lap": True})
    if not goc:
        print(f"⚠️ Chưa có lịch lặp gốc trên Google cho {lark_id}, bỏ qua")
        return "giu_nguyen"
    t = datetime.fromtimestamp(int(duoi), timezone.utc)
    hau_to = t.astimezone(VN_TZ).strftime("%Y%m%d") if goc.get("ca_ngay") else t.strftime("%Y%m%dT%H%M%SZ")
    inst_id = f"{goc['gg_event_id']}_{hau_to}"

    if huy:
        kq = _kiem_tra(gcal.huy_su_kien(inst_id, send_updates="none"))
        if not kq.get("success"):
            raise RuntimeError(f"huỷ 1 lần lặp trên Google lỗi: {kq.get('error')}")
        _links_col.delete_one({"_id": lark_id})
        return "xoa"

    body = _body_google(ev)
    body.pop("recurrence", None)
    link = _links_col.find_one({"_id": lark_id})
    if link and link.get("hash") == _bam(body):
        return "giu_nguyen"
    kq = _kiem_tra(gcal.cap_nhat_su_kien(inst_id, body))
    if not kq.get("success"):
        raise RuntimeError(f"sửa 1 lần lặp trên Google lỗi: {kq.get('error')}")
    _luu_link(lark_id, inst_id, body)
    return "sua"


# ==================== KHOÁ (chống 2 lượt chạy song song tạo trùng) ====================
def _giu_khoa(user, giay=120):
    now = datetime.now(timezone.utc)
    try:
        _sync_col.update_one(
            {"_id": user, "$or": [{"lock_until": {"$lt": now}}, {"lock_until": {"$exists": False}}]},
            {"$set": {"lock_until": now + timedelta(seconds=giay)}},
            upsert=True,
        )
        return True
    except DuplicateKeyError:
        return False   # đang có lượt khác chạy


def _nha_khoa(user):
    _sync_col.update_one({"_id": user}, {"$unset": {"lock_until": ""}})


# ==================== HÀM CHÍNH ====================
def dong_bo(user=None):
    """Đồng bộ các thay đổi Lark Calendar -> Google Calendar.
    Trả dict thống kê. Raise LoiXacThucLark / gcal.LoiXacThucGoogle khi token hỏng
    (chỗ gọi sẽ nhắn sếp link cấp quyền lại)."""
    user = user or LARK_CAL_USER
    if not da_cau_hinh():
        return {"bo_qua": "chưa cấu hình Lark OAuth (LARK_REDIRECT_URI)"}
    if not da_ket_noi(user):
        return {"bo_qua": "sếp chưa cấp quyền Lark Calendar (/auth-lark)"}
    if not (gcal.da_cau_hinh() and gcal.da_ket_noi()):
        return {"bo_qua": "chưa kết nối Google Calendar (/auth-google)"}
    if not _giu_khoa(user):
        return {"bo_qua": "đang có lượt đồng bộ khác chạy"}

    tk = {"tao": 0, "sua": 0, "xoa": 0, "giu_nguyen": 0, "loi": []}
    try:
        cid = lich_chinh(user)
        if not (_tokens_col.find_one({"_id": user}) or {}).get("da_dang_ky_webhook"):
            try:
                dang_ky_nhan_thay_doi(user)
            except Exception as e:
                print(f"⚠️ Lỗi đăng ký webhook lịch Lark: {e}")

        state = _sync_col.find_one({"_id": user}) or {}
        sync_token = state.get("sync_token")
        try:
            items, sync_moi = _lay_thay_doi(user, cid, sync_token)
        except LoiApiLark as e:
            if not sync_token:
                raise
            print(f"⚠️ sync_token Lark không dùng được nữa ({e}) -> quét lại từ đầu")
            items, sync_moi = _lay_thay_doi(user, cid, None)

        # Lịch lặp gốc trước, các lần sửa riêng sau (để đã có id gốc trên Google)
        items.sort(key=lambda e: 1 if e.get("is_exception") else 0)
        for ev in items:
            try:
                tk[_ap_dung(ev)] += 1
            except gcal.LoiXacThucGoogle:
                raise
            except Exception as e:
                tk["loi"].append(f"{ev.get('summary', ev.get('event_id'))}: {e}"[:300])
                print(f"⚠️ Lỗi đồng bộ sự kiện Lark {ev.get('event_id')}: {e}")

        so_lan_loi = state.get("so_lan_loi", 0) + 1 if tk["loi"] else 0
        upd = {"lan_cuoi": datetime.now(timezone.utc), "loi_cuoi": tk["loi"][:10],
               "thong_ke_cuoi": {k: v for k, v in tk.items() if k != "loi"},
               "so_lan_loi": so_lan_loi}
        # Có lỗi thì GIỮ sync_token cũ để lượt sau làm lại (việc đã xong sẽ được bỏ qua
        # nhờ so hash). Lỗi lặp lại 5 lượt thì cho qua để không kẹt mãi.
        if sync_moi and (not tk["loi"] or so_lan_loi >= 5):
            upd["sync_token"] = sync_moi
            upd["so_lan_loi"] = 0
        _sync_col.update_one({"_id": user}, {"$set": upd})
        if tk["tao"] or tk["sua"] or tk["xoa"] or tk["loi"]:
            print(f"🔁 Đồng bộ Lark->Google: tạo {tk['tao']}, sửa {tk['sua']}, "
                  f"xoá {tk['xoa']}, lỗi {len(tk['loi'])}")
        return tk
    finally:
        _nha_khoa(user)


def trang_thai(user=None):
    user = user or LARK_CAL_USER
    ok, loi = kiem_tra_ket_noi(user)
    row = (_tokens_col.find_one({"_id": user}) if _tokens_col is not None else None) or {}
    st = (_sync_col.find_one({"_id": user}) if _sync_col is not None else None) or {}
    return {
        "da_cau_hinh": da_cau_hinh(),
        "da_ket_noi": da_ket_noi(user),
        "token_dung_duoc": ok,
        "loi_token": loi,
        "calendar_id": row.get("calendar_id"),
        "da_dang_ky_webhook": row.get("da_dang_ky_webhook"),
        "loi_dang_ky_webhook": row.get("loi_dang_ky_webhook"),
        "co_sync_token": bool(st.get("sync_token")),
        "lan_dong_bo_cuoi": str(st.get("lan_cuoi")) if st.get("lan_cuoi") else None,
        "thong_ke_cuoi": st.get("thong_ke_cuoi"),
        "loi_cuoi": st.get("loi_cuoi"),
        "so_su_kien_da_noi": _links_col.count_documents({}) if _links_col is not None else 0,
    }
