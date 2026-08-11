"""
============================================================
 TRỢ LÝ AI TRUNG TÂM (realtime + nhắc deadline)
============================================================
Có 2 chế độ chạy trong cùng 1 file:

  A) WEB (realtime): endpoint /xu-ly
     - Bot Lark/Zalo gọi ngay khi có tin mới
     - AI phân tích tin đó -> báo sếp nếu quan trọng -> ghi deadline

  B) CRON (nhắc deadline): endpoint /nhac  (Railway cron gọi mỗi 30 phút)
     - Quét việc sắp tới hạn, nhắc trước 15 phút, không lọt

Ngoài ra:
  - Endpoint /lenh: xử lý lệnh sếp đổi luật (gọi khi sếp nhắn bot)
  - File luật lưu trong MongoDB (collection settings)

Cần cài: flask gunicorn pymongo python-dotenv certifi requests openai
============================================================
"""
import os
import re
import json
import time
import threading
from datetime import datetime, timezone, timedelta

from flask import Flask, request, jsonify, redirect
from pymongo import MongoClient
from dotenv import load_dotenv
import certifi
import requests
from openai import OpenAI
import google_calendar as gcal

EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

load_dotenv()

# ==================== CẤU HÌNH ====================
MONGO_URI = os.environ.get("MONGO_URI")

# --- NHÀ CUNG CẤP AI (đổi được qua env, KHÔNG cần sửa code) ---
#   Dùng OpenAI  : để trống AI_BASE_URL, AI_MODEL=gpt-5.5
#   Dùng Groq    : AI_BASE_URL=https://api.groq.com/openai/v1, AI_MODEL=<model Groq>
# AI_API_KEY ưu tiên; nếu trống thì lấy lại OPENAI_API_KEY cũ cho tương thích ngược.
AI_BASE_URL = os.environ.get("AI_BASE_URL", "").strip()
AI_API_KEY = os.environ.get("AI_API_KEY") or os.environ.get("OPENAI_API_KEY")
AI_MODEL = os.environ.get("AI_MODEL", "gpt-5.5")
OPENAI_API_KEY = AI_API_KEY  # giữ tên cũ để phần còn lại của file không phải đổi
LARK_APP_ID = os.environ.get("LARK_APP_ID")
LARK_APP_SECRET = os.environ.get("LARK_APP_SECRET")
BOSS_OPEN_ID = os.environ.get("BOSS_OPEN_ID")
LARK_BASE_URL = "https://open.larksuite.com"
VN_TZ = timezone(timedelta(hours=7))

# Khoảng cách giữa 2 lần quét (phút) - luồng nền tự chạy
CRON_INTERVAL_MIN = int(os.environ.get("CRON_INTERVAL_MIN", 15))
# Nhắc trước hạn bao nhiêu phút (mặc định, có thể sếp đổi qua luật)
PHUT_NHAC_TRUOC_DEFAULT = int(os.environ.get("PHUT_NHAC_TRUOC", 15))
PORT = int(os.environ.get("PORT", 8080))

# --- Google Calendar (chạy THẲNG trong app này, không cần Vercel) ---
# Cấu hình nằm trong google_calendar.py, đọc từ env:
#   GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET / GOOGLE_REDIRECT_URI / GOOGLE_CAL_USER
# PUBLIC_URL: domain Railway của app này, dùng để gửi link cấp quyền cho sếp
PUBLIC_URL = os.environ.get("PUBLIC_URL", "").rstrip("/")
# =================================================

app = Flask(__name__)
mongo = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db = mongo["lark_bot"]
messages_col = db["messages"]
tasks_col = db["tasks"]
settings_col = db["settings"]
chat_history_col = db["chat_history"]   # trí nhớ hội thoại của sếp
pending_bookings_col = db["pending_bookings"]  # lịch họp đang chờ người đặt cung cấp email
known_contacts_col = db["known_contacts"]  # nhớ email theo từng người, để lần sau không hỏi lại
google_tokens_col = db["google_tokens"]    # refresh_token Google (kết nối 1 lần, dùng mãi)
reminded_events_col = db["reminded_events"]  # nhớ event Google đã nhắc, tránh nhắc trùng
gcal.init(db)
client = OpenAI(api_key=AI_API_KEY, base_url=AI_BASE_URL or None) if AI_API_KEY else None
print(f"🤖 AI: model={AI_MODEL} | endpoint={AI_BASE_URL or 'OpenAI mặc định'} | key={'có' if AI_API_KEY else 'THIẾU'}")

# Số cặp hỏi-đáp gần nhất bot nhớ (tiết kiệm)
SO_TIN_NHO = int(os.environ.get("SO_TIN_NHO", 5))

try:
    mongo.admin.command("ping")
    print("✅ Đã kết nối MongoDB thành công")
except Exception as e:
    print("❌ Lỗi MongoDB:", e)


# ==================== FILE LUẬT (SYSTEM PROMPT dạng văn bản tự do) ====================
DEFAULT_SYSTEM = {
    "_id": "system",
    "noi_dung": (
        "QUY TẮC TRỢ LÝ (sếp có thể sửa bằng cách nhắn bot):\n"
        "- Giờ gửi báo cáo tổng hợp hàng ngày: 09:00\n"
        "- Nhắc trước deadline: 15 phút\n"
        "- Phong cách: thân thiện, ấm áp, quan tâm sức khỏe sếp, xưng 'em' gọi 'sếp'\n"
        "- Báo cáo hàng ngày gồm: tổng hợp việc/tin trong ngày + việc sắp tới hạn + lời chào ấm áp\n"
    ),
    "gio_bao_cao": "09:00",        # trích ra để hệ thống biết giờ gửi báo cáo
    "phut_nhac_truoc": PHUT_NHAC_TRUOC_DEFAULT,
    "bao_cao_lan_cuoi": "",        # ngày đã gửi báo cáo gần nhất (tránh gửi trùng)
}

# Nhấn mạnh xưng hô: model nhỏ (Groq/OSS) hay trôi sang "anh/chị/cô" nếu chỉ dặn nhẹ
QUY_TAC_XUNG_HO = (
    "QUY TẮC XƯNG HÔ BẮT BUỘC: luôn tự xưng là 'em', luôn gọi người đối thoại là 'sếp'. "
    "TUYỆT ĐỐI KHÔNG dùng 'anh', 'chị', 'cô', 'bạn', 'quý khách' để gọi người đối thoại. "
    "Viết văn xuôi thuần, KHÔNG dùng markdown (không dùng ** * # -) vì Lark không hiển thị được. "
)


def get_system():
    s = settings_col.find_one({"_id": "system"})
    if not s:
        settings_col.insert_one(dict(DEFAULT_SYSTEM))
        return dict(DEFAULT_SYSTEM)
    return s

def update_system(changes):
    settings_col.update_one({"_id": "system"}, {"$set": changes}, upsert=True)


# ==================== TRÍ NHỚ HỘI THOẠI (5 cặp gần nhất) ====================
def lay_lich_su():
    """Lấy các cặp hỏi-đáp gần nhất của sếp, dạng list message cho GPT."""
    rows = list(chat_history_col.find().sort("created_at", -1).limit(SO_TIN_NHO))
    rows.reverse()  # cũ -> mới
    msgs = []
    for r in rows:
        msgs.append({"role": "user", "content": r.get("hoi", "")})
        msgs.append({"role": "assistant", "content": r.get("dap", "")})
    return msgs

def luu_lich_su(hoi, dap):
    chat_history_col.insert_one({
        "hoi": hoi, "dap": dap,
        "created_at": datetime.now(timezone.utc),
    })
    # Dọn bớt: chỉ giữ SO_TIN_NHO * 2 bản ghi gần nhất cho gọn
    tong = chat_history_col.count_documents({})
    if tong > SO_TIN_NHO * 2:
        cu = list(chat_history_col.find().sort("created_at", 1).limit(tong - SO_TIN_NHO * 2))
        for c in cu:
            chat_history_col.delete_one({"_id": c["_id"]})

def xoa_lich_su():
    chat_history_col.delete_many({})


# ==================== GỬI TIN CHO SẾP ====================
def gui_sep(text):
    if not (LARK_APP_ID and LARK_APP_SECRET and BOSS_OPEN_ID):
        print("⚠️ Thiếu cấu hình Lark"); return
    try:
        tok = requests.post(
            f"{LARK_BASE_URL}/open-apis/auth/v3/tenant_access_token/internal",
            json={"app_id": LARK_APP_ID, "app_secret": LARK_APP_SECRET}, timeout=10
        ).json().get("tenant_access_token")
        if not tok:
            print("⚠️ Không lấy được token Lark"); return
        requests.post(
            f"{LARK_BASE_URL}/open-apis/im/v1/messages?receive_id_type=open_id",
            headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
            json={"receive_id": BOSS_OPEN_ID, "msg_type": "text",
                  "content": json.dumps({"text": text})}, timeout=10
        )
        print(f"🔔 Đã gửi sếp: {text[:40]}")
    except Exception as e:
        print(f"⚠️ Lỗi gửi sếp: {e}")


def _fmt(iso):
    try:
        return datetime.fromisoformat(iso).strftime("%H:%M ngày %d/%m")
    except Exception:
        return iso


# ==================== TẠO SỰ KIỆN GOOGLE CALENDAR (khi có người đặt lịch) ====================
def tao_su_kien_google(summary, bat_dau_iso, ket_thuc_iso, description="", attendee_email=None):
    """
    Tạo sự kiện Google Calendar NGAY TRONG APP NÀY (module google_calendar.py).
    - Tự check trùng lịch (Google freeBusy)
    - Mời người đặt qua email (Google tự gửi lời mời)
    - Tự tạo link Google Meet
    Trả về dict có 'success' / 'duplicate' / 'error', hoặc None nếu chưa cấu hình.
    """
    if not gcal.da_cau_hinh():
        print("ℹ️ Chưa cấu hình GOOGLE_CLIENT_ID/SECRET/REDIRECT_URI -> bỏ qua tạo lịch Google")
        return None

    if not gcal.da_ket_noi():
        link = f"{PUBLIC_URL}/auth-google" if PUBLIC_URL else "/auth-google"
        gui_sep("⚠️ Em chưa kết nối được Google Calendar của sếp ạ. "
                f"Sếp bấm link này cấp quyền giúp em 1 lần thôi nhé: {link}")
        print("⚠️ Chưa có refresh_token Google -> đã nhắc sếp cấp quyền")
        return None

    kq = gcal.tao_su_kien(
        summary, bat_dau_iso, ket_thuc_iso,
        description=description,
        attendee_email=attendee_email,
        tao_meet=True,        # bỏ dòng này nếu không cần link Google Meet
    )
    if kq.get("success"):
        print(f"📅 Đã tạo sự kiện Google Calendar: {kq.get('link')}")
    elif kq.get("duplicate"):
        print(f"⏭️ Trùng lịch, không tạo: {kq.get('message')}")
    else:
        print(f"⚠️ Lỗi tạo sự kiện Google Calendar: {kq.get('error')}")
    return kq


# ==================== GỬI PHẢN HỒI CHO NGƯỜI ĐẶT LỊCH ====================
ZALO_BOT_TOKEN = os.environ.get("ZALO_BOT_TOKEN")

def gui_nguoi_dat(platform, chat_id, text):
    """Gửi tin phản hồi lại cho người vừa đặt lịch, theo đúng kênh họ nhắn."""
    try:
        if platform == "lark":
            tok = requests.post(
                f"{LARK_BASE_URL}/open-apis/auth/v3/tenant_access_token/internal",
                json={"app_id": LARK_APP_ID, "app_secret": LARK_APP_SECRET}, timeout=10
            ).json().get("tenant_access_token")
            if tok:
                requests.post(
                    f"{LARK_BASE_URL}/open-apis/im/v1/messages?receive_id_type=chat_id",
                    headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
                    json={"receive_id": chat_id, "msg_type": "text",
                          "content": json.dumps({"text": text})}, timeout=10)
        elif platform == "zalo" and ZALO_BOT_TOKEN:
            requests.post(
                f"https://bot-api.zapps.me/bot{ZALO_BOT_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": text}, timeout=10)
        print(f"↩️ Đã báo người đặt ({platform})")
    except Exception as e:
        print(f"⚠️ Lỗi báo người đặt: {e}")


# ==================== AI PHÂN TÍCH 1 TIN ====================
def _phan_tich(text, sender):
    now_str = datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M (%A)")
    if not client:
        return {"bao_sep": False, "co_deadline": False, "deadline_iso": None, "tom_tat": text}
    system = (
        f"Bây giờ là {now_str} (giờ VN UTC+7). Phân tích tin nhắn, trả JSON:\n"
        "- bao_sep: true nếu là công việc/việc gấp/họp cần sếp biết, false nếu chào hỏi/tán gẫu\n"
        "- tom_tat: tóm tắt NGẮN GỌN nội dung việc/họp (chỉ nêu việc gì, KHÔNG nhắc lại tên người gửi hay chữ 'đặt lịch'), vd 'Họp với đối tác về hợp đồng mới'\n"
        "- co_deadline: true nếu có nhắc thời hạn\n"
        "- loai_viec: 'hop' nếu là cuộc họp/lịch hẹn, 'cong_viec' nếu là việc cần làm/nộp, 'khac' nếu khác\n"
        "- deadline_iso: nếu có, tính mốc tuyệt đối ISO8601 offset +07:00 "
        "(vd bây giờ 11:00 việc 'xong trong 2h' -> '...T13:00:00+07:00'), không thì null\n"
        "- bat_dau_iso: (CHỈ với loai_viec='hop') giờ BẮT ĐẦU họp dạng ISO8601 +07:00, không rõ thì null\n"
        "- ket_thuc_iso: (CHỈ với loai_viec='hop') giờ KẾT THÚC họp dạng ISO8601 +07:00. "
        "Tính từ giờ bắt đầu + thời lượng (vd 'họp 10h trong 1 tiếng' -> bắt đầu 10:00, kết thúc 11:00). "
        "Nếu KHÔNG rõ thời lượng thì null\n"
        "- du_thong_tin_hop: (CHỈ với loai_viec='hop') true nếu tin có ĐỦ: giờ bắt đầu + thời lượng + nội dung họp; false nếu thiếu\n"
        "CHỈ trả JSON."
    )
    try:
        r = client.chat.completions.create(
            model=AI_MODEL,
            messages=[{"role": "system", "content": system},
                      {"role": "user", "content": f"{sender}: {text}"}],
            response_format={"type": "json_object"},
        )
        return json.loads(r.choices[0].message.content)
    except Exception as e:
        print(f"⚠️ Lỗi phân tích: {e}")
        return {"bao_sep": False, "co_deadline": False, "deadline_iso": None, "tom_tat": text}


# ==================== XỬ LÝ MỘT TIN (realtime) ====================
def _chuan_hoa_ten(ten):
    return (ten or "").strip().lower()


def _tim_email_da_luu(platform, sender_id, sender_name):
    """Tìm email đã lưu trước đó của người này (ưu tiên theo sender_id, sau đó theo tên)."""
    if sender_id:
        row = known_contacts_col.find_one({"platform": platform, "sender_id": str(sender_id)})
        if row and row.get("email"):
            return row["email"]
    row = known_contacts_col.find_one({"platform": platform, "ten_chuan_hoa": _chuan_hoa_ten(sender_name)})
    if row and row.get("email"):
        return row["email"]
    return None


def _luu_email(platform, sender_id, sender_name, email):
    """Lưu/ghi đè email của người này để lần đặt lịch sau không cần hỏi lại."""
    known_contacts_col.update_one(
        {"platform": platform, "sender_id": str(sender_id) if sender_id else "", "ten_chuan_hoa": _chuan_hoa_ten(sender_name)},
        {"$set": {
            "platform": platform, "sender_id": str(sender_id) if sender_id else "",
            "ten_chuan_hoa": _chuan_hoa_ten(sender_name), "sender_name": sender_name,
            "email": email, "updated_at": datetime.now(timezone.utc),
        }}, upsert=True,
    )


def xu_ly_tin(tin):
    # Khóa chống trùng: đánh dấu processed NGAY, chỉ xử lý nếu chưa bị đánh dấu
    kq_lock = messages_col.update_one(
        {"_id": tin["_id"], "processed": {"$ne": True}},
        {"$set": {"processed": True}},
    )
    if kq_lock.modified_count == 0:
        return  # tin đã được xử lý bởi lần gọi khác -> bỏ qua

    text = tin.get("text", "")
    sender = tin.get("sender_name", "Ẩn danh")
    platform = tin.get("platform", tin.get("source", "?"))
    sender_id = tin.get("sender_id", "")

    # Nếu là sếp nhắn -> có thể là lệnh / hỏi dữ liệu
    if BOSS_OPEN_ID and sender_id == BOSS_OPEN_ID:
        _thu_xu_ly_lenh(text)
        return

    chat_id = tin.get("chat_id", "")
    message_id = tin.get("message_id", "")

    # ===== ĐANG CHỜ EMAIL CỦA MỘT LỊCH HỌP TRƯỚC ĐÓ? =====
    # Nếu người này vừa được hỏi email cho 1 lịch họp -> tin nhắn tiếp theo được coi là email trả lời
    cho_email = pending_bookings_col.find_one({"chat_id": str(chat_id), "platform": platform})
    if cho_email:
        m = EMAIL_REGEX.search(text)
        if not m:
            gui_nguoi_dat(platform, chat_id,
                "Dạ em chưa nhận được email Google hợp lệ ạ. Anh/chị cho em xin lại email Google "
                "để em gửi lời mời họp qua Calendar nhé (vd: ten@gmail.com). Em cảm ơn!")
            print("⚠️ Chờ email hợp lệ, chưa hoàn tất đặt lịch")
            return
        _hoan_tat_dat_lich(cho_email, m.group(0))
        pending_bookings_col.delete_one({"_id": cho_email["_id"]})
        return

    kq = _phan_tich(text, sender)

    # ===== XỬ LÝ RIÊNG CHO CUỘC HỌP =====
    if kq.get("loai_viec") == "hop":
        # Kiểm tra đủ thông tin họp (giờ bắt đầu + thời lượng + nội dung)
        if not kq.get("du_thong_tin_hop") or not kq.get("bat_dau_iso") or not kq.get("ket_thuc_iso"):
            gui_nguoi_dat(platform, chat_id,
                "Dạ để đặt lịch họp cho sếp, anh/chị vui lòng ghi rõ giúp em:\n"
                "1) Giờ bắt đầu\n2) Họp trong bao lâu\n3) Nội dung họp\n"
                "Ví dụ: 'Họp lúc 10h trong 1 tiếng về kế hoạch Marketing'. Em cảm ơn ạ!")
            print("⚠️ Lịch họp thiếu thông tin, đã yêu cầu bổ sung")
            return

        # Chống tạo lịch họp trùng lặp (cùng người + cùng giờ bắt đầu + chưa xong)
        da_ton_tai = tasks_col.find_one({
            "loai_viec": "hop", "nguoi_gui": sender,
            "bat_dau": kq["bat_dau_iso"], "hoan_thanh": False,
        })
        if da_ton_tai:
            print(f"⏭️ Bỏ qua lịch họp trùng: {kq.get('tom_tat')}")
            return

        # --- Đã có email của người này từ lần đặt lịch trước? -> chốt lịch luôn, khỏi hỏi lại ---
        email_da_luu = _tim_email_da_luu(platform, sender_id, sender)
        if email_da_luu:
            _hoan_tat_dat_lich({
                "platform": platform, "chat_id": chat_id, "nguoi_gui": sender,
                "message_id": message_id, "viec": kq.get("tom_tat", text),
                "bat_dau_iso": kq["bat_dau_iso"], "ket_thuc_iso": kq["ket_thuc_iso"],
                "sender_id": sender_id,
            }, email_da_luu)
            print(f"✅ Dùng email đã lưu trước đó ({email_da_luu}), khỏi hỏi lại")
            return

        # Chưa chốt ngay - lưu tạm thông tin họp, chờ người đặt cung cấp email Google
        pending_bookings_col.update_one(
            {"chat_id": str(chat_id), "platform": platform},
            {"$set": {
                "chat_id": str(chat_id), "platform": platform, "nguoi_gui": sender,
                "sender_id": sender_id, "message_id": message_id,
                "viec": kq.get("tom_tat", text),
                "bat_dau_iso": kq["bat_dau_iso"], "ket_thuc_iso": kq["ket_thuc_iso"],
                "created_at": datetime.now(timezone.utc),
            }}, upsert=True,
        )
        gui_nguoi_dat(platform, chat_id,
            f"Dạ em đã ghi nhận lịch họp \"{kq.get('tom_tat', text)}\" lúc {_fmt(kq['bat_dau_iso'])}. "
            "Anh/chị cho em xin email Google để em gửi lời mời họp qua Calendar nhé, "
            "sau khi có email em sẽ chốt lịch và báo sếp ngay ạ. Em cảm ơn!")
        print(f"⏳ Chờ email để hoàn tất lịch họp: {kq.get('tom_tat')}")
        return

    # ===== CÔNG VIỆC CÓ DEADLINE (không phải họp) =====
    if kq.get("co_deadline") and kq.get("deadline_iso"):
        # Chống trùng: đã có việc cùng người gửi + cùng hạn + chưa xong thì bỏ qua
        da_ton_tai = tasks_col.find_one({
            "nguoi_gui": sender,
            "deadline": kq["deadline_iso"],
            "hoan_thanh": False,
        })
        if da_ton_tai:
            print(f"⏭️ Bỏ qua việc trùng: {kq.get('tom_tat')}")
            return

        tasks_col.update_one(
            {"nguon_message_id": tin.get("message_id")},
            {"$setOnInsert": {
                "nguon_message_id": tin.get("message_id"),
                "platform": platform, "nguoi_gui": sender,
                "viec": kq.get("tom_tat", text), "deadline": kq["deadline_iso"],
                "loai_viec": kq.get("loai_viec", "cong_viec"),
                "da_nhac_som": False, "da_nhac_sat": False, "hoan_thanh": False,
                "created_at": datetime.now(timezone.utc),
            }}, upsert=True,
        )
        print(f"📅 Ghi việc: {kq.get('tom_tat')} - hạn {kq['deadline_iso']}")
        if kq.get("bao_sep"):
            gui_sep(f"🔔 [{platform}] Việc mới từ {sender}:\n{kq['tom_tat']}\n⏰ Hạn: {_fmt(kq['deadline_iso'])}")
    elif kq.get("bao_sep"):
        gui_sep(f"🔔 [{platform}] Tin quan trọng từ {sender}:\n{kq['tom_tat']}")


def _hoan_tat_dat_lich(cho, email):
    """Gọi khi người đặt lịch vừa cung cấp email hợp lệ -> chốt lịch thật sự:
    ghi vào tasks, tạo sự kiện Google Calendar (mời email đó), báo sếp + xác nhận người đặt."""
    platform = cho.get("platform")
    chat_id = cho.get("chat_id")
    sender = cho.get("nguoi_gui", "Ẩn danh")
    viec = cho.get("viec", "")
    bat_dau_iso = cho.get("bat_dau_iso")
    ket_thuc_iso = cho.get("ket_thuc_iso")

    ket_qua_luu = tasks_col.update_one(
        {"nguon_message_id": cho.get("message_id")},
        {"$setOnInsert": {
            "nguon_message_id": cho.get("message_id"),
            "platform": platform, "nguoi_gui": sender, "email_nguoi_dat": email,
            "viec": viec, "deadline": bat_dau_iso,
            "bat_dau": bat_dau_iso, "ket_thuc": ket_thuc_iso,
            "loai_viec": "hop", "chat_id": chat_id,
            "da_nhac_som": False, "da_nhac_sat": False, "hoan_thanh": False,
            "created_at": datetime.now(timezone.utc),
        }}, upsert=True,
    )

    # --- TỰ TẠO SỰ KIỆN + MỜI NGƯỜI ĐẶT QUA EMAIL TRÊN GOOGLE CALENDAR ---
    gg = None
    if ket_qua_luu.upserted_id:
        gg = tao_su_kien_google(
            viec, bat_dau_iso, ket_thuc_iso,
            description=f"Người đặt lịch: {sender} (qua {platform}) - {email}",
            attendee_email=email,
        )
        if gg and gg.get("success"):
            tasks_col.update_one(
                {"_id": ket_qua_luu.upserted_id},
                {"$set": {"gg_event_id": gg.get("event_id"), "gg_event_link": gg.get("link")}},
            )

    link_txt = f"\n🔗 Đã lên Google Calendar (đã mời {email}): {gg['link']}" if gg and gg.get("success") else \
                f"\n⚠️ Chưa mời được qua Google Calendar (email: {email})"

    # Ghi nhớ email của người này, để lần đặt lịch sau khỏi phải hỏi lại
    _luu_email(platform, cho.get("sender_id"), sender, email)

    # Báo sếp
    gui_sep(f"🔔 [{platform}] Lịch họp mới từ {sender} ({email}):\n{viec}\n"
            f"🕐 {_fmt(bat_dau_iso)} - {_fmt(ket_thuc_iso)}{link_txt}")
    # Xác nhận với người đặt
    gui_nguoi_dat(platform, chat_id,
        f"Dạ em đã chốt lịch họp lúc {_fmt(bat_dau_iso)} và gửi lời mời Google Calendar tới {email} rồi ạ. "
        "Em cảm ơn anh/chị!")
    print(f"✅ Đã hoàn tất đặt lịch (có email): {viec} - {email}")


def _thu_xu_ly_lenh(text):
    print(f"👤 Sếp nhắn: {text[:80]}")
    if not client:
        print("❌ OPENAI_API_KEY chưa cấu hình (client=None) -> không thể trả lời sếp")
        return
    sys = get_system()
    now_str = datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M (%A)")
    system = (
        f"Bây giờ là {now_str} (giờ VN UTC+7). "
        "Sếp đang nhắn cho trợ lý. Đây là QUY TẮC hiện tại của trợ lý:\n"
        f"---\n{sys.get('noi_dung','')}\n---\n\n"
        "Phân loại câu của sếp:\n"
        "- 'tao_lich': sếp muốn TẠO MỚI một lịch họp / cuộc hẹn / việc cần làm "
        "(vd 'đặt lịch test chiều nay 17h', 'set lịch họp với GIP 9h sáng mai', 'nhắc tôi nộp báo cáo 5h chiều')\n"
        "- 'doi_luat': sếp ra lệnh thay đổi quy tắc (đổi giờ báo cáo, phút nhắc, thêm quy tắc...)\n"
        "- 'hoi_du_lieu': sếp hỏi thông tin/thống kê (ai nhắn gì, việc nào tới hạn, tổng hợp...)\n"
        "- 'danh_dau_xong': sếp báo đã hoàn thành một việc (vd 'việc báo cáo xong rồi', 'làm xong họp team')\n"
        "- 'doi_lich': sếp muốn ĐỔI/LÙI giờ một cuộc họp (vd 'lùi lịch họp với Long sang 14h', 'dời cuộc họp 12h sang 15h')\n"
        "- 'huy_lich': sếp muốn HỦY/XÓA một lịch/cuộc họp (vd 'hủy lịch họp với GIP', 'xóa cuộc họp 3h chiều', 'bỏ lịch họp mai giúp tôi')\n"
        "- 'quen_di': sếp muốn xóa lịch sử trò chuyện (vd 'quên hết đi', 'bắt đầu lại', 'reset')\n"
        "- 'khac': trò chuyện thường\n"
        "Trả JSON: {loai: 'tao_lich'|'doi_luat'|'hoi_du_lieu'|'danh_dau_xong'|'doi_lich'|'huy_lich'|'quen_di'|'khac', ...}\n"
        "Nếu 'tao_lich', thêm: viec (tên việc/nội dung họp NGẮN GỌN), "
        "loai_viec ('hop' nếu là cuộc họp/hẹn gặp, 'cong_viec' nếu là việc cần làm), "
        "bat_dau_iso (giờ bắt đầu ISO8601 +07:00, tính tuyệt đối từ thời điểm hiện tại ở trên), "
        "ket_thuc_iso (giờ kết thúc ISO8601 +07:00 nếu sếp có nói thời lượng, không rõ thì null), "
        "deadline_iso (với 'cong_viec' là hạn chót ISO8601 +07:00; với 'hop' thì bằng bat_dau_iso).\n"
        "Nếu 'doi_luat', thêm: noi_dung_moi (toàn bộ quy tắc sau cập nhật), gio_bao_cao (HH:MM), "
        "phut_nhac_truoc (số), xac_nhan (câu xác nhận ấm áp).\n"
        "Nếu 'danh_dau_xong', thêm: mo_ta_viec (mô tả việc sếp báo xong, để tìm trong danh sách).\n"
        "Nếu 'doi_lich', thêm: mo_ta_lich (mô tả cuộc họp cần đổi - tên người hoặc giờ cũ), "
        "gio_moi_iso (giờ mới bắt đầu dạng ISO8601 +07:00).\n"
        "Nếu 'huy_lich', thêm: mo_ta_lich (mô tả lịch cần hủy - tên người hoặc giờ).\n"
        "CHỈ trả JSON."
    )
    try:
        r = client.chat.completions.create(
            model=AI_MODEL,
            messages=[{"role": "system", "content": system},
                      {"role": "user", "content": text}],
            response_format={"type": "json_object"},
        )
        kq = json.loads(r.choices[0].message.content)
        loai = kq.get("loai", "khac")

        if loai == "tao_lich":
            _tao_lich_cho_sep(kq)

        elif loai == "doi_luat":
            changes = {}
            if kq.get("noi_dung_moi"):
                changes["noi_dung"] = kq["noi_dung_moi"]
            if kq.get("gio_bao_cao"):
                changes["gio_bao_cao"] = kq["gio_bao_cao"]
            if kq.get("phut_nhac_truoc") is not None:
                changes["phut_nhac_truoc"] = int(kq["phut_nhac_truoc"])
            if changes:
                update_system(changes)
                gui_sep(f"✅ {kq.get('xac_nhan', 'Dạ em đã cập nhật quy tắc ạ')}")
                print(f"⚙️ Sếp đổi quy tắc: {list(changes.keys())}")

        elif loai == "hoi_du_lieu":
            tra_loi = _tra_loi_du_lieu(text)
            gui_sep(tra_loi)
            print("📊 Đã trả lời câu hỏi dữ liệu của sếp")

        elif loai == "danh_dau_xong":
            mo_ta = kq.get("mo_ta_viec", text)
            _danh_dau_viec_xong(mo_ta)

        elif loai == "doi_lich":
            _doi_lich_hop(kq.get("mo_ta_lich", ""), kq.get("gio_moi_iso"))

        elif loai == "huy_lich":
            _huy_lich_hop(kq.get("mo_ta_lich", ""))

        elif loai == "quen_di":
            xoa_lich_su()
            gui_sep("✅ Dạ em đã xóa lịch sử trò chuyện, mình bắt đầu lại nhé sếp ạ!")
            print("🧹 Đã xóa trí nhớ hội thoại")

        else:  # 'khac' -> trò chuyện chung chung, GPT trả lời tự do
            tra_loi = _tro_chuyen_chung(text)
            gui_sep(tra_loi)
            print("💬 Đã trả lời trò chuyện chung")

    except Exception as e:
        print(f"⚠️ Lỗi hiểu lệnh: {e}")


def _cong_phut_iso(iso_str, phut):
    """Cộng thêm 'phut' phút vào một mốc ISO8601, trả lại chuỗi ISO (giữ offset +07:00)."""
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=VN_TZ)
        return (dt + timedelta(minutes=phut)).isoformat()
    except Exception:
        return None


THOI_LUONG_MAC_DINH_PHUT = int(os.environ.get("THOI_LUONG_HOP_PHUT", 60))


def _tao_lich_cho_sep(kq):
    """Sếp tự yêu cầu đặt lịch/giao việc -> GHI vào tasks + ĐẨY LÊN Google Calendar.
    (Trước đây rơi vào nhánh trò chuyện nên bot chỉ nói 'em đã ghi' mà không lưu gì.)"""
    viec = (kq.get("viec") or "").strip() or "Lịch sếp đặt"
    loai = kq.get("loai_viec") or "cong_viec"
    bat_dau = kq.get("bat_dau_iso")
    ket_thuc = kq.get("ket_thuc_iso")
    han = kq.get("deadline_iso") or bat_dau

    if not han:
        gui_sep("Dạ sếp cho em xin giờ cụ thể để em ghi lịch nhé ạ "
                "(vd 'đặt lịch họp 15h chiều nay trong 1 tiếng').")
        return

    # Giờ bắt đầu/kết thúc để lên Google Calendar. Sếp không nói thời lượng -> mặc định 60 phút.
    bd_iso = bat_dau or han
    kt_iso = ket_thuc or _cong_phut_iso(bd_iso, THOI_LUONG_MAC_DINH_PHUT)

    khoa = f"sep::{han}::{viec[:40].lower()}"   # chống ghi trùng cùng việc + cùng giờ
    doc = {
        "nguon_message_id": khoa,
        "platform": "lark", "nguoi_gui": "Sếp",
        "viec": viec, "deadline": han, "loai_viec": loai,
        "bat_dau": bd_iso, "ket_thuc": kt_iso,
        "da_nhac_som": False, "da_nhac_sat": False, "hoan_thanh": False,
        "created_at": datetime.now(timezone.utc),
    }
    luu = tasks_col.update_one({"nguon_message_id": khoa}, {"$setOnInsert": doc}, upsert=True)
    if luu.upserted_id is None:
        gui_sep(f"Dạ việc \"{viec}\" lúc {_fmt(han)} em đã ghi từ trước rồi ạ, sếp yên tâm nhé.")
        return

    # --- ĐẨY LÊN GOOGLE CALENDAR CỦA SẾP (đúng giờ bắt đầu/kết thúc) ---
    link_txt = ""
    if gcal.da_cau_hinh():
        if gcal.da_ket_noi():
            gg = tao_su_kien_google(viec, bd_iso, kt_iso, description="Sếp tự đặt qua trợ lý")
            if gg and gg.get("success"):
                tasks_col.update_one({"_id": luu.upserted_id},
                                     {"$set": {"gg_event_id": gg.get("event_id"),
                                               "gg_event_link": gg.get("link")}})
                link_txt = "\n📅 Đã thêm vào Google Calendar của sếp rồi ạ."
            elif gg and gg.get("duplicate"):
                link_txt = "\n📅 (Google Calendar báo sếp đã có lịch khác trùng khung giờ này.)"
            else:
                link_txt = "\n⚠️ Em ghi vào lịch rồi nhưng chưa đưa lên Google Calendar được, em sẽ thử lại sau ạ."
        else:
            # Chưa cấp quyền Google -> tao_su_kien_google sẽ tự gửi link cho sếp; ở đây chỉ nhắc thêm
            tao_su_kien_google(viec, bd_iso, kt_iso)
            link_txt = "\n⚠️ Để lịch tự lên Google Calendar, sếp bấm link cấp quyền em vừa gửi giúp em nhé ạ."

    phut = int(get_system().get("phut_nhac_truoc", PHUT_NHAC_TRUOC_DEFAULT))
    khung_gio = _fmt(bd_iso) + (f" – {_fmt(kt_iso)}" if kt_iso else "")
    gui_sep(f"✅ Dạ em đã ghi vào lịch của sếp ạ:\n"
            f"Nội dung: {viec}\n"
            f"Thời gian: {khung_gio}\n"
            f"Em sẽ nhắc sếp trước {phut} phút.{link_txt}")
    print(f"📌 Sếp đặt lịch: {viec} - {bd_iso} -> {kt_iso}")


def _tro_chuyen_chung(cau_hoi):
    """Sếp hỏi chung chung (kiến thức, tư vấn, viết lách...). GPT trả lời tự do."""
    if not client:
        return "Dạ em chưa trả lời được câu này ạ."
    sys = get_system()
    system = (
        "Bạn là trợ lý cá nhân thân thiện của sếp, tên TingTing. "
        f"Phong cách: {sys.get('noi_dung','')[:200]}. "
        "Sếp có thể hỏi bất cứ điều gì (kiến thức, tư vấn, viết lách, gợi ý...). "
        "Trả lời hữu ích, tự nhiên, bằng tiếng Việt. " + QUY_TAC_XUNG_HO +
        "Lưu ý: em không có dữ liệu thời gian thực (thời tiết, giá cả, tin tức hôm nay), "
        "nếu sếp hỏi loại đó thì nói rõ em không tra cứu trực tiếp được và gợi ý sếp nguồn phù hợp. "
        "CỰC KỲ QUAN TRỌNG: trong lúc trò chuyện em KHÔNG ghi được lịch/việc vào hệ thống. "
        "TUYỆT ĐỐI KHÔNG nói 'em đã ghi', 'em đã lưu', 'em đã đặt lịch', 'em đã ghi nhận' — "
        "vì thực tế không có gì được lưu, nói vậy là lừa sếp. "
        "Nếu sếp muốn đặt lịch/giao việc, hãy mời sếp nhắn rõ nội dung kèm giờ cụ thể để hệ thống ghi lại."
    )
    try:
        messages = [{"role": "system", "content": system}]
        messages += lay_lich_su()
        messages.append({"role": "user", "content": cau_hoi})
        r = client.chat.completions.create(model=AI_MODEL, messages=messages)
        tra_loi = r.choices[0].message.content.strip()
        luu_lich_su(cau_hoi, tra_loi)
        return tra_loi
    except Exception as e:
        print(f"⚠️ Lỗi trò chuyện: {e}")
        return "Dạ em chưa trả lời được câu này, sếp thử lại sau nhé ạ."


def _danh_dau_viec_xong(mo_ta):
    """Tìm việc khớp mô tả sếp báo, đánh dấu hoàn thành."""
    viec_list = list(tasks_col.find({"hoan_thanh": False}))
    if not viec_list:
        gui_sep("Dạ hiện không có việc nào đang chờ ạ.")
        return

    # Nhờ GPT chọn việc khớp nhất
    if client:
        ds = "\n".join(f"{i}. {v.get('viec','')}" for i, v in enumerate(viec_list))
        try:
            r = client.chat.completions.create(
                model=AI_MODEL,
                messages=[
                    {"role": "system", "content": (
                        "Sếp báo đã xong một việc. Chọn SỐ THỨ TỰ việc khớp nhất trong danh sách. "
                        "Trả JSON {chi_so: số, chac_chan: true/false}. "
                        "Nếu không việc nào khớp rõ, chac_chan=false."
                    )},
                    {"role": "user", "content": f"Sếp báo xong: {mo_ta}\n\nDanh sách:\n{ds}"},
                ],
                response_format={"type": "json_object"},
            )
            kq = json.loads(r.choices[0].message.content)
            idx = kq.get("chi_so")
            if kq.get("chac_chan") and idx is not None and 0 <= idx < len(viec_list):
                v = viec_list[idx]
                tasks_col.update_one({"_id": v["_id"]}, {"$set": {"hoan_thanh": True}})
                gui_sep(f"✅ Dạ em đã đánh dấu HOÀN THÀNH việc: {v.get('viec','')} ạ. Sếp giỏi quá!")
                print(f"✅ Đánh dấu xong: {v.get('viec','')}")
                return
        except Exception as e:
            print(f"⚠️ Lỗi đánh dấu xong: {e}")

    # Không tìm được việc rõ ràng
    ds_txt = "\n".join(f"- {v.get('viec','')}" for v in viec_list)
    gui_sep(f"Dạ em chưa rõ sếp báo xong việc nào. Các việc đang chờ:\n{ds_txt}\nSếp nói rõ hơn giúp em nhé ạ.")


def _doi_lich_hop(mo_ta, gio_moi_iso):
    """Sếp lùi/đổi giờ họp. Cập nhật lịch + báo người đặt + xác nhận sếp."""
    if not gio_moi_iso:
        gui_sep("Dạ sếp cho em biết giờ mới cụ thể để em dời lịch nhé ạ (vd 'dời sang 14h').")
        return

    ds_hop = list(tasks_col.find({"hoan_thanh": False}))
    if not ds_hop:
        gui_sep("Dạ hiện không có lịch nào để dời ạ.")
        return

    chon = None
    if client and mo_ta:
        ds = "\n".join(
            f"{i}. {v.get('viec','')} (bắt đầu {_fmt(v.get('bat_dau',''))}, từ {v.get('nguoi_gui','?')})"
            for i, v in enumerate(ds_hop)
        )
        try:
            r = client.chat.completions.create(
                model=AI_MODEL,
                messages=[
                    {"role": "system", "content": (
                        "Sếp muốn dời một cuộc họp. Chọn SỐ THỨ TỰ cuộc họp khớp nhất với mô tả. "
                        "Trả JSON {chi_so: số, chac_chan: bool}."
                    )},
                    {"role": "user", "content": f"Mô tả: {mo_ta}\n\nDanh sách:\n{ds}"},
                ],
                response_format={"type": "json_object"},
            )
            k = json.loads(r.choices[0].message.content)
            idx = k.get("chi_so")
            if k.get("chac_chan") and idx is not None and 0 <= idx < len(ds_hop):
                chon = ds_hop[idx]
        except Exception as e:
            print(f"⚠️ Lỗi chọn lịch: {e}")

    if not chon and len(ds_hop) == 1:
        chon = ds_hop[0]

    if not chon:
        ds_txt = "\n".join(f"- {v.get('viec','')} ({_fmt(v.get('bat_dau',''))})" for v in ds_hop)
        gui_sep(f"Dạ em chưa rõ sếp muốn dời lịch nào. Các lịch họp hiện có:\n{ds_txt}\nSếp nói rõ hơn giúp em ạ.")
        return

    gio_cu = chon.get("bat_dau", "")
    try:
        bd_cu = datetime.fromisoformat(gio_cu)
        kt_cu = datetime.fromisoformat(chon.get("ket_thuc", gio_cu))
        thoi_luong = kt_cu - bd_cu
        bd_moi = datetime.fromisoformat(gio_moi_iso)
        kt_moi = bd_moi + thoi_luong
        tasks_col.update_one({"_id": chon["_id"]}, {"$set": {
            "bat_dau": bd_moi.isoformat(), "ket_thuc": kt_moi.isoformat(),
            "deadline": bd_moi.isoformat(),
            "da_nhac_som": False, "da_nhac_sat": False,
        }})
        # Dời luôn sự kiện trên Google Calendar (Google tự báo lại cho khách)
        if chon.get("gg_event_id"):
            gcal.doi_gio_su_kien(chon["gg_event_id"], bd_moi.isoformat(), kt_moi.isoformat())
    except Exception:
        tasks_col.update_one({"_id": chon["_id"]}, {"$set": {
            "bat_dau": gio_moi_iso, "deadline": gio_moi_iso,
            "da_nhac_som": False, "da_nhac_sat": False,
        }})

    # Báo người đặt lịch
    gui_nguoi_dat(
        chon.get("platform", "lark"), chon.get("chat_id", ""),
        f"Dạ sếp muốn dời cuộc họp \"{chon.get('viec','')}\" sang {_fmt(gio_moi_iso)} ạ. "
        f"Anh/chị sắp xếp lại giúp em nhé, em cảm ơn!"
    )
    # Xác nhận với sếp
    gui_sep(
        f"✅ Dạ em đã dời cuộc họp \"{chon.get('viec','')}\"\n"
        f"Từ: {_fmt(gio_cu)}\nSang: {_fmt(gio_moi_iso)}\n"
        f"Và đã báo lại cho {chon.get('nguoi_gui','người đặt')} rồi ạ."
    )
    print(f"🔄 Đã dời lịch: {chon.get('viec','')} sang {gio_moi_iso}")


def _chon_lich_theo_mo_ta(mo_ta, ds):
    """Cho GPT chọn task khớp mô tả sếp nói. Trả task hoặc None.
    Nếu chỉ có 1 lịch thì chọn luôn."""
    if not ds:
        return None
    if len(ds) == 1:
        return ds[0]
    if not (client and mo_ta):
        return None
    danh_sach = "\n".join(
        f"{i}. {v.get('viec','')} (bắt đầu {_fmt(v.get('bat_dau', v.get('deadline','')))}, từ {v.get('nguoi_gui','?')})"
        for i, v in enumerate(ds)
    )
    try:
        r = client.chat.completions.create(
            model=AI_MODEL,
            messages=[
                {"role": "system", "content": (
                    "Sếp muốn thao tác một lịch/việc. Chọn SỐ THỨ TỰ khớp nhất với mô tả. "
                    "Trả JSON {chi_so: số, chac_chan: bool}. Không chắc thì chac_chan=false."
                )},
                {"role": "user", "content": f"Mô tả: {mo_ta}\n\nDanh sách:\n{danh_sach}"},
            ],
            response_format={"type": "json_object"},
        )
        k = json.loads(r.choices[0].message.content)
        idx = k.get("chi_so")
        if k.get("chac_chan") and idx is not None and 0 <= idx < len(ds):
            return ds[idx]
    except Exception as e:
        print(f"⚠️ Lỗi chọn lịch: {e}")
    return None


def _huy_lich_hop(mo_ta):
    """Sếp hủy/xóa một lịch -> XÓA sự kiện Google Calendar + đánh dấu hủy + báo người đặt."""
    ds = list(tasks_col.find({"hoan_thanh": False}))
    if not ds:
        gui_sep("Dạ hiện không có lịch nào để hủy ạ.")
        return

    chon = _chon_lich_theo_mo_ta(mo_ta, ds)
    if not chon:
        ds_txt = "\n".join(
            f"- {v.get('viec','')} ({_fmt(v.get('bat_dau', v.get('deadline','')))})" for v in ds
        )
        gui_sep(f"Dạ em chưa rõ sếp muốn hủy lịch nào. Các lịch hiện có:\n{ds_txt}\nSếp nói rõ hơn giúp em ạ.")
        return

    # Xóa sự kiện trên Google Calendar (nếu đã tạo)
    da_xoa_google = False
    if chon.get("gg_event_id") and gcal.da_cau_hinh() and gcal.da_ket_noi():
        kq = gcal.huy_su_kien(chon["gg_event_id"])
        da_xoa_google = bool(kq and kq.get("success"))

    # Đánh dấu hủy để dừng nhắc (không xóa hẳn để còn lưu vết)
    tasks_col.update_one({"_id": chon["_id"]},
                         {"$set": {"hoan_thanh": True, "da_huy": True,
                                   "da_nhac_som": True, "da_nhac_sat": True}})

    # Báo người đặt nếu là lịch do người ngoài đặt
    if chon.get("chat_id"):
        gui_nguoi_dat(chon.get("platform", "lark"), chon.get("chat_id", ""),
            f"Dạ cuộc họp \"{chon.get('viec','')}\" lúc "
            f"{_fmt(chon.get('bat_dau', chon.get('deadline','')))} đã được hủy ạ. Em cảm ơn anh/chị!")

    gg_txt = "\n🗑️ Đã xóa khỏi Google Calendar." if da_xoa_google else ""
    gui_sep(f"✅ Dạ em đã hủy lịch \"{chon.get('viec','')}\" "
            f"({_fmt(chon.get('bat_dau', chon.get('deadline','')))}) rồi ạ.{gg_txt}")
    print(f"🗑️ Đã hủy lịch: {chon.get('viec','')}")


def _tra_loi_du_lieu(cau_hoi):
    """Sếp hỏi -> đọc database -> GPT trả lời. CHỈ gọi khi đã xác thực là sếp."""
    now = datetime.now(VN_TZ)
    since = now - timedelta(days=7)  # dữ liệu 7 ngày gần nhất

    # Lấy tin nhắn gần đây
    tin = list(messages_col.find(
        {"created_at": {"$gte": since.astimezone(timezone.utc)}}
    ).sort("created_at", -1).limit(100))
    tin_txt = "\n".join(
        f"- [{t.get('platform', t.get('source','?'))}] "
        f"{t.get('sender_name','?')}: {t.get('text','')}" for t in tin
    ) or "(không có tin nào)"

    # Lấy việc chưa xong
    viec = list(tasks_col.find({"hoan_thanh": False}).sort("deadline", 1))
    viec_txt = "\n".join(
        f"- {v.get('viec','')} (hạn {_fmt(v.get('deadline',''))}, từ {v.get('nguoi_gui','?')})"
        for v in viec
    ) or "(không có việc nào đang chờ)"

    # Lấy LỊCH THẬT trên Google Calendar của sếp (14 ngày trước -> 30 ngày tới).
    # Bọc an toàn: đọc lỗi/chưa kết nối cũng KHÔNG làm vỡ câu trả lời.
    lich_txt = "(chưa kết nối Google Calendar)"
    try:
        evs = gcal.liet_ke_su_kien(
            tu_iso=(now - timedelta(days=14)).isoformat(),
            den_iso=(now + timedelta(days=60)).isoformat(),
        )
        if evs:
            lich_txt = "\n".join(
                f"- {_fmt(e.get('start',''))}: {e.get('summary','')}"
                + (f" @ {e.get('location')}" if e.get('location') else "")
                for e in evs
            )
        elif gcal.da_ket_noi():
            lich_txt = "(không có sự kiện nào trong khoảng 14 ngày trước → 30 ngày tới)"
    except Exception as e:
        print(f"⚠️ Lỗi đọc lịch Google cho câu hỏi sếp: {e}")

    thu = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"][now.weekday()]
    system = (
        f"Bạn là trợ lý của sếp. Bây giờ là {now.strftime('%H:%M')} {thu} ngày {now.strftime('%d/%m/%Y')}. "
        "Trả lời câu hỏi của sếp dựa trên dữ liệu dưới đây.\n"
        "CÁCH TRẢ LỜI THÔNG MINH:\n"
        "- Hiểu đúng khoảng thời gian sếp hỏi (hôm nay / ngày mai / tuần này / tuần sau / tháng 8...) "
        "và CHỈ nêu đúng khoảng đó, không liệt kê thừa các mốc ngoài phạm vi.\n"
        "- Về LỊCH/HỌP: dùng 'LỊCH GOOGLE CALENDAR' (lịch thật của sếp). Sắp xếp theo thời gian, "
        "gom theo từng ngày, ghi rõ giờ + tên + địa điểm (nếu có).\n"
        "- Nếu khoảng sếp hỏi không có sự kiện nào thì nói rõ 'khoảng đó sếp không có lịch'.\n"
        "- Bỏ qua các mục trùng lặp. Ngắn gọn, đúng trọng tâm, tiếng Việt. " + QUY_TAC_XUNG_HO + "\n\n"
        f"=== LỊCH GOOGLE CALENDAR CỦA SẾP (14 ngày trước → 60 ngày tới) ===\n{lich_txt}\n\n"
        f"=== VIỆC ĐANG CHỜ (do bot theo dõi trong hệ thống) ===\n{viec_txt}\n\n"
        f"=== TIN NHẮN 7 NGÀY QUA ===\n{tin_txt}"
    )
    try:
        # Ghép: system + lịch sử hội thoại (trí nhớ) + câu hỏi mới
        messages = [{"role": "system", "content": system}]
        messages += lay_lich_su()
        messages.append({"role": "user", "content": cau_hoi})

        r = client.chat.completions.create(model=AI_MODEL, messages=messages)
        tra_loi = r.choices[0].message.content.strip()
        luu_lich_su(cau_hoi, tra_loi)   # lưu vào trí nhớ
        return tra_loi
    except Exception as e:
        print(f"⚠️ Lỗi trả lời dữ liệu: {e}")
        return "Dạ em chưa lấy được dữ liệu, sếp thử lại sau nhé ạ."


# ==================== NHẮC DEADLINE (logic 2 lần, không lọt) ====================
def nhac_deadline():
    """
    Quét việc chưa xong. Với mỗi việc:
    - mốc cần nhắc = deadline - phut_truoc (mặc định 15p)
    - Nếu SẮP LỌT (mốc cần nhắc rơi vào trước lượt quét kế tiếp) -> nhắc ngay (lần sớm)
    - Nếu ĐANG trong khoảng 1..phut_truoc phút trước hạn -> nhắc (lần sát hạn)
    Mỗi việc nhắc tối đa 2 lần: 'sớm' và 'sát hạn'. Dùng field da_nhac_som / da_nhac_sat.
    """
    sys = get_system()
    phut_truoc = int(sys.get("phut_nhac_truoc", PHUT_NHAC_TRUOC_DEFAULT))
    now = datetime.now(VN_TZ)
    luot_sau = now + timedelta(minutes=CRON_INTERVAL_MIN)

    for v in tasks_col.find({"hoan_thanh": False}):
        try:
            han = datetime.fromisoformat(v["deadline"])
        except Exception:
            continue

        if han < now - timedelta(minutes=5):
            continue  # đã quá hạn, bỏ qua

        moc_can_nhac = han - timedelta(minutes=phut_truoc)
        con_phut = int((han - now).total_seconds() / 60)
        da_som = v.get("da_nhac_som", False)
        da_sat = v.get("da_nhac_sat", False)

        # --- Lần SÁT HẠN: đang trong khoảng 0..phut_truoc phút trước hạn ---
        if 0 <= con_phut <= phut_truoc and not da_sat:
            # Khóa atomic: chỉ gửi nếu chưa ai đánh dấu (chống trùng khi chạy song song)
            locked = tasks_col.update_one(
                {"_id": v["_id"], "da_nhac_sat": {"$ne": True}},
                {"$set": {"da_nhac_sat": True, "da_nhac_som": True}},
            )
            if locked.modified_count == 1:
                gui_sep(_soan_loi_nhac(v, con_phut, "sat"))
                print(f"⏰ Nhắc SÁT HẠN: {v['viec']}")
            continue

        # --- Lần SỚM: chỉ khi CÒN XA hơn khoảng sát hạn (tránh dồn 2 tin) ---
        if moc_can_nhac <= luot_sau and not da_som and con_phut > phut_truoc:
            locked = tasks_col.update_one(
                {"_id": v["_id"], "da_nhac_som": {"$ne": True}},
                {"$set": {"da_nhac_som": True}},
            )
            if locked.modified_count == 1:
                gui_sep(_soan_loi_nhac(v, con_phut, "som"))
                print(f"🔔 Nhắc SỚM: {v['viec']}")

    # Nhắc luôn các cuộc họp trên Google Calendar (lịch sếp tự thêm, không tạo qua bot)
    try:
        nhac_lich_google()
    except Exception as e:
        print(f"⚠️ Lỗi nhắc lịch Google: {e}")


def nhac_lich_google():
    """Nhắc sếp các cuộc họp SẮP TỚI trên Google Calendar (kể cả lịch không tạo qua bot).
    Chống nhắc trùng bằng collection reminded_events (khóa atomic theo event id)."""
    if not (gcal.da_cau_hinh() and gcal.da_ket_noi()):
        return
    sys = get_system()
    phut_truoc = int(sys.get("phut_nhac_truoc", PHUT_NHAC_TRUOC_DEFAULT))
    now = datetime.now(VN_TZ)
    # Nhìn trước = phút nhắc + 1 chu kỳ quét, để không lọt event nào giữa 2 lượt cron
    cua_so = phut_truoc + CRON_INTERVAL_MIN
    evs = gcal.liet_ke_su_kien(
        tu_iso=now.isoformat(),
        den_iso=(now + timedelta(minutes=cua_so)).isoformat(),
    )
    for e in evs:
        if e.get("ca_ngay"):   # bỏ sự kiện cả ngày (lễ, sinh nhật... không nhắc kiểu 15 phút)
            continue
        start = e.get("start")
        try:
            bd = datetime.fromisoformat(start)
        except Exception:
            continue
        con = int((bd - now).total_seconds() / 60)
        if con < 0:            # đã bắt đầu -> bỏ
            continue
        khoa = e.get("id") or f"{start}|{e.get('summary','')}"
        # Khóa atomic: chỉ nhắc nếu chưa từng nhắc event này
        locked = reminded_events_col.update_one(
            {"_id": khoa},
            {"$setOnInsert": {"_id": khoa, "start": start,
                              "reminded_at": datetime.now(timezone.utc)}},
            upsert=True,
        )
        if locked.upserted_id is None:
            continue           # đã nhắc rồi
        dia_diem = f"\nĐịa điểm: {e.get('location')}" if e.get("location") else ""
        gui_sep(
            f"🔔 TingTing nhắc sếp sắp có lịch ạ!\n"
            f"Nội dung: {e.get('summary','')}\n"
            f"Bắt đầu: {_fmt(start)} (còn ~{con} phút){dia_diem}\n"
            f"Sếp sắp xếp có mặt nhé, chúc sếp họp vui vẻ ạ! 😊"
        )
        print(f"🔔 Nhắc lịch Google: {e.get('summary')} ({con}')")

    # Dọn bản ghi nhắc cũ (> 7 ngày) cho gọn
    try:
        reminded_events_col.delete_many(
            {"reminded_at": {"$lt": datetime.now(timezone.utc) - timedelta(days=7)}}
        )
    except Exception:
        pass


def _soan_loi_nhac(v, con_phut, muc):
    """Soạn lời nhắc thân thiện theo loại việc (họp / công việc)."""
    loai = v.get("loai_viec", "cong_viec")
    viec = v.get("viec", "")
    han = _fmt(v.get("deadline", ""))
    con = max(con_phut, 0)

    if loai == "hop":
        than = (
            f"🔔 TingTing nhắc sếp có lịch họp sắp tới ạ!\n"
            f"Nội dung: {viec}\n"
            f"Bắt đầu lúc: {han} (còn ~{con} phút)\n"
            f"Sếp check qua giúp em và sắp xếp có mặt nhé, chúc sếp họp vui vẻ ạ! 😊"
        )
    else:
        than = (
            f"🔔 TingTing có việc sắp tới hạn sếp ơi!\n"
            f"Việc: {viec}\n"
            f"Hạn chót: {han} (còn ~{con} phút)\n"
            f"Sếp check qua hộ em nhé, cố lên sếp! 💪"
        )
    return than


# ==================== BÁO CÁO HÀNG NGÀY ĐÚNG GIỜ ====================
def kiem_tra_bao_cao_hang_ngay():
    """Nếu tới giờ báo cáo (theo system) và hôm nay chưa gửi -> soạn & gửi."""
    sys = get_system()
    gio_bao_cao = sys.get("gio_bao_cao", "09:00")
    now = datetime.now(VN_TZ)
    hom_nay = now.strftime("%Y-%m-%d")

    # Đã gửi hôm nay rồi thì thôi
    if sys.get("bao_cao_lan_cuoi") == hom_nay:
        return

    # Đã qua giờ báo cáo chưa?
    try:
        gio, phut = map(int, gio_bao_cao.split(":"))
    except Exception:
        gio, phut = 9, 0
    moc = now.replace(hour=gio, minute=phut, second=0, microsecond=0)

    # Chỉ gửi khi đã tới/qua giờ báo cáo (trong vòng 1 tiếng để tránh gửi muộn quá)
    if now >= moc and (now - moc) <= timedelta(hours=1):
        noi_dung = _soan_bao_cao(sys)
        if noi_dung:
            gui_sep(noi_dung)
        update_system({"bao_cao_lan_cuoi": hom_nay})
        print(f"📋 Đã gửi báo cáo hàng ngày ({gio_bao_cao})")


def _soan_bao_cao(sys):
    now = datetime.now(VN_TZ)
    # Lấy việc chưa xong + tin quan trọng gần đây
    viec = list(tasks_col.find({"hoan_thanh": False}).sort("deadline", 1))
    viec_txt = "\n".join(
        f"- {v.get('viec','')} (hạn {_fmt(v.get('deadline',''))})" for v in viec
    ) or "(không có việc nào đang chờ)"

    # Lịch HÔM NAY trên Google Calendar (từ đầu ngày -> cuối ngày). Bọc an toàn.
    lich_hom_nay = "(chưa kết nối Google Calendar)"
    try:
        dau_ngay = now.replace(hour=0, minute=0, second=0, microsecond=0)
        cuoi_ngay = now.replace(hour=23, minute=59, second=59, microsecond=0)
        evs = gcal.liet_ke_su_kien(tu_iso=dau_ngay.isoformat(), den_iso=cuoi_ngay.isoformat())
        if evs:
            lich_hom_nay = "\n".join(
                f"- {_fmt(e.get('start',''))}: {e.get('summary','')}"
                + (f" @ {e.get('location')}" if e.get('location') else "")
                for e in evs
            )
        elif gcal.da_ket_noi():
            lich_hom_nay = "(hôm nay không có lịch nào trên Google Calendar)"
    except Exception as e:
        print(f"⚠️ Lỗi đọc lịch Google cho báo cáo sáng: {e}")

    if not client:
        return f"📋 Báo cáo sáng:\nLịch hôm nay:\n{lich_hom_nay}\n\nViệc đang chờ:\n{viec_txt}"

    thu = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"][now.weekday()]
    system = (
        f"Bạn là trợ lý cá nhân của sếp. Bây giờ là {now.strftime('%H:%M')} {thu} ngày {now.strftime('%d/%m/%Y')}.\n"
        f"Quy tắc/phong cách:\n{sys.get('noi_dung','')}\n\n"
        "Soạn BÁO CÁO SÁNG cho sếp, gồm 2 phần: (1) LỊCH HÔM NAY của sếp (từ Google Calendar, "
        "liệt kê theo giờ, kèm địa điểm nếu có); (2) việc đang chờ / sắp tới hạn. "
        "Mở đầu bằng lời chào ấm áp, quan tâm sức khỏe. Kết bằng lời chúc ngày làm việc tốt. "
        "Ngắn gọn, tự nhiên, tiếng Việt, KHÔNG markdown. " + QUY_TAC_XUNG_HO
    )
    user = f"LỊCH HÔM NAY (Google Calendar):\n{lich_hom_nay}\n\nVIỆC ĐANG CHỜ:\n{viec_txt}"
    try:
        r = client.chat.completions.create(
            model=AI_MODEL,
            messages=[{"role": "system", "content": system},
                      {"role": "user", "content": user}],
        )
        return r.choices[0].message.content.strip()
    except Exception as e:
        print(f"⚠️ Lỗi soạn báo cáo: {e}")
        return f"📋 Báo cáo hôm nay:\nViệc đang chờ:\n{viec_txt}"


# ==================== CÁC ENDPOINT ====================
@app.route("/xu-ly", methods=["POST"])
def api_xu_ly():
    """Bot gọi ngay khi có tin mới (realtime). Body: {message_id: ...}
    Xử lý ở thread nền và trả về NGAY để bot không phải chờ GPT."""
    data = request.get_json(force=True, silent=True) or {}
    mid = data.get("message_id")
    print(f"📥 /xu-ly được gọi. message_id={mid} | payload={data}")

    def _xu_ly_nen():
        try:
            if mid:
                tin = messages_col.find_one({"message_id": mid, "processed": {"$ne": True}})
                if tin:
                    xu_ly_tin(tin)
                else:
                    print(f"⚠️ Không tìm thấy tin chưa xử lý với message_id={mid} (tin chưa được ghi vào Mongo, hoặc đã processed)")
            else:
                so_tin = 0
                for tin in messages_col.find({"processed": {"$ne": True}}).sort("created_at", 1):
                    so_tin += 1
                    xu_ly_tin(tin)
                print(f"ℹ️ Quét tin chưa xử lý (không kèm message_id): {so_tin} tin")
        except Exception as e:
            print(f"⚠️ Lỗi xử lý nền: {e}")

    threading.Thread(target=_xu_ly_nen, daemon=True).start()
    return jsonify({"ok": True})


@app.route("/nhac", methods=["GET", "POST"])
def api_nhac():
    """Cron ngoài (cron-job.org) gọi định kỳ để nhắc deadline + kiểm tra báo cáo sáng.
    Bắt buộc trên serverless (Vercel) vì không có luồng nền."""
    nhac_deadline()
    kiem_tra_bao_cao_hang_ngay()

    # LƯỚI AN TOÀN (thay change stream trên serverless): vớt tin MỚI (30 phút gần đây)
    # mà webhook lỡ chưa xử lý. Giới hạn 30' để KHÔNG đụng tin cũ; khóa 'processed' chống trùng.
    try:
        moc = datetime.now(timezone.utc) - timedelta(minutes=30)
        so = 0
        for tin in messages_col.find(
            {"processed": {"$ne": True}, "created_at": {"$gte": moc}}
        ).sort("created_at", 1):
            xu_ly_tin(tin)
            so += 1
        if so:
            print(f"🛟 Lưới an toàn /nhac: vớt {so} tin lỡ")
    except Exception as e:
        print(f"⚠️ Lỗi lưới an toàn /nhac: {e}")

    # Dấu vết để kiểm tra cron thật sự đang gọi (mỗi lần gọi cập nhật giờ)
    try:
        update_system({"nhac_lan_cuoi": datetime.now(VN_TZ).isoformat()})
    except Exception:
        pass
    return jsonify({"ok": True})


@app.route("/auth-google")
def auth_google():
    """Sếp mở link này 1 LẦN để cấp quyền Google Calendar cho bot."""
    if not gcal.da_cau_hinh():
        return "❌ Thiếu GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET / GOOGLE_REDIRECT_URI", 500
    return redirect(gcal.auth_url(request.args.get("user")))


@app.route("/oauth2callback")
def oauth2callback():
    """Google gọi ngược về đây kèm code -> đổi lấy refresh_token, lưu MongoDB."""
    err = request.args.get("error")
    if err:
        return f"❌ Google từ chối: {err}", 400
    code = request.args.get("code")
    user = request.args.get("state") or gcal.GOOGLE_CAL_USER
    if not code:
        return "❌ Thiếu code từ Google", 400
    try:
        gcal.doi_code_lay_token(code, user)
        return "✅ Đã kết nối Google Calendar thành công! Sếp đóng tab này được rồi ạ."
    except Exception as e:
        return f"❌ Lỗi kết nối Google: {e}", 500


@app.route("/trang-thai-google")
def trang_thai_google():
    """Kiểm tra nhanh bot đã kết nối Google chưa."""
    return jsonify({
        "da_cau_hinh": gcal.da_cau_hinh(),
        "da_ket_noi": gcal.da_ket_noi(),
        "user": gcal.GOOGLE_CAL_USER,
        "calendar_id": gcal.CALENDAR_ID,
    })


@app.route("/", methods=["GET"])
def home():
    return "Trợ lý AI đang chạy"


# ==================== LUỒNG NỀN: tự quét deadline định kỳ ====================
def vong_lap_nhac():
    """Chạy nền trong service: cứ CRON_INTERVAL_MIN phút quét deadline 1 lần."""
    # Đợi 30 giây cho app khởi động ổn định
    time.sleep(30)
    while True:
        try:
            print(f"🕐 Quét định kỳ lúc {datetime.now(VN_TZ)}")
            nhac_deadline()
            kiem_tra_bao_cao_hang_ngay()
        except Exception as e:
            print(f"⚠️ Lỗi luồng nền: {e}")
        time.sleep(CRON_INTERVAL_MIN * 60)  # nghỉ tới lượt sau


# ==================== LƯỚI AN TOÀN: tự quét tin mới trong Mongo ====================
# Không phụ thuộc endpoint /xu-ly nữa: tầng nhận tin chỉ cần ghi tin vào Mongo,
# app này tự quét các tin processed != true và xử lý ngay (gần realtime).
POLL_TIN_GIAY = int(os.environ.get("POLL_TIN_GIAY", 30))  # chu kỳ quét dự phòng (giây)

def quet_tin_chua_xu_ly():
    """Xử lý mọi tin chưa xử lý theo thứ tự cũ -> mới (xu_ly_tin có khóa chống trùng)."""
    for tin in messages_col.find({"processed": {"$ne": True}}).sort("created_at", 1):
        try:
            xu_ly_tin(tin)
        except Exception as e:
            print(f"⚠️ Lỗi xử lý tin {tin.get('message_id')}: {e}")

def vong_lap_changestream():
    """TỨC THÌ: lắng nghe insert mới trên collection messages -> xử lý ngay lập tức.
    Không phụ thuộc webhook, không chờ poll. Tự nối lại nếu stream đứt."""
    time.sleep(3)  # đợi app ổn định
    while True:
        try:
            with messages_col.watch([{"$match": {"operationType": "insert"}}]) as stream:
                print("⚡ Đang lắng nghe tin mới TỨC THÌ (MongoDB change stream)")
                for change in stream:
                    doc = change.get("fullDocument")
                    if not doc:
                        continue
                    try:
                        xu_ly_tin(doc)
                    except Exception as e:
                        print(f"⚠️ Lỗi xử lý tin (change stream) {doc.get('message_id')}: {e}")
        except Exception as e:
            print(f"⚠️ Change stream đứt, thử nối lại sau 10s: {e}")
            time.sleep(10)

def vong_lap_quet_tin():
    """LƯỚI DỰ PHÒNG: cứ POLL_TIN_GIAY giây quét lại, phòng khi change stream bỏ sót
    (vd app từng offline lúc có tin tới). xu_ly_tin có khóa nên không xử lý trùng."""
    time.sleep(5)  # đợi app ổn định
    while True:
        try:
            quet_tin_chua_xu_ly()
        except Exception as e:
            print(f"⚠️ Lỗi vòng quét tin: {e}")
        time.sleep(POLL_TIN_GIAY)


# Khởi động luồng nền ngay khi app load (chạy cả với gunicorn)
_thread_started = False
def _start_background():
    global _thread_started
    if not _thread_started:
        _thread_started = True

        # --- Dọn BACKLOG: đánh dấu các tin cũ đã tồn tại là processed để KHÔNG gửi loạt
        #     phản hồi cũ (kể cả cho người ngoài) khi lưới an toàn bật lần đầu ---
        try:
            cutoff = datetime.now(timezone.utc)
            res = messages_col.update_many(
                {"processed": {"$ne": True}, "created_at": {"$lt": cutoff}},
                {"$set": {"processed": True, "bo_qua_backlog": True}},
            )
            print(f"🧹 Bỏ qua {res.modified_count} tin cũ (backlog) - chỉ xử lý tin mới từ giờ")
        except Exception as e:
            print(f"⚠️ Lỗi dọn backlog: {e}")

        threading.Thread(target=vong_lap_nhac, daemon=True).start()
        print("✅ Đã khởi động luồng nhắc deadline nền")
        threading.Thread(target=vong_lap_changestream, daemon=True).start()
        print("✅ Đã khởi động luồng lắng nghe tin TỨC THÌ (change stream)")
        threading.Thread(target=vong_lap_quet_tin, daemon=True).start()
        print(f"✅ Đã khởi động lưới dự phòng quét tin (mỗi {POLL_TIN_GIAY}s)")

# Chỉ chạy luồng nền khi host LUÔN BẬT (VM/Oracle/máy nhà/Koyeb-giữ-thức).
# Trên Vercel serverless: đặt CHAY_NEN=0 -> KHÔNG chạy nền (tránh khởi động lại
# change stream + dọn backlog mỗi lần cold start). Webhook sẽ xử lý đồng bộ.
if os.environ.get("CHAY_NEN", "1") == "1":
    _start_background()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
