"""
============================================================
 TRỢ LÝ AI — BẢN GỘP 1 SERVICE
============================================================
Gộp 3 con vào chung 1 process:
  - Nhận tin Lark   (cũ: lark-message-bot-main)  -> POST /lark-webhook
  - Nhận tin Zalo   (cũ: zalo-bot-main)          -> POST /webhook
  - Trợ lý xử lý    (assistant.py, dùng lại nguyên) -> tạo/sửa/xóa lịch, nhắc, Google Calendar

Khác bản cũ: webhook nhận tin -> LƯU Mongo + gọi THẲNG xu_ly_tin() cùng process,
không gọi chéo qua mạng nữa (bỏ mắt xích từng gây lỗi).

Chạy:  gunicorn app:app --bind 0.0.0.0:$PORT
       (hoặc: python app.py)
============================================================
"""
import os
import json
import time
import threading
from datetime import datetime, timezone

import requests
from flask import request, jsonify

# Import nguyên "não" trợ lý: Flask app, Mongo, xử lý tin, Google Calendar,
# luồng nhắc + change stream + poll dự phòng (đã tự khởi động trong assistant.py).
import assistant
from assistant import (
    app, messages_col, xu_ly_tin,
    LARK_APP_ID, LARK_APP_SECRET, LARK_BASE_URL, BOSS_OPEN_ID,
)


# ============================================================
#  DÙNG CHUNG: lưu tin vào Mongo rồi xử lý ngay (thread)
# ============================================================
# CHAY_NEN=1 (mặc định): host luôn-bật -> xử lý ở thread nền, trả webhook nhanh.
# CHAY_NEN=0 (Vercel serverless): PHẢI xử lý ĐỒNG BỘ trong request, vì function bị
# đóng băng sau khi trả response -> thread nền sẽ không chạy xong.
CHAY_NEN = os.environ.get("CHAY_NEN", "1") == "1"

def _luu_va_xu_ly(doc):
    """Lưu tin (chống trùng theo message_id+platform) rồi xử lý.
    Khóa 'processed' trong xu_ly_tin đảm bảo không xử lý 2 lần (kể cả khi Lark/Zalo
    gửi lại do timeout, hay change stream cũng bắt được tin)."""
    messages_col.update_one(
        {"message_id": doc["message_id"], "platform": doc["platform"]},
        {"$setOnInsert": doc}, upsert=True,
    )
    tin = messages_col.find_one({"message_id": doc["message_id"], "platform": doc["platform"]})
    if not tin:
        return
    if CHAY_NEN:
        threading.Thread(target=xu_ly_tin, args=(tin,), daemon=True).start()
    else:
        # Serverless: xử lý xong mới trả response (chấp nhận webhook chờ vài giây)
        try:
            xu_ly_tin(tin)
        except Exception as e:
            print(f"⚠️ Lỗi xử lý đồng bộ: {e}")


# ============================================================
#  LARK — nhận webhook, lấy tên, lưu Mongo  (gộp từ lark_bot.py)
# ============================================================
_lark_token = {"token": None, "exp": 0}

def _lark_get_token():
    if _lark_token["token"] and time.time() < _lark_token["exp"]:
        return _lark_token["token"]
    try:
        r = requests.post(
            f"{LARK_BASE_URL}/open-apis/auth/v3/tenant_access_token/internal",
            json={"app_id": LARK_APP_ID, "app_secret": LARK_APP_SECRET}, timeout=10,
        ).json()
        if r.get("code") == 0:
            _lark_token["token"] = r["tenant_access_token"]
            _lark_token["exp"] = time.time() + r.get("expire", 3600) - 60
            return _lark_token["token"]
        print("[LỖI] Không lấy được token Lark:", r)
    except Exception as e:
        print("⚠️ Lỗi lấy token Lark:", e)
    return None

def _lark_user_info(open_id):
    tok = _lark_get_token()
    if not tok:
        return None
    try:
        r = requests.get(
            f"{LARK_BASE_URL}/open-apis/contact/v3/users/{open_id}?user_id_type=open_id",
            headers={"Authorization": f"Bearer {tok}"}, timeout=10,
        ).json()
        if r.get("code") == 0:
            u = r["data"]["user"]
            return {"name": u.get("name"), "email": u.get("email")}
    except Exception as e:
        print("⚠️ Lỗi lấy tên người Lark:", e)
    return None

def _lark_reply(open_id, text):
    tok = _lark_get_token()
    if not tok:
        return
    try:
        requests.post(
            f"{LARK_BASE_URL}/open-apis/im/v1/messages?receive_id_type=open_id",
            headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
            json={"receive_id": open_id, "msg_type": "text",
                  "content": json.dumps({"text": text})}, timeout=10,
        )
    except Exception as e:
        print("⚠️ Lỗi trả lời Lark:", e)

def _lark_parse(msg_type, raw):
    try:
        c = json.loads(raw)
    except Exception:
        return f"(không đọc được: {raw})"
    if msg_type == "text":
        return c.get("text", "")
    if msg_type == "image":
        return f"[Hình ảnh] image_key = {c.get('image_key')}"
    if msg_type == "file":
        return f"[File] file_key = {c.get('file_key')}"
    if msg_type == "audio":
        return f"[Ghi âm] file_key = {c.get('file_key')}"
    return f"[{msg_type}] {c}"


@app.route("/lark-webhook", methods=["POST"])
def lark_webhook():
    data = request.get_json(force=True, silent=True) or {}

    # (A) Xác thực URL lúc cấu hình (Lark gửi challenge)
    if "challenge" in data:
        return jsonify({"challenge": data["challenge"]})

    # (B) Sự kiện có người nhắn tin
    if data.get("header", {}).get("event_type") == "im.message.receive_v1":
        ev = data["event"]
        m = ev["message"]
        open_id = ev["sender"]["sender_id"]["open_id"]
        text = _lark_parse(m["message_type"], m["content"])
        u = _lark_user_info(open_id)
        name = u["name"] if u else open_id
        doc = {
            "platform": "lark",
            "message_id": str(m["message_id"]),
            "sender_id": str(open_id),
            "sender_name": name,
            "sender_email": u["email"] if u else None,
            "text": text,
            "msg_type": m["message_type"],
            "chat_id": str(m["chat_id"]),
            "chat_type": m.get("chat_type"),
            "created_at": datetime.now(timezone.utc),
        }
        print(f"👤 [Lark] {name}: {str(text)[:50]}")
        _luu_va_xu_ly(doc)

        # Tự trả lời NGƯỜI NGOÀI (không phải sếp) — sếp đã có trợ lý trả lời riêng
        if (m.get("chat_type") == "p2p" and m["message_type"] == "text"
                and open_id != BOSS_OPEN_ID):
            _lark_reply(open_id,
                f"Xin chào {name}, em đã nhận được tin nhắn của anh/chị, sẽ phản hồi sớm nhất ạ!")

    return jsonify({"code": 0}), 200


# ============================================================
#  ZALO — nhận webhook, lưu Mongo  (gộp từ main.py)
# ============================================================
ZALO_BOT_TOKEN = os.environ.get("ZALO_BOT_TOKEN")
ZALO_WEBHOOK_URL = os.environ.get("ZALO_WEBHOOK_URL")
ZALO_WEBHOOK_SECRET = os.environ.get("ZALO_WEBHOOK_SECRET", "mot_chuoi_bi_mat_bat_ky")

try:
    from zalo_bot import Bot
    _zalo = Bot(token=ZALO_BOT_TOKEN) if ZALO_BOT_TOKEN else None
except Exception as e:
    _zalo = None
    print("⚠️ Chưa dùng được zalo_bot (thiếu thư viện/token):", e)

def _zalo_send(chat_id, text):
    if not ZALO_BOT_TOKEN:
        return
    try:
        requests.post(f"https://bot-api.zapps.me/bot{ZALO_BOT_TOKEN}/sendMessage",
                      json={"chat_id": chat_id, "text": text}, timeout=10)
    except Exception as e:
        print("⚠️ Lỗi gửi Zalo:", e)


@app.route("/webhook", methods=["POST"])
def zalo_webhook():
    data = request.get_json(force=True, silent=True) or {}
    m = data.get("message")
    if m and "text" in m:
        s = m.get("from", {})
        chat = m.get("chat", {})
        raw_type = chat.get("chat_type", "PRIVATE")
        doc = {
            "platform": "zalo",
            "message_id": str(m.get("message_id", "")),
            "sender_id": str(s.get("id", "unknown")),
            "sender_name": s.get("display_name", "Ẩn danh"),
            "sender_email": None,
            "text": m.get("text", ""),
            "msg_type": "text",
            "chat_id": str(chat.get("id", "")),
            "chat_type": "group" if raw_type == "GROUP" else "p2p",
            "created_at": datetime.now(timezone.utc),
        }
        print(f"👤 [Zalo] {doc['sender_name']}: {str(doc['text'])[:50]}")
        _luu_va_xu_ly(doc)
        _zalo_send(doc["chat_id"],
                   f"Xin chào {doc['sender_name']}, em đã nhận được tin, sẽ phản hồi sớm nhất ạ!")
    return "ok"


@app.route("/setup-zalo", methods=["GET"])
def setup_zalo():
    """Mở link này 1 lần sau khi deploy để đăng ký webhook Zalo."""
    if not _zalo or not ZALO_WEBHOOK_URL:
        return "❌ Chưa cấu hình ZALO_BOT_TOKEN / ZALO_WEBHOOK_URL"
    try:
        _zalo.set_webhook(url=ZALO_WEBHOOK_URL, secret_token=ZALO_WEBHOOK_SECRET)
        return f"✅ Đã đăng ký webhook Zalo: {ZALO_WEBHOOK_URL}"
    except Exception as e:
        return f"❌ Lỗi đăng ký webhook Zalo: {e}"


print("✅ Bản GỘP sẵn sàng: /lark-webhook + /webhook (zalo) + trợ lý (assistant)")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
