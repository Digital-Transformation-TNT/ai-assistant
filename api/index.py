"""
Điểm vào cho Vercel (serverless). Vercel Python runtime tự dùng biến `app` (WSGI).
Thêm thư mục gốc vào path để import được app.py / assistant.py / google_calendar.py.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app  # noqa: E402,F401  (Vercel dùng biến `app` này)
