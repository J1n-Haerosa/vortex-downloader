import os
import json
import time
import uuid
import re
import threading
import queue
import hashlib
import random
import sys
import multiprocessing
from datetime import datetime

# ==========================================
# VALIDASI INSTALASI DEPENDENSI PRODUCTION
# ==========================================
try:
    import yt_dlp
    from flask import Flask, request, jsonify, send_file, Response
    from waitress import serve
    from werkzeug.utils import secure_filename
    from werkzeug.middleware.proxy_fix import ProxyFix
    from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadSignature
    from dotenv import load_dotenv
except ImportError as e:
    print(f"\n[!!!] FATAL ERROR: Library hilang -> {e}")
    print("Install: pip install flask yt-dlp waitress python-dotenv itsdangerous")
    sys.exit(1)

# ==========================================
# KONFIGURASI KEAMANAN & ENVIRONMENT
# ==========================================
load_dotenv()

# [MENGATASI 9] STRICT SECRET KEY. Gagal booting jika tidak di-set.
SECRET_KEY = os.getenv('SECRET_KEY')
if not SECRET_KEY:
    raise RuntimeError("CRITICAL ERROR: SECRET_KEY wajib di-set di file .env untuk lingkungan Production!")

ADMIN_USER = os.getenv('ADMIN_USER', 'admin')
ADMIN_PASS = os.getenv('ADMIN_PASS', 'vortex123')

app = Flask(__name__)
app.config['SECRET_KEY'] = SECRET_KEY
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
url_signer = URLSafeTimedSerializer(app.config['SECRET_KEY'])

# ==========================================
# KONFIGURASI SISTEM & LIMIT PRODUCTION
# ==========================================
DOWNLOAD_DIR = "downloads"
FILE_LIMIT = "data_web_limits.json"

LIMIT_YOUTUBE = 5
LIMIT_LAINNYA = 20
MAX_FILESIZE_BYTES = 150 * 1024 * 1024  
MAX_BANDWIDTH_PER_DAY = 1.5 * 1024 * 1024 * 1024  

# Reservasi kuota di awal (Booking)
DEFAULT_RESERVE_BYTES = 50 * 1024 * 1024 

ALLOWED_EXTENSIONS = {'.mp4', '.mp3', '.m4a', '.webm', '.mkv'}
BLACKLIST_DOMAINS = ['pornhub.com', 'xvideos.com', 'onlyfans.com', 'xnxx.com', 'twitch.tv']

if not os.path.exists(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)

lock_db = threading.RLock()
lock_rl = threading.Lock()
lock_cache = threading.Lock()
lock_tasks = threading.Lock()
lock_cancel = threading.Lock() # [BARU] Lock khusus untuk sistem pembatalan

rate_limit_db = {}
RATE_LIMIT_WINDOW = 60 
MAX_REQUESTS_PER_WINDOW = 15 

MAKSIMAL_WORKER = 3 
# [MENGATASI 3] Bounded Queue untuk mencegah OOM saat Spam
task_queue = queue.Queue(maxsize=100) 
task_status_db = {} 
tugas_dibatalkan = set() 

url_cache_db = {}
CACHE_LIFETIME = 1800 
FILE_LIFETIME = 7200  

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
]

# ==========================================
# FUNGSI HELPER
# ==========================================
def hash_ip(ip_address):
    if not ip_address: return "unknown"
    return hashlib.sha256(ip_address.encode('utf-8')).hexdigest()

def clean_ansi(text):
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', str(text)).strip()

def format_size(bytes_size):
    if bytes_size >= 1024**3: return f"{bytes_size / (1024**3):.2f} GB"
    return f"{bytes_size / (1024**2):.2f} MB"

def is_blacklisted(url):
    url_lower = url.lower()
    return any(domain in url_lower for domain in BLACKLIST_DOMAINS)

# ==========================================
# SISTEM RESERVASI KUOTA (ANTI RACE CONDITION)
# ==========================================
def is_rate_limited(hashed_ip):
    waktu_sekarang = time.time()
    with lock_rl:
        if hashed_ip not in rate_limit_db: rate_limit_db[hashed_ip] = []
        rate_limit_db[hashed_ip] = [t for t in rate_limit_db[hashed_ip] if waktu_sekarang - t < RATE_LIMIT_WINDOW]
        if len(rate_limit_db[hashed_ip]) >= MAX_REQUESTS_PER_WINDOW: return True
        rate_limit_db[hashed_ip].append(waktu_sekarang)
        return False

def muat_data_limit():
    if os.path.exists(FILE_LIMIT):
        try:
            with open(FILE_LIMIT, 'r') as f: return json.load(f)
        except: return {}
    return {}

def simpan_data_limit(data):
    with open(FILE_LIMIT, 'w') as f: json.dump(data, f, indent=4)

def _init_user_limit(data, hashed_ip, tanggal):
    if hashed_ip not in data or data[hashed_ip].get("tanggal") != tanggal:
        data[hashed_ip] = {"tanggal": tanggal, "youtube": 0, "lainnya": 0, "bandwidth": 0}
    return data[hashed_ip]

def reservasi_kuota(hashed_ip, url):
    tanggal_hari_ini = datetime.now().strftime('%Y-%m-%d')
    is_youtube = bool(re.search(r'(youtube\.com|youtu\.be)', url.lower()))
    
    with lock_db:
        data = muat_data_limit()
        user_data = _init_user_limit(data, hashed_ip, tanggal_hari_ini)
        
        current_bw = user_data.get("bandwidth", 0)
        if current_bw + DEFAULT_RESERVE_BYTES > MAX_BANDWIDTH_PER_DAY:
            return False, f"Limit Bandwidth Harian Habis ({format_size(MAX_BANDWIDTH_PER_DAY)})."

        if is_youtube:
            if user_data["youtube"] >= LIMIT_YOUTUBE: return False, f"Limit YouTube Habis ({LIMIT_YOUTUBE}/hari)."
            user_data["youtube"] += 1
        else:
            if user_data["lainnya"] >= LIMIT_LAINNYA: return False, f"Limit Media Lain Habis ({LIMIT_LAINNYA}/hari)."
            user_data["lainnya"] += 1
            
        user_data["bandwidth"] += DEFAULT_RESERVE_BYTES
        simpan_data_limit(data)
    return True, ""

def selesaikan_kuota(hashed_ip, url, actual_size_bytes, success):
    tanggal_hari_ini = datetime.now().strftime('%Y-%m-%d')
    is_youtube = bool(re.search(r'(youtube\.com|youtu\.be)', url.lower()))
    
    with lock_db:
        data = muat_data_limit()
        if hashed_ip not in data: return
        user_data = data[hashed_ip]
        if user_data.get("tanggal") != tanggal_hari_ini: return

        if success:
            user_data["bandwidth"] = max(0, user_data["bandwidth"] - DEFAULT_RESERVE_BYTES + actual_size_bytes)
        else:
            user_data["bandwidth"] = max(0, user_data["bandwidth"] - DEFAULT_RESERVE_BYTES)
            if is_youtube: user_data["youtube"] = max(0, user_data["youtube"] - 1)
            else: user_data["lainnya"] = max(0, user_data["lainnya"] - 1)
            
        simpan_data_limit(data)

# ==========================================
# [MENGATASI 6 & 4] HARD-KILL PROCESS & IPC QUEUE
# ==========================================
# Fungsi ini berjalan di Process terpisah (bukan Thread), sehingga bisa di-terminate paksa oleh OS.
def _ytdlp_isolated_process(ydl_opts, url, q_result, q_progress):
    def hook(d):
        if d['status'] == 'downloading':
            # Melempar data progres ke queue lintas-proses
            q_progress.put({
                'progress': clean_ansi(d.get('_percent_str', '0%')),
                'eta': clean_ansi(d.get('_eta_str', '...')),
                'speed': clean_ansi(d.get('_speed_str', '...'))
            })
    ydl_opts['progress_hooks'] = [hook]

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            filename = ydl.prepare_filename(info)
            q_result.put({'success': True, 'info': info, 'filename': filename})
    except Exception as e:
        q_result.put({'success': False, 'error': str(e)})

def worker_download():
    while True:
        try: task = task_queue.get(timeout=5)
        except queue.Empty: continue
            
        task_id = task['task_id']
        url = task['url']
        fmt_pilihan = task['format']
        uid_acak = task['uid']
        hashed_ip = task['hashed_ip']
        
        format_ydl = 'best'
        postprocessors = []
        
        if 'mp3' in fmt_pilihan:
            format_ydl = 'bestaudio/best'
            bitrate = '192'
            if fmt_pilihan == 'mp3-320': bitrate = '320'
            elif fmt_pilihan == 'mp3-128': bitrate = '128'
            postprocessors = [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': bitrate}]
        elif fmt_pilihan == 'mp4-1080p': format_ydl = 'bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best'
        elif fmt_pilihan == 'mp4-720p': format_ydl = 'bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/bestvideo[height<=480][ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best'
        elif fmt_pilihan == 'mp4-360p': format_ydl = 'bestvideo[height<=360][ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best'

        nama_target_base = f"{DOWNLOAD_DIR}/%(title)s_{uid_acak}.%(ext)s"
        
        ydl_opts = {
            'format': format_ydl,
            'outtmpl': nama_target_base,
            'max_filesize': MAX_FILESIZE_BYTES,
            'noplaylist': True,
            'quiet': True,
            'no_warnings': True,
            'geo_bypass': True,
            'socket_timeout': 15, 
            'match_filter': yt_dlp.utils.match_filter_func("!is_live"),
            'retries': 3,
            'http_headers': {'User-Agent': random.choice(USER_AGENTS)}
        }
        if postprocessors: ydl_opts['postprocessors'] = postprocessors

        sukses = False
        pesan_error_final = ""
        filename_asli = None
        file_size_final = 0

        for percobaan in range(3):
            with lock_cancel:
                if task_id in tugas_dibatalkan: 
                    pesan_error_final = "Proses dihentikan oleh pengguna."
                    break

            # IPC Queues
            q_result = multiprocessing.Queue()
            q_progress = multiprocessing.Queue()
            
            p = multiprocessing.Process(target=_ytdlp_isolated_process, args=(ydl_opts, url, q_result, q_progress))
            p.start()

            start_time = time.time()
            hard_timeout = 600 # 10 Menit batas nyawa maksimal
            process_crashed = False
            error_msg = ""

            # Loop Monitoring (Watchdog Asli)
            while p.is_alive():
                # 1. Hard Kill jika melewati batas waktu (ZOMBIE THREAD DESTROYER)
                if time.time() - start_time > hard_timeout:
                    p.terminate()
                    p.join()
                    process_crashed = True
                    error_msg = "TIMEOUT_KERAS"
                    break

                # 2. Hard Kill jika user pencet tombol batal di UI
                with lock_cancel:
                    if task_id in tugas_dibatalkan:
                        p.terminate()
                        p.join()
                        process_crashed = True
                        error_msg = "DIBATALKAN_USER"
                        break

                # 3. Update progres ke memori utama agar dibaca SSE
                while not q_progress.empty():
                    prog = q_progress.get()
                    with lock_tasks:
                        if task_id in task_status_db:
                            task_status_db[task_id]['progress'] = prog['progress']
                            task_status_db[task_id]['eta'] = prog['eta']
                            task_status_db[task_id]['speed'] = prog['speed']
                            task_status_db[task_id]['timestamp'] = time.time() # [MENGATASI 2] Refresh GC

                time.sleep(0.5)

            # Eksekusi setelah proses selesai atau dibunuh
            if process_crashed:
                if error_msg == "TIMEOUT_KERAS":
                    pesan_error_final = "Timeout: Proses OS macet dan dihentikan paksa."
                    break # Jangan retry kalau nge-hang
                elif error_msg == "DIBATALKAN_USER":
                    pesan_error_final = "Proses dihentikan oleh pengguna."
                    break

            if not q_result.empty():
                res = q_result.get()
                if res['success']:
                    info = res['info']
                    filename_asli = res['filename']
                    
                    if 'mp3' in fmt_pilihan:
                        filename_asli = os.path.splitext(filename_asli)[0] + '.mp3'
                    
                    nama_file_murni = secure_filename(os.path.basename(filename_asli))
                    if os.path.exists(filename_asli): file_size_final = os.path.getsize(filename_asli)
                    
                    cache_key = f"{url}_{fmt_pilihan}"
                    with lock_cache:
                        url_cache_db[cache_key] = {
                            "filename": nama_file_murni,
                            "title": info.get('title', 'Video'),
                            "thumbnail": info.get('thumbnail', ''),
                            "timestamp": time.time()
                        }
                    
                    token_url = url_signer.dumps(nama_file_murni)
                    
                    with lock_tasks:
                        task_status_db[task_id] = {
                            "status": "done",
                            "data": {
                                "title": info.get('title', 'Video'),
                                "thumbnail": info.get('thumbnail', ''),
                                "download_url": f"/download/{token_url}"
                            }
                        }
                    sukses = True
                    break 
                else:
                    error_msg = res['error'].lower()
                    if "private" in error_msg: pesan_error_final = "Video bersifat Private."; break
                    if "live" in error_msg: pesan_error_final = "Tidak bisa mendownload Live Stream."; break
                    if "max_filesize" in error_msg: pesan_error_final = "File melampaui 150MB."; break
                    time.sleep(3) # Retry
            else:
                # Proses mati mendadak tanpa log
                time.sleep(3)

        # Akhir Siklus Pekerja
        if not sukses:
            with lock_tasks:
                if task_id in task_status_db: task_status_db[task_id] = {"status": "error", "message": pesan_error_final or "Gagal memproses video."}
            
            if filename_asli:
                try:
                    if os.path.exists(filename_asli + '.part'): os.remove(filename_asli + '.part')
                    if os.path.exists(filename_asli + '.ytdl'): os.remove(filename_asli + '.ytdl')
                    if os.path.exists(filename_asli): os.remove(filename_asli)
                except: pass
                
        with lock_cancel:
            tugas_dibatalkan.discard(task_id)
        
        selesaikan_kuota(hashed_ip, url, file_size_final, sukses)
        task_queue.task_done()

# ==========================================
# GARBAGE COLLECTOR
# ==========================================
def bersihkan_sampah_sistem():
    while True:
        try:
            time.sleep(300) 
            now = time.time()
            tanggal_hari_ini = datetime.now().strftime('%Y-%m-%d')
            
            for filename in os.listdir(DOWNLOAD_DIR):
                filepath = os.path.join(DOWNLOAD_DIR, filename)
                if os.path.isfile(filepath) and os.stat(filepath).st_mtime < now - FILE_LIFETIME:
                    os.remove(filepath)
                        
            with lock_cache:
                for k in list(url_cache_db.keys()):
                    if now - url_cache_db[k]['timestamp'] > CACHE_LIFETIME: del url_cache_db[k]
                        
            with lock_tasks:
                for t in list(task_status_db.keys()):
                    st = task_status_db[t]
                    # [MENGATASI 2] JANGAN BUNUH TASK YANG MASIH JALAN
                    if st.get("status") == "processing": continue
                    if now - st.get("timestamp", now) > 1800: del task_status_db[t]
            
            with lock_db:
                data = muat_data_limit()
                keys_to_delete = [ip for ip, d in data.items() if d.get('tanggal') != tanggal_hari_ini]
                for k in keys_to_delete: del data[k]
                if keys_to_delete: simpan_data_limit(data)
        except: pass

# ==========================================
# FRONTEND HTML
# ==========================================
HTML_CONTENT = """
<!DOCTYPE html>
<html lang="id">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>VORTEX V9 — Fortress Edition</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;500;600;700;800&family=DM+Mono:ital,wght@0,300;0,400;0,500;1,400&display=swap" rel="stylesheet" />
  <style>
    :root { --bg-void: #060608; --bg-panel: #0d0d12; --bg-card: #111118; --bg-card-hover: #16161f; --border-dim: rgba(255,255,255,0.06); --border-glow: rgba(0,240,180,0.3); --accent: #00f0b4; --accent-dim: rgba(0,240,180,0.12); --accent-glow: rgba(0,240,180,0.5); --red: #ff4060; --yellow: #ffd060; --text-primary: #eeeef5; --text-secondary:#7878a0; --text-muted: #3e3e58; --font-display: 'Syne', sans-serif; --font-mono: 'DM Mono', monospace; --radius-sm: 6px; --radius-md: 12px; --radius-lg: 18px; --transition: all 0.2s cubic-bezier(0.4,0,0.2,1); } :root.light-mode { --bg-void: #f4f4f8; --bg-panel: #ffffff; --bg-card: #f9fafc; --bg-card-hover: #f1f3f7; --border-dim: rgba(0,0,0,0.08); --border-glow: rgba(0,180,140,0.3); --accent: #00c090; --accent-dim: rgba(0,180,140,0.1); --accent-glow: rgba(0,180,140,0.3); --red: #e03050; --yellow: #e0a020; --text-primary: #1a1a24; --text-secondary:#5a5a78; --text-muted: #8a8aab; } *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; } html { scroll-behavior: smooth; } body { background: var(--bg-void); color: var(--text-primary); font-family: var(--font-display); min-height: 100vh; overflow-x: hidden; -webkit-font-smoothing: antialiased; transition: background 0.3s, color 0.3s; } .bg-grid { position: fixed; inset: 0; z-index: 0; pointer-events: none; background-image: linear-gradient(rgba(128,128,128,0.04) 1px, transparent 1px), linear-gradient(90deg, rgba(128,128,128,0.04) 1px, transparent 1px); background-size: 48px 48px; } .bg-orb { position: fixed; border-radius: 50%; filter: blur(100px); pointer-events: none; z-index: 0; opacity: 0.6; } .bg-orb-1 { width: 600px; height: 600px; background: radial-gradient(circle, var(--accent-dim) 0%, transparent 70%); top: -200px; left: -200px; animation: orb-drift 18s ease-in-out infinite alternate; } .bg-orb-2 { width: 500px; height: 500px; background: radial-gradient(circle, rgba(100,80,255,0.06) 0%, transparent 70%); bottom: -200px; right: -150px; animation: orb-drift 22s ease-in-out infinite alternate-reverse; } @keyframes orb-drift { from { transform: translate(0, 0) scale(1); } to { transform: translate(40px, 30px) scale(1.1); } } .wrapper { position: relative; z-index: 1; min-height: 100vh; display: flex; flex-direction: column; align-items: center; padding: 32px 16px 80px; } header { width: 100%; max-width: 680px; display: flex; align-items: center; justify-content: space-between; margin-bottom: 56px; padding: 0 4px; } .logo { display: flex; align-items: center; gap: 10px; } .logo-icon { width: 34px; height: 34px; background: var(--accent); border-radius: 8px; display: flex; align-items: center; justify-content: center; box-shadow: 0 0 20px var(--accent-glow); flex-shrink: 0; color: #fff;} .logo-text { font-size: 18px; font-weight: 800; letter-spacing: 0.1em; color: var(--text-primary); } .top-controls { display: flex; gap: 12px; align-items: center; } .theme-toggle { background: var(--bg-card); border: 1px solid var(--border-dim); border-radius: 50%; width: 32px; height: 32px; display: flex; align-items: center; justify-content: center; color: var(--text-muted); cursor: pointer; transition: var(--transition); } .theme-toggle:hover { color: var(--text-primary); background: var(--bg-card-hover); } .status-badge { display: flex; align-items: center; gap: 6px; background: var(--bg-card); border: 1px solid var(--border-dim); border-radius: 100px; padding: 6px 12px; font-family: var(--font-mono); font-size: 11px; color: var(--text-muted); } .status-dot { width: 6px; height: 6px; border-radius: 50%; background: var(--accent); box-shadow: 0 0 8px var(--accent); } .hero { width: 100%; max-width: 680px; text-align: center; margin-bottom: 40px; } .hero-tag { display: inline-flex; align-items: center; gap: 6px; background: var(--accent-dim); border: 1px solid rgba(0,240,180,0.2); border-radius: 100px; padding: 4px 14px 4px 10px; font-family: var(--font-mono); font-size: 11px; color: var(--accent); letter-spacing: 0.08em; text-transform: uppercase; margin-bottom: 20px; } h1 { font-size: clamp(32px, 6vw, 52px); font-weight: 800; line-height: 1.08; letter-spacing: -0.02em; margin-bottom: 14px; } h1 .accent { color: var(--accent); } .hero-desc { font-family: var(--font-mono); font-size: 13px; color: var(--text-secondary); line-height: 1.7; max-width: 480px; margin: 0 auto; } .main-card { width: 100%; max-width: 680px; background: var(--bg-panel); border: 1px solid var(--border-dim); border-radius: var(--radius-lg); padding: 28px; box-shadow: 0 0 0 1px rgba(0,0,0,0.05), 0 40px 80px rgba(0,0,0,0.2); } .input-group { display: flex; gap: 10px; align-items: stretch; margin-bottom: 24px; } .input-wrap { flex: 1; position: relative; } #url-input { width: 100%; background: var(--bg-card); border: 1px solid var(--border-dim); border-radius: var(--radius-md); padding: 14px 14px 14px 42px; color: var(--text-primary); font-family: var(--font-mono); font-size: 13px; outline: none; transition: var(--transition); height: 52px; } #url-input:focus { border-color: var(--border-glow); background: var(--bg-card-hover); } .url-icon { position: absolute; left: 14px; top: 50%; transform: translateY(-50%); color: var(--text-muted); pointer-events: none; } .paste-btn { height: 52px; padding: 0 16px; background: var(--bg-card); border: 1px solid var(--border-dim); border-radius: var(--radius-md); color: var(--text-secondary); font-family: var(--font-mono); font-size: 12px; cursor: pointer; transition: var(--transition); display: flex; align-items: center; gap: 6px; } .paste-btn:hover { background: var(--bg-card-hover); color: var(--text-primary); } #preview-card { background: var(--bg-card); border: 1px solid var(--border-dim); border-radius: var(--radius-md); padding: 14px; display: none; gap: 14px; align-items: center; margin-bottom: 24px; animation: slide-up 0.3s ease; } #preview-card.visible { display: flex; } .preview-thumb { width: 64px; height: 64px; border-radius: 8px; object-fit: cover; background: var(--bg-panel); display: flex; align-items: center; justify-content: center; color: var(--text-muted); overflow: hidden; } .preview-meta { flex: 1; min-width: 0; } .preview-title { font-size: 13px; font-weight: 600; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; margin-bottom: 4px; } .preview-duration { font-family: var(--font-mono); font-size: 11px; color: var(--text-muted); } .section-label { font-family: var(--font-mono); font-size: 10px; color: var(--text-muted); letter-spacing: 0.12em; text-transform: uppercase; margin-bottom: 12px; } .format-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; margin-bottom: 12px; } .format-btn { background: var(--bg-card); border: 1px solid var(--border-dim); border-radius: var(--radius-md); padding: 12px 8px; cursor: pointer; text-align: center; transition: var(--transition); } .format-btn.active { border-color: var(--border-glow); background: var(--accent-dim); box-shadow: 0 0 16px rgba(0,240,180,0.1); } .format-icon { font-size: 18px; display: block; margin-bottom: 6px; } .format-name { font-family: var(--font-mono); font-size: 12px; font-weight: 500; color: var(--text-primary); display: block; } .format-btn.active .format-name { color: var(--accent); } .format-quality { font-family: var(--font-mono); font-size: 10px; color: var(--text-muted); display: block; margin-top: 2px; } .bitrate-wrap { display: none; justify-content: center; gap: 8px; margin-bottom: 24px; animation: fade-in 0.2s ease; } .bitrate-wrap.visible { display: flex; } .bitrate-btn { background: var(--bg-card); border: 1px solid var(--border-dim); border-radius: 100px; padding: 6px 14px; font-family: var(--font-mono); font-size: 11px; color: var(--text-secondary); cursor: pointer; transition: var(--transition); } .bitrate-btn.active { background: var(--accent-dim); border-color: var(--border-glow); color: var(--accent); } .divider { height: 1px; background: linear-gradient(90deg, transparent, var(--border-dim), transparent); margin: 24px 0; } #download-btn { width: 100%; background: var(--accent); border: none; border-radius: var(--radius-md); padding: 16px 24px; color: #fff; font-family: var(--font-display); font-size: 15px; font-weight: 700; cursor: pointer; display: flex; align-items: center; justify-content: center; gap: 10px; transition: var(--transition); } #download-btn:disabled { opacity: 0.7; cursor: not-allowed; } .btn-spinner { width: 18px; height: 18px; border: 2px solid rgba(0,0,0,0.3); border-top-color: currentColor; border-radius: 50%; animation: spin 0.7s linear infinite; display: none; } @keyframes spin { to { transform: rotate(360deg); } } #download-btn.loading .btn-spinner { display: block; } #download-btn.loading .btn-icon { display: none; } .progress-wrap { margin-top: 16px; display: none; background: var(--bg-card); padding: 16px; border-radius: var(--radius-md); border: 1px solid var(--border-dim); } .progress-wrap.visible { display: block; } .progress-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; } .progress-label { font-family: var(--font-mono); font-size: 11px; color: var(--text-secondary); } .progress-pct { font-family: var(--font-mono); font-size: 14px; font-weight: bold; color: var(--accent); } .progress-track { height: 6px; background: var(--bg-panel); border-radius: 3px; overflow: hidden; margin-bottom: 10px; } .progress-fill { height: 100%; background: linear-gradient(90deg, var(--accent), #00c8ff); width: 0%; transition: width 0.3s; } .progress-stats { display: flex; justify-content: space-between; font-family: var(--font-mono); font-size: 10px; color: var(--text-muted); } .cancel-btn { background: none; border: none; color: var(--red); font-family: var(--font-mono); font-size: 11px; cursor: pointer; text-decoration: underline; margin-top: 10px; display: inline-block; } #result-card { margin-top: 20px; background: var(--bg-card); border: 1px solid var(--border-dim); border-radius: var(--radius-md); padding: 16px; display: none; gap: 14px; align-items: flex-start; } #result-card.visible { display: flex; } .result-thumb { width: 80px; height: 52px; border-radius: var(--radius-sm); object-fit: cover; } .result-meta { flex: 1; } .result-title { font-size: 13px; font-weight: 600; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; margin-bottom: 4px; } .result-info { font-family: var(--font-mono); font-size: 11px; color: var(--text-muted); display: flex; gap: 12px; } #toast-container { position: fixed; bottom: 24px; left: 50%; transform: translateX(-50%); z-index: 9999; display: flex; flex-direction: column-reverse; gap: 10px; pointer-events: none; width: 90%; max-width: 360px; } .toast { pointer-events: all; background: var(--bg-panel); border: 1px solid var(--border-dim); border-radius: var(--radius-md); padding: 14px 16px; display: flex; align-items: flex-start; gap: 12px; animation: toast-in 0.4s cubic-bezier(0.4,0,0.2,1); cursor: pointer; width: 100%; box-shadow: 0 10px 30px rgba(0,0,0,0.2); } .toast.removing { animation: toast-out 0.3s forwards; } .toast-title { font-size: 13px; font-weight: 600; margin-bottom: 4px; line-height: 1.2; } .toast-msg { font-family: var(--font-mono); font-size: 11px; color: var(--text-secondary); line-height: 1.5; word-wrap: break-word; } .toast.success { border-color: rgba(0,240,180,0.4); } .toast.error { border-color: rgba(255,64,96,0.4); } .toast.warning { border-color: rgba(255,208,96,0.4); } .history-section { width: 100%; max-width: 680px; margin-top: 24px; display: none; } .history-section.visible { display: block; animation: fade-in 0.3s ease; } .history-title { font-size: 12px; font-weight: 600; color: var(--text-secondary); text-transform: uppercase; letter-spacing: 0.1em; margin-bottom: 12px; display: flex; justify-content: space-between; } .clear-history { background: none; border: none; color: var(--text-muted); cursor: pointer; font-family: var(--font-mono); font-size: 10px; } .clear-history:hover { color: var(--red); } .history-list { display: flex; flex-direction: column; gap: 8px; } .history-item { background: var(--bg-panel); border: 1px solid var(--border-dim); border-radius: var(--radius-md); padding: 12px 16px; display: flex; justify-content: space-between; align-items: center; text-decoration: none; color: var(--text-primary); transition: var(--transition); } .history-item:hover { background: var(--bg-card-hover); border-color: rgba(255,255,255,0.1); } .history-info { overflow: hidden; } .history-name { font-size: 13px; font-weight: 500; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; margin-bottom: 4px; } .history-meta { font-family: var(--font-mono); font-size: 10px; color: var(--text-muted); } .history-dl { color: var(--accent); flex-shrink: 0; padding-left: 10px; } @keyframes toast-in { from { opacity: 0; transform: translateY(20px) scale(0.95); } to { opacity: 1; transform: translateY(0) scale(1); } } @keyframes toast-out { from { opacity: 1; transform: scale(1); } to { opacity: 0; transform: scale(0.9); } } @keyframes slide-up { from {opacity:0; transform:translateY(10px);} to {opacity:1; transform:translateY(0);} } @keyframes fade-in { from {opacity:0;} to {opacity:1;} } footer { margin-top: 40px; font-family: var(--font-mono); font-size: 10px; color: var(--text-muted); text-align: center; line-height: 1.8; max-width: 500px; } @media (max-width: 520px) { .main-card { padding: 20px 16px; } .format-grid { grid-template-columns: repeat(2, 1fr); } .input-group { flex-direction: column; } .paste-btn { height: 44px; justify-content: center; } h1 { font-size: 28px; } }
  </style>
</head>
<body>
  <div class="bg-grid"></div><div class="bg-orb bg-orb-1"></div><div class="bg-orb bg-orb-2"></div>
  <div id="toast-container"></div>
  <div class="wrapper">
    <header>
      <div class="logo">
        <div class="logo-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg></div>
        <div><div class="logo-text">VORTEX</div><div style="font-family: var(--font-mono); font-size: 10px; color: var(--text-muted);">SRE Edition V9</div></div>
      </div>
      <div class="top-controls">
        <button class="theme-toggle" id="theme-toggle" title="Toggle Light/Dark Mode">☀️</button>
        <div class="status-badge"><div class="status-dot online"></div><span>Fortress Core</span></div>
      </div>
    </header>
    
    <div class="hero">
      <div class="hero-tag"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="m4.93 4.93 14.14 14.14"/></svg> Production Grade Architecture</div>
      <h1>Download <span class="accent">Anything.</span><br>From Anywhere.</h1>
      <p class="hero-desc">Anti-OOM, Hard-Kill Timeout, Bounded Queues, dan Caching System.</p>
    </div>
    
    <div class="main-card">
      <div class="input-group">
        <div class="input-wrap">
          <svg class="url-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/></svg>
          <input type="url" id="url-input" placeholder="https://www.youtube.com/watch?v=..." autocomplete="off" spellcheck="false" />
        </div>
        <button class="paste-btn" id="paste-btn">
            <div id="paste-spinner" class="btn-spinner" style="border-top-color:currentColor; width:14px; height:14px; display:none;"></div>
            <span id="paste-text">Check</span>
        </button>
      </div>

      <div id="preview-card">
        <div id="preview-thumb" class="preview-thumb"></div>
        <div class="preview-meta">
          <div class="preview-title" id="preview-title">Judul Video</div>
          <div class="preview-duration" id="preview-duration">00:00</div>
        </div>
      </div>

      <p class="section-label">— pilih format & kualitas</p>
      <div class="format-grid" id="format-grid">
        <button class="format-btn" data-format="mp3"><span class="format-icon">🎵</span><span class="format-name">MP3</span><span class="format-quality">Audio</span></button>
        <button class="format-btn active" data-format="mp4-720p"><span class="format-icon">📹</span><span class="format-name">MP4</span><span class="format-quality">720p</span></button>
        <button class="format-btn" data-format="mp4-1080p"><span class="format-icon">🎬</span><span class="format-name">MP4</span><span class="format-quality">1080p</span></button>
        <button class="format-btn" data-format="mp4-360p"><span class="format-icon">📱</span><span class="format-name">MP4</span><span class="format-quality">360p</span></button>
      </div>

      <div class="bitrate-wrap" id="bitrate-wrap">
          <button class="bitrate-btn" data-bitrate="128">128 kbps</button>
          <button class="bitrate-btn active" data-bitrate="192">192 kbps</button>
          <button class="bitrate-btn" data-bitrate="320">320 kbps</button>
      </div>

      <div class="divider"></div>
      <button id="download-btn" disabled>
        <div class="btn-spinner"></div>
        <svg class="btn-icon" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
        Mulai Download
      </button>
      
      <div class="progress-wrap" id="progress-wrap">
        <div class="progress-header">
            <span class="progress-label" id="progress-label">Menghubungkan...</span>
            <span class="progress-pct" id="progress-pct">0%</span>
        </div>
        <div class="progress-track"><div class="progress-fill" id="progress-fill"></div></div>
        <div class="progress-stats">
            <span id="progress-speed">-- MB/s</span>
            <span id="progress-eta">ETA: --:--</span>
        </div>
        <button class="cancel-btn" id="cancel-btn">Batalkan Proses</button>
      </div>
      
      <div id="result-card">
        <div class="result-meta">
          <div class="result-title" id="result-title">—</div>
          <div class="result-info"><span id="result-format">Selesai diunduh. File aman dihapus dari server kami.</span></div>
        </div>
      </div>
    </div>
    
    <div class="history-section" id="history-section">
        <div class="history-title">Riwayat Unduhan Anda <button class="clear-history" id="clear-history">Hapus Riwayat</button></div>
        <div class="history-list" id="history-list"></div>
    </div>

    <footer>
        🔒 <b>Privasi Anda Aman:</b> IP Anda di-hash (anonim). Riwayat hanya disimpan di browser Anda (Local Storage).
    </footer>
  </div>

  <script>
    const API_BASE = ""; 
    let selectedBaseFormat = "mp4-720p";
    let selectedBitrate = "192";
    let isFetchingInfo = false;
    let isLoading = false;
    let eventSource = null;
    let currentValidUrl = "";
    let currentTaskId = "";

    const urlInput = document.getElementById("url-input");
    const pasteBtn = document.getElementById("paste-btn");
    const downloadBtn = document.getElementById("download-btn");
    const previewCard = document.getElementById("preview-card");
    const bitrateWrap = document.getElementById("bitrate-wrap");
    const cancelBtn = document.getElementById("cancel-btn");

    const themeToggle = document.getElementById("theme-toggle");
    if(localStorage.getItem('vortex_theme') === 'light') { document.documentElement.classList.add('light-mode'); themeToggle.textContent = '🌙'; }
    themeToggle.addEventListener('click', () => {
        document.documentElement.classList.toggle('light-mode');
        const isLight = document.documentElement.classList.contains('light-mode');
        themeToggle.textContent = isLight ? '🌙' : '☀️';
        localStorage.setItem('vortex_theme', isLight ? 'light' : 'dark');
    });

    function showToast(type, title, message) {
      const iconMap = {
        success: `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="var(--accent)" stroke-width="2.5"><circle cx="12" cy="12" r="10"/><path d="m9 12 2 2 4-4"/></svg>`,
        error:   `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="var(--red)" stroke-width="2.5"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>`,
        warning: `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="var(--yellow)" stroke-width="2.5"><path d="m10.29 3.86-8.16 14.14a1 1 0 0 0 .86 1.5h16.6a1 1 0 0 0 .86-1.5L12.71 3.86a1 1 0 0 0-1.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>`
      };
      const t = document.createElement("div"); t.className = `toast ${type}`;
      t.innerHTML = `<div class="toast-icon">${iconMap[type]}</div><div class="toast-body"><div class="toast-title">${title}</div><div class="toast-msg">${message}</div></div>`;
      document.getElementById("toast-container").appendChild(t);
      setTimeout(() => { t.classList.add("removing"); t.addEventListener("animationend", () => t.remove()); }, 4500);
    }

    function formatTime(seconds) {
      if(!seconds) return "Durasi tidak diketahui";
      const h = Math.floor(seconds / 3600);
      const m = Math.floor((seconds % 3600) / 60);
      const s = seconds % 60;
      return h > 0 ? `${h}:${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}` : `${m}:${s.toString().padStart(2, '0')}`;
    }

    function updateHistory(title, url) {
        let hist = JSON.parse(localStorage.getItem('vortex_history') || '[]');
        hist.unshift({title, url, date: new Date().toLocaleDateString()});
        if(hist.length > 5) hist.pop();
        localStorage.setItem('vortex_history', JSON.stringify(hist));
        renderHistory();
    }
    
    function renderHistory() {
        let hist = JSON.parse(localStorage.getItem('vortex_history') || '[]');
        const section = document.getElementById("history-section");
        const list = document.getElementById("history-list");
        if(hist.length === 0) { section.classList.remove('visible'); return; }
        
        section.classList.add('visible');
        list.innerHTML = hist.map(item => `
            <a href="${item.url}" target="_blank" class="history-item">
                <div class="history-info"><div class="history-name">${item.title}</div><div class="history-meta">${item.date}</div></div>
                <div class="history-dl"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg></div>
            </a>
        `).join('');
    }
    document.getElementById("clear-history").addEventListener("click", () => { localStorage.removeItem('vortex_history'); renderHistory(); });
    renderHistory(); 

    document.getElementById("format-grid").addEventListener("click", (e) => {
      const btn = e.target.closest(".format-btn");
      if (!btn) return;
      document.querySelectorAll(".format-btn").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      selectedBaseFormat = btn.dataset.format;
      if (selectedBaseFormat === "mp3") bitrateWrap.classList.add("visible");
      else bitrateWrap.classList.remove("visible");
    });

    bitrateWrap.addEventListener("click", (e) => {
        const btn = e.target.closest(".bitrate-btn");
        if (!btn) return;
        document.querySelectorAll(".bitrate-btn").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        selectedBitrate = btn.dataset.bitrate;
    });

    async function fetchVideoInfo() {
        const url = urlInput.value.trim();
        if (!url || isFetchingInfo) return;
        
        isFetchingInfo = true;
        document.getElementById("paste-text").style.display = "none";
        document.getElementById("paste-spinner").style.display = "block";
        previewCard.classList.remove("visible");
        downloadBtn.disabled = true;
        currentValidUrl = "";

        try {
            const res = await fetch(`${API_BASE}/api/info`, {
                method: "POST", headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ url })
            });
            const data = await res.json();
            if (!res.ok || !data.success) throw new Error(data.error);
            
            document.getElementById("preview-title").textContent = data.title;
            document.getElementById("preview-duration").textContent = `⏱ ${formatTime(data.duration)}`;
            
            const thumbEl = document.getElementById("preview-thumb");
            if(data.thumbnail) {
                thumbEl.innerHTML = `<img src="${data.thumbnail}" style="width:100%; height:100%; object-fit:cover;">`;
            } else {
                thumbEl.innerHTML = `<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="2" y="2" width="20" height="20" rx="2"/><path d="m9 9 6 6M15 9l-6 6"/></svg>`;
            }
            
            previewCard.classList.add("visible");
            currentValidUrl = url;
            downloadBtn.disabled = false;

        } catch (err) {
            showToast("error", "Info Gagal", err.message);
        } finally {
            isFetchingInfo = false;
            document.getElementById("paste-text").style.display = "block";
            document.getElementById("paste-spinner").style.display = "none";
        }
    }

    pasteBtn.addEventListener("click", fetchVideoInfo);
    urlInput.addEventListener("change", fetchVideoInfo);
    urlInput.addEventListener("keydown", (e) => { if (e.key === "Enter") fetchVideoInfo(); });

    cancelBtn.addEventListener("click", async () => {
        if(!currentTaskId) return;
        cancelBtn.textContent = "Membatalkan...";
        try {
            await fetch(`${API_BASE}/api/cancel/${currentTaskId}`, {method: "POST"});
            showToast("warning", "Dibatalkan", "Sinyal pembatalan dikirim ke server.");
        } catch(e) {}
    });

    function resetUI() {
      isLoading = false; currentTaskId = "";
      downloadBtn.disabled = false; downloadBtn.classList.remove("loading");
      document.getElementById("progress-wrap").classList.remove("visible");
      if (eventSource) { eventSource.close(); eventSource = null; }
      cancelBtn.textContent = "Batalkan Proses";
    }

    function startSSE(taskId) {
        if (eventSource) eventSource.close();
        eventSource = new EventSource(`${API_BASE}/api/stream/${taskId}`);
        
        eventSource.onmessage = function(e) {
            const data = JSON.parse(e.data);
            
            if (data.status === 'processing') {
                document.getElementById("progress-label").textContent = "Mengunduh...";
                if(data.progress) {
                    document.getElementById("progress-fill").style.width = data.progress;
                    document.getElementById("progress-pct").textContent = data.progress;
                }
                if(data.eta) document.getElementById("progress-eta").textContent = `ETA: ${data.eta}`;
                if(data.speed) document.getElementById("progress-speed").textContent = `${data.speed}`;
            } 
            else if (data.status === 'done') {
                eventSource.close();
                document.getElementById("progress-fill").style.width = "100%";
                document.getElementById("progress-pct").textContent = "SIAP!";
                document.getElementById("progress-label").textContent = "File Siap! ✓";
                
                document.getElementById("result-title").textContent = data.data.title;
                document.getElementById("result-card").classList.add("visible");
                
                const a = document.createElement("a");
                a.href = data.data.download_url; a.target = "_blank"; a.rel = "noopener noreferrer";
                document.body.appendChild(a); a.click(); document.body.removeChild(a);
                
                showToast("success", "Selesai!", "File didownload ke perangkat Anda.");
                updateHistory(data.data.title, currentValidUrl);
                setTimeout(resetUI, 1500);
            } 
            else if (data.status === 'error') {
                eventSource.close();
                showToast("error", "Gagal Memproses", data.message);
                resetUI();
            }
        };

        eventSource.onerror = function() {
            eventSource.close();
            // Silent fallback just in case the stream drops but the process is still running
            resetUI();
        };
    }

    downloadBtn.addEventListener("click", async () => {
      if (isLoading || !currentValidUrl) return;

      isLoading = true; 
      downloadBtn.disabled = true; downloadBtn.classList.add("loading");
      document.getElementById("result-card").classList.remove("visible");
      
      const wrap = document.getElementById("progress-wrap");
      wrap.classList.add("visible"); 
      document.getElementById("progress-fill").style.width = "0%";
      document.getElementById("progress-pct").textContent = "Mulai";

      let finalFormat = selectedBaseFormat;
      if (selectedBaseFormat === "mp3") finalFormat = `mp3-${selectedBitrate}`;

      try {
        const response = await fetch(`${API_BASE}/api/fetch_async`, {
          method: "POST", headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ url: currentValidUrl, format: finalFormat })
        });
        const data = await response.json();
        
        if (response.status === 429) throw new Error("Terlalu banyak permintaan. Limit IP Anda.");
        if (response.status === 503) throw new Error("Server sedang kepenuhan (Overload). Coba beberapa saat lagi.");
        if (!response.ok || !data.success) throw new Error(data.error);

        if (data.cached) {
            document.getElementById("progress-fill").style.width = "100%";
            document.getElementById("progress-pct").textContent = "CACHE HIT!";
            document.getElementById("progress-label").textContent = "Ditemukan di Cache Server! ✓";
            
            document.getElementById("result-title").textContent = data.data.title;
            document.getElementById("result-card").classList.add("visible");
            
            const a = document.createElement("a");
            a.href = data.data.download_url; a.target = "_blank"; a.rel = "noopener noreferrer";
            document.body.appendChild(a); a.click(); document.body.removeChild(a);
            
            showToast("success", "Instant Download!", "File diambil dari Cache Server.");
            updateHistory(data.data.title, currentValidUrl);
            setTimeout(resetUI, 1500);
        } else if (data.task_id) {
            currentTaskId = data.task_id;
            startSSE(data.task_id); 
        }

      } catch (err) {
        showToast("error", "Error", err.message);
        resetUI();
      }
    });
  </script>
</body>
</html>
"""

# ==========================================
# ROUTING FLASK (API)
# ==========================================
@app.route('/')
def index():
    return HTML_CONTENT

@app.route('/health', methods=['GET'])
def health_check():
    import shutil
    total, used, free = shutil.disk_usage("/")
    free_gb = free // (2**30)
    
    with lock_tasks:
        queue_size = task_queue.qsize()
        active_tasks = sum(1 for v in task_status_db.values() if v.get('status') == 'processing')
        
    return jsonify({
        "status": "healthy",
        "yt_dlp_version": yt_dlp.version.__version__,
        "disk_free_gb": free_gb,
        "active_downloads": active_tasks,
        "queue_length": queue_size
    }), 200

@app.route('/api/info', methods=['POST'])
def api_info():
    hashed_ip = hash_ip(request.remote_addr)
    if is_rate_limited(hashed_ip): return jsonify({"success": False, "error": "Terlalu banyak request."}), 429
    url = request.json.get('url', '').strip()
    
    if len(url) > 2000:
        return jsonify({"success": False, "error": "URL terlalu panjang."}), 400
    
    if not url or is_blacklisted(url): 
        return jsonify({"success": False, "error": "URL kosong atau domain tidak diizinkan."}), 400

    ydl_opts = {'quiet': True, 'extract_flat': False, 'noplaylist': True, 'socket_timeout': 10}
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            if info.get('is_live'): return jsonify({"success": False, "error": "Ditolak: Tidak bisa mendownload Live Stream."}), 400
            
            estimated_size = info.get('filesize') or info.get('filesize_approx') or 0
            if estimated_size > MAX_FILESIZE_BYTES:
                return jsonify({"success": False, "error": "Estimasi ukuran video terlalu besar untuk server ini."}), 400
                
            return jsonify({"success": True, "title": info.get('title', 'Video Tidak Berjudul'), "thumbnail": info.get('thumbnail', ''), "duration": info.get('duration', 0)})
    except Exception as e:
        return jsonify({"success": False, "error": parse_ytdlp_error(e)}), 400

# [MENGATASI 4] SSE GENERATOREXIT FIX
@app.route('/api/stream/<task_id>')
def stream_status(task_id):
    def generate():
        try:
            while True:
                with lock_tasks: task = task_status_db.get(task_id)
                if not task:
                    yield f"data: {json.dumps({'status': 'error', 'message': 'Tugas hilang dari memori server'})}\n\n"
                    break
                yield f"data: {json.dumps(task)}\n\n"
                if task['status'] in ['done', 'error']: break
                time.sleep(0.5) 
        except GeneratorExit:
            return
            
    return Response(generate(), mimetype='text/event-stream')


@app.route('/api/cancel/<task_id>', methods=['POST'])
def cancel_task(task_id):
    with lock_cancel:
        tugas_dibatalkan.add(task_id)
    with lock_tasks:
        if task_id in task_status_db and task_status_db[task_id]['status'] == 'processing':
            task_status_db[task_id]['status'] = 'error'
            task_status_db[task_id]['message'] = 'Proses dihentikan oleh pengguna.'
    return jsonify({"success": True})


@app.route('/api/fetch_async', methods=['POST'])
def api_fetch_async():
    hashed_ip = hash_ip(request.remote_addr)
    if is_rate_limited(hashed_ip): return jsonify({"success": False, "error": "Terlalu banyak permintaan."}), 429

    data = request.json
    url = data.get('url', '').strip()
    fmt_pilihan = data.get('format', 'mp4-720p') 
    
    if len(url) > 2000: return jsonify({"success": False, "error": "URL terlalu panjang."}), 400
    if not url or is_blacklisted(url): return jsonify({"success": False, "error": "URL kosong atau ditolak."}), 400

    diizinkan, pesan_error = reservasi_kuota(hashed_ip, url)
    if not diizinkan: return jsonify({"success": False, "error": pesan_error}), 403

    cache_key = f"{url}_{fmt_pilihan}"
    with lock_cache:
        if cache_key in url_cache_db:
            cache_data = url_cache_db[cache_key]
            if os.path.exists(os.path.join(DOWNLOAD_DIR, cache_data['filename'])):
                token_url = url_signer.dumps(cache_data['filename'])
                cache_data['timestamp'] = time.time() 
                
                selesaikan_kuota(hashed_ip, url, actual_size_bytes=0, success=True)
                
                return jsonify({"success": True, "cached": True, "data": {"title": cache_data['title'], "download_url": f"/download/{token_url}"}})
            else: del url_cache_db[cache_key]

    task_id = str(uuid.uuid4())
    uid_acak = uuid.uuid4().hex[:8]
    
    with lock_tasks: task_status_db[task_id] = {"status": "processing", "timestamp": time.time(), "progress": "0%"}
    
    # [MENGATASI 3] Batasan Queue Penuh
    try:
        task_queue.put({'task_id': task_id, 'url': url, 'format': fmt_pilihan, 'uid': uid_acak, 'hashed_ip': hashed_ip}, block=False)
    except queue.Full:
        selesaikan_kuota(hashed_ip, url, actual_size_bytes=0, success=False) # Refund kuota karena mental
        return jsonify({"success": False, "error": "Server sedang penuh (Overload). Harap coba beberapa saat lagi."}), 503
    
    return jsonify({"success": True, "cached": False, "task_id": task_id})

@app.route('/download/<token>')
def download_file(token):
    try: nama_file_asli = url_signer.loads(token, max_age=3600)
    except SignatureExpired: return "Tautan unduhan telah kedaluwarsa.", 403
    except BadSignature: return "Tautan unduhan tidak valid.", 403

    safe_filename = secure_filename(nama_file_asli)
    ext = os.path.splitext(safe_filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS: return "Format file ditolak.", 403

    safe_dir = os.path.abspath(DOWNLOAD_DIR)
    file_path = os.path.abspath(os.path.join(safe_dir, safe_filename))
    
    if not file_path.startswith(safe_dir): return "Path Traversal Detected.", 403
    if os.path.exists(file_path): return send_file(file_path, as_attachment=True)
    return "File tidak ditemukan atau telah dihapus.", 404

# ==========================================
# MENJALANKAN SERVER
# ==========================================
if __name__ == '__main__':
    # Di Windows, multiprocessing butuh spawn protection
    multiprocessing.freeze_support()

    gc_thread = threading.Thread(target=bersihkan_sampah_sistem)
    gc_thread.daemon = True
    gc_thread.start()
    
    for _ in range(MAKSIMAL_WORKER):
        t = threading.Thread(target=worker_download)
        t.daemon = True
        t.start()
    
    print("===========================================")
    print("[√] VORTEX SERVER V9 (THE FORTRESS EDITION)")
    print("[√] Multiprocessing Hard-Kill Timeout Aktif")
    print("[√] Strict Queue Bounding & 503 Handling")
    print("[√] Security Environment Enforced")
    print("[√] Server Berjalan Menggunakan Waitress (WSGI)")
    print("[√] Buka di browser: http://localhost:5000")
    print("===========================================")
    
    serve(app, host='0.0.0.0', port=5000, threads=12)
