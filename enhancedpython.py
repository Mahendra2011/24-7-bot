# -*- coding: utf-8 -*-
import telebot
import subprocess
import os
import zipfile
import tempfile
import shutil
from telebot import types
import time
from datetime import datetime, timedelta
import logging
import psutil
import sqlite3
import threading
import re
import sys
import atexit
import requests
import json
from PIL import Image, ImageDraw, ImageFont
import qrcode
import io

# --- Flask Keep Alive ---
from flask import Flask
from threading import Thread

app = Flask('')

@app.route('/')
def home():
    return "I'm Marco File Host - Enhanced Version"

def run_flask():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run_flask)
    t.daemon = True
    t.start()
    print("Flask Keep-Alive server started.")
# --- End Flask Keep Alive ---

# --- Configuration ---
TOKEN = '8526128320:AAEI2SyuFoT1DRKXZFNKOTN3W3fHWtHFYjQ'
OWNER_ID = 7855338525
ADMIN_ID = 7855338525
YOUR_USERNAME = '@Deleted0Account'
UPDATE_CHANNEL = 'https://t.me/+ThhtU6Jx0MI2ZGRl'

# Payment Configuration
PAYMENT_CONFIG_FILE = 'payment_config.json'
DEFAULT_PAYMENT_CONFIG = {
    'upi_id': 'yourupi@bank',
    'qr_code_enabled': True,
    'subscription_price_30_days': 299,
    'subscription_price_90_days': 799,
    'subscription_price_180_days': 1499
}

# Inline Button Links Configuration
INLINE_LINKS_FILE = 'inline_links.json'
DEFAULT_INLINE_LINKS = {
    'updates_channel': 'https://t.me/+ThhtU6Jx0MI2ZGRl',
    'support_group': 'https://t.me/your_support_group',
    'tutorial_channel': 'https://t.me/your_tutorial_channel',
    'github_repo': 'https://github.com/your/repo',
    'donation_link': 'https://paypal.me/yourdonation'
}

# Folder setup
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_BOTS_DIR = os.path.join(BASE_DIR, 'upload_bots')
IROTECH_DIR = os.path.join(BASE_DIR, 'inf')
DATABASE_PATH = os.path.join(IROTECH_DIR, 'bot_data.db')
PROBLEMS_DIR = os.path.join(IROTECH_DIR, 'problems')

# File upload limits
FREE_USER_LIMIT = 5
SUBSCRIBED_USER_LIMIT = 15
ADMIN_LIMIT = 999
OWNER_LIMIT = float('inf')

# Create necessary directories
os.makedirs(UPLOAD_BOTS_DIR, exist_ok=True)
os.makedirs(IROTECH_DIR, exist_ok=True)
os.makedirs(PROBLEMS_DIR, exist_ok=True)

# Initialize bot
bot = telebot.TeleBot(TOKEN)

# --- Data structures ---
bot_scripts = {}
user_subscriptions = {}
user_files = {}
active_users = set()
admin_ids = {ADMIN_ID, OWNER_ID}
banned_users = set()
bot_locked = False

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Load Configuration Files ---
def load_payment_config():
    """Load payment configuration from file"""
    try:
        if os.path.exists(PAYMENT_CONFIG_FILE):
            with open(PAYMENT_CONFIG_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Error loading payment config: {e}")
    return DEFAULT_PAYMENT_CONFIG.copy()

def save_payment_config(config):
    """Save payment configuration to file"""
    try:
        with open(PAYMENT_CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=4)
        return True
    except Exception as e:
        logger.error(f"Error saving payment config: {e}")
        return False

def load_inline_links():
    """Load inline button links from file"""
    try:
        if os.path.exists(INLINE_LINKS_FILE):
            with open(INLINE_LINKS_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Error loading inline links: {e}")
    return DEFAULT_INLINE_LINKS.copy()

def save_inline_links(links):
    """Save inline button links to file"""
    try:
        with open(INLINE_LINKS_FILE, 'w') as f:
            json.dump(links, f, indent=4)
        return True
    except Exception as e:
        logger.error(f"Error saving inline links: {e}")
        return False

# Load configurations
payment_config = load_payment_config()
inline_links = load_inline_links()

# --- Command Button Layouts (ReplyKeyboardMarkup) ---
COMMAND_BUTTONS_LAYOUT_USER_SPEC = [
    ["📢 Updates Channel"],
    ["📤 Upload File", "📂 Check Files"],
    ["⚡ Bot Speed", "📊 Statistics"],
    ["💳 Buy Subscription", "📝 Submit Problem"],
    ["📞 Contact Owner"]
]

ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC = [
    ["📢 Updates Channel"],
    ["📤 Upload File", "📂 Check Files"],
    ["⚡ Bot Speed", "📊 Statistics"],
    ["💳 Subscriptions", "📢 Broadcast"],
    ["🔒 Lock Bot", "🟢 Running All Code"],
    ["👑 Admin Panel", "👥 User Management"],
    ["💰 Payment Settings", "🔗 Edit Links"],
    ["📝 View Problems", "📞 Contact Owner"]
]

# --- Database Setup ---
def init_db():
    """Initialize the database with required tables"""
    logger.info(f"Initializing database at: {DATABASE_PATH}")
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        
        # Existing tables
        c.execute('''CREATE TABLE IF NOT EXISTS subscriptions
                     (user_id INTEGER PRIMARY KEY, expiry TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS user_files
                     (user_id INTEGER, file_name TEXT, file_type TEXT,
                      PRIMARY KEY (user_id, file_name))''')
        c.execute('''CREATE TABLE IF NOT EXISTS active_users
                     (user_id INTEGER PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS admins
                     (user_id INTEGER PRIMARY KEY)''')
        
        # New tables for enhanced features
        c.execute('''CREATE TABLE IF NOT EXISTS banned_users
                     (user_id INTEGER PRIMARY KEY, reason TEXT, banned_by INTEGER, banned_at TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS problems
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      user_id INTEGER, user_name TEXT,
                      problem_text TEXT, status TEXT DEFAULT 'pending',
                      created_at TEXT, resolved_at TEXT)''')
        
        # Ensure owner and initial admin are in admins table
        c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (OWNER_ID,))
        if ADMIN_ID != OWNER_ID:
            c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (ADMIN_ID,))
        
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"❌ Database initialization error: {e}", exc_info=True)

def load_data():
    """Load data from database into memory"""
    logger.info("Loading data from database...")
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()

        # Load subscriptions
        c.execute('SELECT user_id, expiry FROM subscriptions')
        for user_id, expiry in c.fetchall():
            try:
                user_subscriptions[user_id] = {'expiry': datetime.fromisoformat(expiry)}
            except ValueError:
                logger.warning(f"⚠️ Invalid expiry date format for user {user_id}: {expiry}. Skipping.")

        # Load user files
        c.execute('SELECT user_id, file_name, file_type FROM user_files')
        for user_id, file_name, file_type in c.fetchall():
            if user_id not in user_files:
                user_files[user_id] = []
            user_files[user_id].append((file_name, file_type))

        # Load active users
        c.execute('SELECT user_id FROM active_users')
        active_users.update(user_id for (user_id,) in c.fetchall())

        # Load admins
        c.execute('SELECT user_id FROM admins')
        admin_ids.update(user_id for (user_id,) in c.fetchall())
        
        # Load banned users
        c.execute('SELECT user_id FROM banned_users')
        banned_users.update(user_id for (user_id,) in c.fetchall())

        conn.close()
        logger.info(f"Data loaded: {len(active_users)} users, {len(user_subscriptions)} subscriptions, {len(admin_ids)} admins, {len(banned_users)} banned users.")
    except Exception as e:
        logger.error(f"❌ Error loading data: {e}", exc_info=True)

# Initialize DB and Load Data at startup
init_db()
load_data()

# --- Helper Functions ---
def generate_payment_qr(upi_id, amount=None):
    """Generate QR code for UPI payment"""
    try:
        # Create UPI payment string
        if amount:
            upi_string = f"upi://pay?pa={upi_id}&pn=Bot%20Subscription&am={amount}&cu=INR&tn=Bot%20Subscription%20Payment"
        else:
            upi_string = f"upi://pay?pa={upi_id}&pn=Bot%20Subscription&cu=INR&tn=Bot%20Subscription%20Payment"
        
        # Generate QR code
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_L,
            box_size=10,
            border=4,
        )
        qr.add_data(upi_string)
        qr.make(fit=True)
        
        # Create QR code image
        qr_img = qr.make_image(fill_color="black", back_color="white")
        
        # Add text below QR
        img_width, img_height = qr_img.size
        new_height = img_height + 50
        new_img = Image.new('RGB', (img_width, new_height), 'white')
        new_img.paste(qr_img, (0, 0))
        
        draw = ImageDraw.Draw(new_img)
        try:
            font = ImageFont.truetype("arial.ttf", 16)
        except:
            font = ImageFont.load_default()
        
        # Add UPI ID text
        text = f"UPI ID: {upi_id}"
        text_width = draw.textlength(text, font=font)
        draw.text(((img_width - text_width) // 2, img_height + 10), 
                 text, fill="black", font=font)
        
        if amount:
            amount_text = f"Amount: ₹{amount}"
            amount_width = draw.textlength(amount_text, font=font)
            draw.text(((img_width - amount_width) // 2, img_height + 30), 
                     amount_text, fill="black", font=font)
        
        # Convert to bytes
        img_byte_arr = io.BytesIO()
        new_img.save(img_byte_arr, format='PNG')
        img_byte_arr.seek(0)
        
        return img_byte_arr
    except Exception as e:
        logger.error(f"Error generating QR code: {e}")
        return None

def is_user_banned(user_id):
    """Check if user is banned"""
    return user_id in banned_users

def get_ban_reason(user_id):
    """Get ban reason for user"""
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute('SELECT reason FROM banned_users WHERE user_id = ?', (user_id,))
        result = c.fetchone()
        conn.close()
        return result[0] if result else "No reason provided"
    except Exception as e:
        logger.error(f"Error getting ban reason: {e}")
        return "Unknown"

def ban_user(user_id, reason, banned_by):
    """Ban a user"""
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        banned_at = datetime.now().isoformat()
        c.execute('INSERT OR REPLACE INTO banned_users (user_id, reason, banned_by, banned_at) VALUES (?, ?, ?, ?)',
                  (user_id, reason, banned_by, banned_at))
        conn.commit()
        conn.close()
        banned_users.add(user_id)
        
        # Kill any running bots for banned user
        for script_key, script_info in list(bot_scripts.items()):
            if script_info.get('script_owner_id') == user_id:
                kill_process_tree(script_info)
                if script_key in bot_scripts:
                    del bot_scripts[script_key]
        
        logger.warning(f"User {user_id} banned by {banned_by}. Reason: {reason}")
        return True
    except Exception as e:
        logger.error(f"Error banning user {user_id}: {e}")
        return False

def unban_user(user_id):
    """Unban a user"""
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute('DELETE FROM banned_users WHERE user_id = ?', (user_id,))
        conn.commit()
        conn.close()
        banned_users.discard(user_id)
        logger.warning(f"User {user_id} unbanned")
        return True
    except Exception as e:
        logger.error(f"Error unbanning user {user_id}: {e}")
        return False

def save_problem(user_id, user_name, problem_text):
    """Save user problem to database"""
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        created_at = datetime.now().isoformat()
        c.execute('INSERT INTO problems (user_id, user_name, problem_text, created_at) VALUES (?, ?, ?, ?)',
                  (user_id, user_name, problem_text, created_at))
        conn.commit()
        problem_id = c.lastrowid
        conn.close()
        
        # Save problem text to file
        problem_file = os.path.join(PROBLEMS_DIR, f"problem_{problem_id}.txt")
        with open(problem_file, 'w', encoding='utf-8') as f:
            f.write(f"User ID: {user_id}\n")
            f.write(f"Username: {user_name}\n")
            f.write(f"Date: {created_at}\n")
            f.write(f"Problem:\n{problem_text}\n")
        
        logger.info(f"Problem saved: ID={problem_id}, User={user_id}")
        return problem_id
    except Exception as e:
        logger.error(f"Error saving problem: {e}")
        return None

def get_pending_problems():
    """Get all pending problems"""
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute('SELECT id, user_id, user_name, problem_text, created_at FROM problems WHERE status = "pending" ORDER BY created_at DESC')
        problems = c.fetchall()
        conn.close()
        return problems
    except Exception as e:
        logger.error(f"Error getting problems: {e}")
        return []

def get_problem_by_id(problem_id):
    """Get problem by ID"""
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute('SELECT * FROM problems WHERE id = ?', (problem_id,))
        problem = c.fetchone()
        conn.close()
        return problem
    except Exception as e:
        logger.error(f"Error getting problem {problem_id}: {e}")
        return None

def update_problem_status(problem_id, status, resolved_by=None):
    """Update problem status"""
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        if status == 'resolved':
            resolved_at = datetime.now().isoformat()
            c.execute('UPDATE problems SET status = ?, resolved_at = ? WHERE id = ?',
                      (status, resolved_at, problem_id))
        else:
            c.execute('UPDATE problems SET status = ? WHERE id = ?',
                      (status, problem_id))
        conn.commit()
        conn.close()
        logger.info(f"Problem {problem_id} status updated to {status}")
        return True
    except Exception as e:
        logger.error(f"Error updating problem status: {e}")
        return False

def get_user_folder(user_id):
    """Get or create user's folder for storing files"""
    user_folder = os.path.join(UPLOAD_BOTS_DIR, str(user_id))
    os.makedirs(user_folder, exist_ok=True)
    return user_folder

def get_user_file_limit(user_id):
    """Get the file upload limit for a user"""
    if is_user_banned(user_id):
        return 0
    
    if user_id == OWNER_ID:
        return OWNER_LIMIT
    if user_id in admin_ids:
        return ADMIN_LIMIT
    if user_id in user_subscriptions and user_subscriptions[user_id]['expiry'] > datetime.now():
        return SUBSCRIBED_USER_LIMIT
    return FREE_USER_LIMIT

def get_user_file_count(user_id):
    """Get the number of files uploaded by a user"""
    return len(user_files.get(user_id, []))

def is_bot_running(script_owner_id, file_name):
    """Check if a bot script is currently running"""
    script_key = f"{script_owner_id}_{file_name}"
    script_info = bot_scripts.get(script_key)
    if script_info and script_info.get('process'):
        try:
            proc = psutil.Process(script_info['process'].pid)
            is_running = proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
            if not is_running:
                if 'log_file' in script_info and hasattr(script_info['log_file'], 'close') and not script_info['log_file'].closed:
                    try:
                        script_info['log_file'].close()
                    except Exception as log_e:
                        logger.error(f"Error closing log file during cleanup {script_key}: {log_e}")
                if script_key in bot_scripts:
                    del bot_scripts[script_key]
            return is_running
        except psutil.NoSuchProcess:
            if 'log_file' in script_info and hasattr(script_info['log_file'], 'close') and not script_info['log_file'].closed:
                try:
                    script_info['log_file'].close()
                except Exception as log_e:
                    logger.error(f"Error closing log file during cleanup of non-existent process {script_key}: {log_e}")
            if script_key in bot_scripts:
                del bot_scripts[script_key]
            return False
        except Exception as e:
            logger.error(f"Error checking process status for {script_key}: {e}")
            return False
    return False

def kill_process_tree(process_info):
    """Kill a process and all its children"""
    pid = None
    script_key = process_info.get('script_key', 'N/A')

    try:
        if 'log_file' in process_info and hasattr(process_info['log_file'], 'close') and not process_info['log_file'].closed:
            try:
                process_info['log_file'].close()
            except Exception as log_e:
                logger.error(f"Error closing log file during kill for {script_key}: {log_e}")

        process = process_info.get('process')
        if process and hasattr(process, 'pid'):
            pid = process.pid
            if pid:
                try:
                    parent = psutil.Process(pid)
                    children = parent.children(recursive=True)
                    
                    for child in children:
                        try:
                            child.terminate()
                        except:
                            pass

                    try:
                        parent.terminate()
                        parent.wait(timeout=1)
                    except:
                        try:
                            parent.kill()
                        except:
                            pass

                except psutil.NoSuchProcess:
                    pass
    except Exception as e:
        logger.error(f"Error killing process tree for {script_key}: {e}")

# --- Map Telegram import names to actual PyPI package names ---
TELEGRAM_MODULES = {
    'telebot': 'pyTelegramBotAPI',
    'telegram': 'python-telegram-bot',
    'python_telegram_bot': 'python-telegram-bot',
    'aiogram': 'aiogram',
    'pyrogram': 'pyrogram',
    'telethon': 'telethon',
    'telethon.sync': 'telethon',
    'from telethon.sync import telegramclient': 'telethon',
    'telepot': 'telepot',
    'pytg': 'pytg',
    'tgcrypto': 'tgcrypto',
    'telegram_upload': 'telegram-upload',
    'telegram_send': 'telegram-send',
    'telegram_text': 'telegram-text',
    'bs4': 'beautifulsoup4',
    'requests': 'requests',
    'pillow': 'Pillow',
    'cv2': 'opencv-python',
    'yaml': 'PyYAML',
    'dotenv': 'python-dotenv',
    'dateutil': 'python-dateutil',
    'pandas': 'pandas',
    'numpy': 'numpy',
    'flask': 'Flask',
    'django': 'Django',
    'sqlalchemy': 'SQLAlchemy',
    'asyncio': None,
    'json': None,
    'datetime': None,
    'os': None,
    'sys': None,
    're': None,
    'time': None,
    'math': None,
    'random': None,
    'logging': None,
    'threading': None,
    'subprocess': None,
    'zipfile': None,
    'tempfile': None,
    'shutil': None,
    'sqlite3': None,
    'psutil': 'psutil',
    'atexit': None
}

# --- Automatic Package Installation & Script Running ---
def attempt_install_pip(module_name, message):
    package_name = TELEGRAM_MODULES.get(module_name.lower(), module_name)
    if package_name is None:
        logger.info(f"Module '{module_name}' is core. Skipping pip install.")
        return False
    try:
        bot.reply_to(message, f"🐍 Module `{module_name}` not found. Installing `{package_name}`...", parse_mode='Markdown')
        command = [sys.executable, '-m', 'pip', 'install', package_name]
        logger.info(f"Running install: {' '.join(command)}")
        result = subprocess.run(command, capture_output=True, text=True, check=False, encoding='utf-8', errors='ignore')
        if result.returncode == 0:
            logger.info(f"Installed {package_name}. Output:\n{result.stdout}")
            bot.reply_to(message, f"✅ Package `{package_name}` (for `{module_name}`) installed.", parse_mode='Markdown')
            return True
        else:
            error_msg = f"❌ Failed to install `{package_name}` for `{module_name}`.\nLog:\n```\n{result.stderr or result.stdout}\n```"
            logger.error(error_msg)
            if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Log truncated)"
            bot.reply_to(message, error_msg, parse_mode='Markdown')
            return False
    except Exception as e:
        error_msg = f"❌ Error installing `{package_name}`: {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message, error_msg)
        return False

def attempt_install_npm(module_name, user_folder, message):
    try:
        bot.reply_to(message, f"🟠 Node package `{module_name}` not found. Installing locally...", parse_mode='Markdown')
        command = ['npm', 'install', module_name]
        logger.info(f"Running npm install: {' '.join(command)} in {user_folder}")
        result = subprocess.run(command, capture_output=True, text=True, check=False, cwd=user_folder, encoding='utf-8', errors='ignore')
        if result.returncode == 0:
            logger.info(f"Installed {module_name}. Output:\n{result.stdout}")
            bot.reply_to(message, f"✅ Node package `{module_name}` installed locally.", parse_mode='Markdown')
            return True
        else:
            error_msg = f"❌ Failed to install Node package `{module_name}`.\nLog:\n```\n{result.stderr or result.stdout}\n```"
            logger.error(error_msg)
            if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Log truncated)"
            bot.reply_to(message, error_msg, parse_mode='Markdown')
            return False
    except FileNotFoundError:
         error_msg = "❌ Error: 'npm' not found. Ensure Node.js/npm are installed and in PATH."
         logger.error(error_msg)
         bot.reply_to(message, error_msg)
         return False
    except Exception as e:
        error_msg = f"❌ Error installing Node package `{module_name}`: {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message, error_msg)
        return False

def run_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt=1):
    """Run Python script"""
    max_attempts = 2
    if attempt > max_attempts:
        bot.reply_to(message_obj_for_reply, f"❌ Failed to run '{file_name}' after {max_attempts} attempts. Check logs.")
        return

    script_key = f"{script_owner_id}_{file_name}"
    logger.info(f"Attempt {attempt} to run Python script: {script_path} (Key: {script_key}) for user {script_owner_id}")

    try:
        if not os.path.exists(script_path):
             bot.reply_to(message_obj_for_reply, f"❌ Error: Script '{file_name}' not found at '{script_path}'!")
             logger.error(f"Script not found: {script_path} for user {script_owner_id}")
             if script_owner_id in user_files:
                 user_files[script_owner_id] = [f for f in user_files.get(script_owner_id, []) if f[0] != file_name]
             remove_user_file_db(script_owner_id, file_name)
             return

        if attempt == 1:
            check_command = [sys.executable, script_path]
            logger.info(f"Running Python pre-check: {' '.join(check_command)}")
            check_proc = None
            try:
                check_proc = subprocess.Popen(check_command, cwd=user_folder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore')
                stdout, stderr = check_proc.communicate(timeout=5)
                return_code = check_proc.returncode
                logger.info(f"Python Pre-check early. RC: {return_code}. Stderr: {stderr[:200]}...")
                if return_code != 0 and stderr:
                    match_py = re.search(r"ModuleNotFoundError: No module named '(.+?)'", stderr)
                    if match_py:
                        module_name = match_py.group(1).strip().strip("'\"")
                        logger.info(f"Detected missing Python module: {module_name}")
                        if attempt_install_pip(module_name, message_obj_for_reply):
                            logger.info(f"Install OK for {module_name}. Retrying run_script...")
                            bot.reply_to(message_obj_for_reply, f"🔄 Install successful. Retrying '{file_name}'...")
                            time.sleep(2)
                            threading.Thread(target=run_script, args=(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt + 1)).start()
                            return
                        else:
                            bot.reply_to(message_obj_for_reply, f"❌ Install failed. Cannot run '{file_name}'.")
                            return
                    else:
                         error_summary = stderr[:500]
                         bot.reply_to(message_obj_for_reply, f"❌ Error in script pre-check for '{file_name}':\n```\n{error_summary}\n```\nFix the script.", parse_mode='Markdown')
                         return
            except subprocess.TimeoutExpired:
                logger.info("Python Pre-check timed out (>5s), imports likely OK. Killing check process.")
                if check_proc and check_proc.poll() is None: check_proc.kill(); check_proc.communicate()
                logger.info("Python Check process killed. Proceeding to long run.")
            except FileNotFoundError:
                 logger.error(f"Python interpreter not found: {sys.executable}")
                 bot.reply_to(message_obj_for_reply, f"❌ Error: Python interpreter '{sys.executable}' not found.")
                 return
            except Exception as e:
                 logger.error(f"Error in Python pre-check for {script_key}: {e}", exc_info=True)
                 bot.reply_to(message_obj_for_reply, f"❌ Unexpected error in script pre-check for '{file_name}': {e}")
                 return
            finally:
                 if check_proc and check_proc.poll() is None:
                     logger.warning(f"Python Check process {check_proc.pid} still running. Killing.")
                     check_proc.kill(); check_proc.communicate()

        logger.info(f"Starting long-running Python process for {script_key}")
        log_file_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        log_file = None; process = None
        try: log_file = open(log_file_path, 'w', encoding='utf-8', errors='ignore')
        except Exception as e:
             logger.error(f"Failed to open log file '{log_file_path}' for {script_key}: {e}", exc_info=True)
             bot.reply_to(message_obj_for_reply, f"❌ Failed to open log file '{log_file_path}': {e}")
             return
        try:
            startupinfo = None; creationflags = 0
            if os.name == 'nt':
                 startupinfo = subprocess.STARTUPINFO(); startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                 startupinfo.wShowWindow = subprocess.SW_HIDE
            process = subprocess.Popen(
                [sys.executable, script_path], cwd=user_folder, stdout=log_file, stderr=log_file,
                stdin=subprocess.PIPE, startupinfo=startupinfo, creationflags=creationflags,
                encoding='utf-8', errors='ignore'
            )
            logger.info(f"Started Python process {process.pid} for {script_key}")
            bot_scripts[script_key] = {
                'process': process, 'log_file': log_file, 'file_name': file_name,
                'chat_id': message_obj_for_reply.chat.id,
                'script_owner_id': script_owner_id,
                'start_time': datetime.now(), 'user_folder': user_folder, 'type': 'py', 'script_key': script_key
            }
            bot.reply_to(message_obj_for_reply, f"✅ Python script '{file_name}' started! (PID: {process.pid}) (For User: {script_owner_id})")
        except FileNotFoundError:
             logger.error(f"Python interpreter {sys.executable} not found for long run {script_key}")
             bot.reply_to(message_obj_for_reply, f"❌ Error: Python interpreter '{sys.executable}' not found.")
             if log_file and not log_file.closed: log_file.close()
             if script_key in bot_scripts: del bot_scripts[script_key]
        except Exception as e:
            if log_file and not log_file.closed: log_file.close()
            error_msg = f"❌ Error starting Python script '{file_name}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            bot.reply_to(message_obj_for_reply, error_msg)
            if process and process.poll() is None:
                 logger.warning(f"Killing potentially started Python process {process.pid} for {script_key}")
                 kill_process_tree({'process': process, 'log_file': log_file, 'script_key': script_key})
            if script_key in bot_scripts: del bot_scripts[script_key]
    except Exception as e:
        error_msg = f"❌ Unexpected error running Python script '{file_name}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message_obj_for_reply, error_msg)
        if script_key in bot_scripts:
             logger.warning(f"Cleaning up {script_key} due to error in run_script.")
             kill_process_tree(bot_scripts[script_key])
             del bot_scripts[script_key]

def run_js_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt=1):
    """Run JS script"""
    max_attempts = 2
    if attempt > max_attempts:
        bot.reply_to(message_obj_for_reply, f"❌ Failed to run '{file_name}' after {max_attempts} attempts. Check logs.")
        return

    script_key = f"{script_owner_id}_{file_name}"
    logger.info(f"Attempt {attempt} to run JS script: {script_path} (Key: {script_key}) for user {script_owner_id}")

    try:
        if not os.path.exists(script_path):
             bot.reply_to(message_obj_for_reply, f"❌ Error: Script '{file_name}' not found at '{script_path}'!")
             logger.error(f"JS Script not found: {script_path} for user {script_owner_id}")
             if script_owner_id in user_files:
                 user_files[script_owner_id] = [f for f in user_files.get(script_owner_id, []) if f[0] != file_name]
             remove_user_file_db(script_owner_id, file_name)
             return

        if attempt == 1:
            check_command = ['node', script_path]
            logger.info(f"Running JS pre-check: {' '.join(check_command)}")
            check_proc = None
            try:
                check_proc = subprocess.Popen(check_command, cwd=user_folder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore')
                stdout, stderr = check_proc.communicate(timeout=5)
                return_code = check_proc.returncode
                logger.info(f"JS Pre-check early. RC: {return_code}. Stderr: {stderr[:200]}...")
                if return_code != 0 and stderr:
                    match_js = re.search(r"Cannot find module '(.+?)'", stderr)
                    if match_js:
                        module_name = match_js.group(1).strip().strip("'\"")
                        if not module_name.startswith('.') and not module_name.startswith('/'):
                             logger.info(f"Detected missing Node module: {module_name}")
                             if attempt_install_npm(module_name, user_folder, message_obj_for_reply):
                                 logger.info(f"NPM Install OK for {module_name}. Retrying run_js_script...")
                                 bot.reply_to(message_obj_for_reply, f"🔄 NPM Install successful. Retrying '{file_name}'...")
                                 time.sleep(2)
                                 threading.Thread(target=run_js_script, args=(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt + 1)).start()
                                 return
                             else:
                                 bot.reply_to(message_obj_for_reply, f"❌ NPM Install failed. Cannot run '{file_name}'.")
                                 return
                        else: logger.info(f"Skipping npm install for relative/core: {module_name}")
                    error_summary = stderr[:500]
                    bot.reply_to(message_obj_for_reply, f"❌ Error in JS script pre-check for '{file_name}':\n```\n{error_summary}\n```\nFix script or install manually.", parse_mode='Markdown')
                    return
            except subprocess.TimeoutExpired:
                logger.info("JS Pre-check timed out (>5s), imports likely OK. Killing check process.")
                if check_proc and check_proc.poll() is None: check_proc.kill(); check_proc.communicate()
                logger.info("JS Check process killed. Proceeding to long run.")
            except FileNotFoundError:
                 error_msg = "❌ Error: 'node' not found. Ensure Node.js is installed for JS files."
                 logger.error(error_msg)
                 bot.reply_to(message_obj_for_reply, error_msg)
                 return
            except Exception as e:
                 logger.error(f"Error in JS pre-check for {script_key}: {e}", exc_info=True)
                 bot.reply_to(message_obj_for_reply, f"❌ Unexpected error in JS pre-check for '{file_name}': {e}")
                 return
            finally:
                 if check_proc and check_proc.poll() is None:
                     logger.warning(f"JS Check process {check_proc.pid} still running. Killing.")
                     check_proc.kill(); check_proc.communicate()

        logger.info(f"Starting long-running JS process for {script_key}")
        log_file_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        log_file = None; process = None
        try: log_file = open(log_file_path, 'w', encoding='utf-8', errors='ignore')
        except Exception as e:
            logger.error(f"Failed to open log file '{log_file_path}' for JS script {script_key}: {e}", exc_info=True)
            bot.reply_to(message_obj_for_reply, f"❌ Failed to open log file '{log_file_path}': {e}")
            return
        try:
            startupinfo = None; creationflags = 0
            if os.name == 'nt':
                 startupinfo = subprocess.STARTUPINFO(); startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                 startupinfo.wShowWindow = subprocess.SW_HIDE
            process = subprocess.Popen(
                ['node', script_path], cwd=user_folder, stdout=log_file, stderr=log_file,
                stdin=subprocess.PIPE, startupinfo=startupinfo, creationflags=creationflags,
                encoding='utf-8', errors='ignore'
            )
            logger.info(f"Started JS process {process.pid} for {script_key}")
            bot_scripts[script_key] = {
                'process': process, 'log_file': log_file, 'file_name': file_name,
                'chat_id': message_obj_for_reply.chat.id,
                'script_owner_id': script_owner_id,
                'start_time': datetime.now(), 'user_folder': user_folder, 'type': 'js', 'script_key': script_key
            }
            bot.reply_to(message_obj_for_reply, f"✅ JS script '{file_name}' started! (PID: {process.pid}) (For User: {script_owner_id})")
        except FileNotFoundError:
             error_msg = "❌ Error: 'node' not found for long run. Ensure Node.js is installed."
             logger.error(error_msg)
             if log_file and not log_file.closed: log_file.close()
             bot.reply_to(message_obj_for_reply, error_msg)
             if script_key in bot_scripts: del bot_scripts[script_key]
        except Exception as e:
            if log_file and not log_file.closed: log_file.close()
            error_msg = f"❌ Error starting JS script '{file_name}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            bot.reply_to(message_obj_for_reply, error_msg)
            if process and process.poll() is None:
                 logger.warning(f"Killing potentially started JS process {process.pid} for {script_key}")
                 kill_process_tree({'process': process, 'log_file': log_file, 'script_key': script_key})
            if script_key in bot_scripts: del bot_scripts[script_key]
    except Exception as e:
        error_msg = f"❌ Unexpected error running JS script '{file_name}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message_obj_for_reply, error_msg)
        if script_key in bot_scripts:
             logger.warning(f"Cleaning up {script_key} due to error in run_js_script.")
             kill_process_tree(bot_scripts[script_key])
             del bot_scripts[script_key]

# --- Database Operations ---
DB_LOCK = threading.Lock()

def save_user_file(user_id, file_name, file_type='py'):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('INSERT OR REPLACE INTO user_files (user_id, file_name, file_type) VALUES (?, ?, ?)',
                      (user_id, file_name, file_type))
            conn.commit()
            if user_id not in user_files:
                user_files[user_id] = []
            user_files[user_id] = [(fn, ft) for fn, ft in user_files[user_id] if fn != file_name]
            user_files[user_id].append((file_name, file_type))
            logger.info(f"Saved file '{file_name}' ({file_type}) for user {user_id}")
        except Exception as e:
            logger.error(f"Error saving file for user {user_id}, {file_name}: {e}")
        finally:
            conn.close()

def remove_user_file_db(user_id, file_name):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('DELETE FROM user_files WHERE user_id = ? AND file_name = ?', (user_id, file_name))
            conn.commit()
            if user_id in user_files:
                user_files[user_id] = [f for f in user_files[user_id] if f[0] != file_name]
                if not user_files[user_id]:
                    del user_files[user_id]
            logger.info(f"Removed file '{file_name}' for user {user_id} from DB")
        except Exception as e:
            logger.error(f"Error removing file for {user_id}, {file_name}: {e}")
        finally:
            conn.close()

def remove_all_user_files_db(user_id):
    """Remove all files for a user from database"""
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('DELETE FROM user_files WHERE user_id = ?', (user_id,))
            conn.commit()
            if user_id in user_files:
                del user_files[user_id]
            logger.info(f"Removed all files for user {user_id} from DB")
        except Exception as e:
            logger.error(f"Error removing all files for {user_id}: {e}")
        finally:
            conn.close()

def add_active_user(user_id):
    active_users.add(user_id)
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('INSERT OR IGNORE INTO active_users (user_id) VALUES (?)', (user_id,))
            conn.commit()
            logger.info(f"Added/Confirmed active user {user_id} in DB")
        except Exception as e:
            logger.error(f"Error adding active user {user_id}: {e}")
        finally:
            conn.close()

def save_subscription(user_id, expiry):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            expiry_str = expiry.isoformat()
            c.execute('INSERT OR REPLACE INTO subscriptions (user_id, expiry) VALUES (?, ?)', (user_id, expiry_str))
            conn.commit()
            user_subscriptions[user_id] = {'expiry': expiry}
            logger.info(f"Saved subscription for {user_id}, expiry {expiry_str}")
        except Exception as e:
            logger.error(f"Error saving subscription for {user_id}: {e}")
        finally:
            conn.close()

def remove_subscription_db(user_id):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('DELETE FROM subscriptions WHERE user_id = ?', (user_id,))
            conn.commit()
            if user_id in user_subscriptions:
                del user_subscriptions[user_id]
            logger.info(f"Removed subscription for {user_id} from DB")
        except Exception as e:
            logger.error(f"Error removing subscription for {user_id}: {e}")
        finally:
            conn.close()

def add_admin_db(admin_id):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (admin_id,))
            conn.commit()
            admin_ids.add(admin_id)
            logger.info(f"Added admin {admin_id} to DB")
        except Exception as e:
            logger.error(f"Error adding admin {admin_id}: {e}")
        finally:
            conn.close()

def remove_admin_db(admin_id):
    if admin_id == OWNER_ID:
        logger.warning("Attempted to remove OWNER_ID from admins.")
        return False
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        removed = False
        try:
            c.execute('SELECT 1 FROM admins WHERE user_id = ?', (admin_id,))
            if c.fetchone():
                c.execute('DELETE FROM admins WHERE user_id = ?', (admin_id,))
                conn.commit()
                removed = c.rowcount > 0
                if removed:
                    admin_ids.discard(admin_id)
                    logger.info(f"Removed admin {admin_id} from DB")
                else:
                    logger.warning(f"Admin {admin_id} found but delete affected 0 rows.")
            else:
                logger.warning(f"Admin {admin_id} not found in DB.")
                admin_ids.discard(admin_id)
            return removed
        except Exception as e:
            logger.error(f"Error removing admin {admin_id}: {e}")
            return False
        finally:
            conn.close()

# --- Menu Creation ---
def create_main_menu_inline(user_id):
    """Create inline keyboard main menu"""
    markup = types.InlineKeyboardMarkup(row_width=2)
    
    # Basic buttons for all users
    buttons = [
        types.InlineKeyboardButton('📢 Updates Channel', url=inline_links['updates_channel']),
        types.InlineKeyboardButton('📤 Upload File', callback_data='upload'),
        types.InlineKeyboardButton('📂 Check Files', callback_data='check_files'),
        types.InlineKeyboardButton('⚡ Bot Speed', callback_data='speed'),
        types.InlineKeyboardButton('💳 Buy Subscription', callback_data='buy_subscription'),
        types.InlineKeyboardButton('📝 Submit Problem', callback_data='submit_problem'),
        types.InlineKeyboardButton('📞 Contact Owner', url=f'https://t.me/{YOUR_USERNAME.replace("@", "")}'),
        types.InlineKeyboardButton('📊 Statistics', callback_data='stats')
    ]
    
    if user_id in admin_ids:
        # Admin specific buttons
        admin_buttons = [
            types.InlineKeyboardButton('👥 User Management', callback_data='user_management'),
            types.InlineKeyboardButton('📝 View Problems', callback_data='view_problems'),
            types.InlineKeyboardButton('💰 Payment Settings', callback_data='payment_settings'),
            types.InlineKeyboardButton('🔗 Edit Links', callback_data='edit_links'),
            types.InlineKeyboardButton('💳 Subscriptions', callback_data='subscription'),
            types.InlineKeyboardButton('📢 Broadcast', callback_data='broadcast'),
            types.InlineKeyboardButton('🔒 Lock Bot' if not bot_locked else '🔓 Unlock Bot',
                                     callback_data='lock_bot' if not bot_locked else 'unlock_bot'),
            types.InlineKeyboardButton('👑 Admin Panel', callback_data='admin_panel'),
            types.InlineKeyboardButton('🟢 Run All Scripts', callback_data='run_all_scripts')
        ]
        
        # Arrange buttons in rows
        markup.row(buttons[0])  # Updates Channel
        markup.row(buttons[1], buttons[2])  # Upload, Check Files
        markup.row(buttons[3], admin_buttons[0])  # Speed, User Management
        markup.row(admin_buttons[1], admin_buttons[2])  # View Problems, Payment Settings
        markup.row(admin_buttons[3], buttons[4])  # Edit Links, Buy Subscription
        markup.row(admin_buttons[4], admin_buttons[5])  # Subscriptions, Broadcast
        markup.row(admin_buttons[6], admin_buttons[8])  # Lock Bot, Run All Scripts
        markup.row(admin_buttons[7])  # Admin Panel
        markup.row(buttons[6])  # Contact Owner
    else:
        # Regular user buttons
        markup.row(buttons[0])  # Updates Channel
        markup.row(buttons[1], buttons[2])  # Upload, Check Files
        markup.row(buttons[3], buttons[7])  # Speed, Statistics
        markup.row(buttons[4], buttons[5])  # Buy Subscription, Submit Problem
        markup.row(buttons[6])  # Contact Owner
    
    return markup

def create_reply_keyboard_main_menu(user_id):
    """Create reply keyboard main menu"""
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    layout_to_use = ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC if user_id in admin_ids else COMMAND_BUTTONS_LAYOUT_USER_SPEC
    for row_buttons_text in layout_to_use:
        markup.add(*[types.KeyboardButton(text) for text in row_buttons_text])
    return markup

def create_user_management_menu():
    """Create user management menu for admins"""
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton('👤 View User Files', callback_data='view_user_files'),
        types.InlineKeyboardButton('🚫 Ban User', callback_data='ban_user')
    )
    markup.row(
        types.InlineKeyboardButton('✅ Unban User', callback_data='unban_user'),
        types.InlineKeyboardButton('📋 List Banned Users', callback_data='list_banned_users')
    )
    markup.row(
        types.InlineKeyboardButton('🔙 Back to Admin Panel', callback_data='admin_panel')
    )
    return markup

def create_payment_menu():
    """Create payment menu for users"""
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton('30 Days - ₹299', callback_data='payment_30'),
        types.InlineKeyboardButton('90 Days - ₹799', callback_data='payment_90')
    )
    markup.row(
        types.InlineKeyboardButton('180 Days - ₹1499', callback_data='payment_180'),
        types.InlineKeyboardButton('🔄 Generate QR', callback_data='generate_qr')
    )
    markup.row(
        types.InlineKeyboardButton('🔙 Back to Main', callback_data='back_to_main')
    )
    return markup

def create_payment_settings_menu():
    """Create payment settings menu for admins"""
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton('✏️ Edit UPI ID', callback_data='edit_upi_id'),
        types.InlineKeyboardButton('💰 Edit Prices', callback_data='edit_prices')
    )
    markup.row(
        types.InlineKeyboardButton('🔄 Generate QR', callback_data='admin_generate_qr'),
        types.InlineKeyboardButton('📋 View Config', callback_data='view_payment_config')
    )
    markup.row(
        types.InlineKeyboardButton('🔙 Back to Admin', callback_data='admin_panel')
    )
    return markup

def create_edit_links_menu():
    """Create menu for editing inline button links"""
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton('📢 Updates Channel', callback_data='edit_link_updates'),
        types.InlineKeyboardButton('👥 Support Group', callback_data='edit_link_support')
    )
    markup.row(
        types.InlineKeyboardButton('📚 Tutorial Channel', callback_data='edit_link_tutorial'),
        types.InlineKeyboardButton('💻 GitHub Repo', callback_data='edit_link_github')
    )
    markup.row(
        types.InlineKeyboardButton('❤️ Donation Link', callback_data='edit_link_donation'),
        types.InlineKeyboardButton('📋 View Links', callback_data='view_links')
    )
    markup.row(
        types.InlineKeyboardButton('🔙 Back to Admin', callback_data='admin_panel')
    )
    return markup

def create_problems_menu(problem_id=None):
    """Create menu for problem management"""
    markup = types.InlineKeyboardMarkup(row_width=2)
    if problem_id:
        markup.row(
            types.InlineKeyboardButton('✅ Mark Resolved', callback_data=f'resolve_{problem_id}'),
            types.InlineKeyboardButton('🗑️ Delete', callback_data=f'delete_problem_{problem_id}')
        )
    markup.row(
        types.InlineKeyboardButton('📋 All Problems', callback_data='view_problems'),
        types.InlineKeyboardButton('🔙 Back to Admin', callback_data='admin_panel')
    )
    return markup

def create_control_buttons(script_owner_id, file_name, is_running=True):
    """Create control buttons for file management"""
    markup = types.InlineKeyboardMarkup(row_width=2)
    if is_running:
        markup.row(
            types.InlineKeyboardButton("🔴 Stop", callback_data=f'stop_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("🔄 Restart", callback_data=f'restart_{script_owner_id}_{file_name}')
        )
        markup.row(
            types.InlineKeyboardButton("🗑️ Delete", callback_data=f'delete_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("📜 Logs", callback_data=f'logs_{script_owner_id}_{file_name}')
        )
    else:
        markup.row(
            types.InlineKeyboardButton("🟢 Start", callback_data=f'start_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("🗑️ Delete", callback_data=f'delete_{script_owner_id}_{file_name}')
        )
        markup.row(
            types.InlineKeyboardButton("📜 View Logs", callback_data=f'logs_{script_owner_id}_{file_name}')
        )
    markup.add(types.InlineKeyboardButton("🔙 Back to Files", callback_data='check_files'))
    return markup

def create_admin_panel():
    """Create admin panel menu"""
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton('➕ Add Admin', callback_data='add_admin'),
        types.InlineKeyboardButton('➖ Remove Admin', callback_data='remove_admin')
    )
    markup.row(
        types.InlineKeyboardButton('📋 List Admins', callback_data='list_admins')
    )
    markup.row(
        types.InlineKeyboardButton('🔙 Back to Main', callback_data='back_to_main')
    )
    return markup

def create_subscription_menu():
    """Create subscription management menu"""
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton('➕ Add Subscription', callback_data='add_subscription'),
        types.InlineKeyboardButton('➖ Remove Subscription', callback_data='remove_subscription')
    )
    markup.row(
        types.InlineKeyboardButton('🔍 Check Subscription', callback_data='check_subscription')
    )
    markup.row(
        types.InlineKeyboardButton('🔙 Back to Main', callback_data='back_to_main')
    )
    return markup

# --- Logic Functions ---
def _logic_send_welcome(message):
    """Send welcome message"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    user_name = message.from_user.first_name
    user_username = message.from_user.username

    logger.info(f"Welcome request from user_id: {user_id}, username: @{user_username}")

    if bot_locked and user_id not in admin_ids:
        bot.send_message(chat_id, "⚠️ Bot locked by admin. Try later.")
        return

    if user_id not in active_users:
        add_active_user(user_id)
        try:
            owner_notification = (f"🎉 New user!\n👤 Name: {user_name}\n✳️ User: @{user_username or 'N/A'}\n"
                                  f"🆔 ID: `{user_id}`")
            bot.send_message(OWNER_ID, owner_notification, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Failed to notify owner about new user {user_id}: {e}")

    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
    expiry_info = ""
    if user_id == OWNER_ID:
        user_status = "👑 Owner"
    elif user_id in admin_ids:
        user_status = "🛡️ Admin"
    elif user_id in user_subscriptions:
        expiry_date = user_subscriptions[user_id].get('expiry')
        if expiry_date and expiry_date > datetime.now():
            user_status = "⭐ Premium"
            days_left = (expiry_date - datetime.now()).days
            expiry_info = f"\n⏳ Subscription expires in: {days_left} days"
        else:
            user_status = "🆓 Free User (Expired Sub)"
            remove_subscription_db(user_id)
    else:
        user_status = "🆓 Free User"

    welcome_msg_text = (f"〽️ Welcome, {user_name}!\n\n🆔 Your User ID: `{user_id}`\n"
                        f"✳️ Username: `@{user_username or 'Not set'}`\n"
                        f"🔰 Your Status: {user_status}{expiry_info}\n"
                        f"📁 Files Uploaded: {current_files} / {limit_str}\n\n"
                        f"🤖 Host & run Python (`.py`) or JS (`.js`) scripts.\n"
                        f"   Upload single scripts or `.zip` archives.\n\n"
                        f"👇 Use buttons or type commands.")
    
    main_reply_markup = create_reply_keyboard_main_menu(user_id)
    try:
        bot.send_message(chat_id, welcome_msg_text, reply_markup=main_reply_markup, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error sending welcome to {user_id}: {e}")

def _logic_updates_channel(message):
    """Updates channel logic"""
    bot.send_message(
        message.chat.id,
        "📢 Click below to join our Updates Channel 👇",
        reply_markup=types.InlineKeyboardMarkup().add(
            types.InlineKeyboardButton("📢 Join Now", url=inline_links['updates_channel'])
        )
    )

def _logic_upload_file(message):
    """Upload file logic"""
    user_id = message.from_user.id
    if is_user_banned(user_id):
        bot.reply_to(message, "🚫 You are banned from uploading files.")
        return
    
    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "⚠️ Bot locked by admin, cannot accept files.")
        return

    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
        bot.reply_to(message, f"⚠️ File limit ({current_files}/{limit_str}) reached. Delete files first.")
        return
    bot.reply_to(message, "📤 Send your Python (`.py`), JS (`.js`), or ZIP (`.zip`) file.")

def _logic_check_files(message):
    """Check files logic"""
    user_id = message.from_user.id
    user_files_list = user_files.get(user_id, [])
    if not user_files_list:
        bot.reply_to(message, "📂 Your files:\n\n(No files uploaded yet)")
        return
    markup = types.InlineKeyboardMarkup(row_width=1)
    for file_name, file_type in sorted(user_files_list):
        is_running = is_bot_running(user_id, file_name)
        status_icon = "🟢 Running" if is_running else "🔴 Stopped"
        btn_text = f"{file_name} ({file_type}) - {status_icon}"
        markup.add(types.InlineKeyboardButton(btn_text, callback_data=f'file_{user_id}_{file_name}'))
    bot.reply_to(message, "📂 Your files:\nClick to manage.", reply_markup=markup, parse_mode='Markdown')

def _logic_bot_speed(message):
    """Bot speed logic"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    start_time_ping = time.time()
    wait_msg = bot.reply_to(message, "🏃 Testing speed...")
    try:
        bot.send_chat_action(chat_id, 'typing')
        response_time = round((time.time() - start_time_ping) * 1000, 2)
        status = "🔓 Unlocked" if not bot_locked else "🔒 Locked"
        if user_id == OWNER_ID:
            user_level = "👑 Owner"
        elif user_id in admin_ids:
            user_level = "🛡️ Admin"
        elif user_id in user_subscriptions and user_subscriptions[user_id].get('expiry', datetime.min) > datetime.now():
            user_level = "⭐ Premium"
        else:
            user_level = "🆓 Free User"
        speed_msg = (f"⚡ Bot Speed & Status:\n\n⏱️ API Response Time: {response_time} ms\n"
                     f"🚦 Bot Status: {status}\n"
                     f"👤 Your Level: {user_level}")
        bot.edit_message_text(speed_msg, chat_id, wait_msg.message_id)
    except Exception as e:
        logger.error(f"Error during speed test: {e}")
        bot.edit_message_text("❌ Error during speed test.", chat_id, wait_msg.message_id)

def _logic_contact_owner(message):
    """Contact owner logic"""
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton('📞 Contact Owner', url=f'https://t.me/{YOUR_USERNAME.replace("@", "")}'))
    bot.reply_to(message, "Click to contact Owner:", reply_markup=markup)

def _logic_statistics(message):
    """Statistics logic"""
    user_id = message.from_user.id
    total_users = len(active_users)
    total_files_records = sum(len(files) for files in user_files.values())

    running_bots_count = 0
    user_running_bots = 0

    for script_key_iter, script_info_iter in list(bot_scripts.items()):
        s_owner_id, _ = script_key_iter.split('_', 1)
        if is_bot_running(int(s_owner_id), script_info_iter['file_name']):
            running_bots_count += 1
            if int(s_owner_id) == user_id:
                user_running_bots += 1

    stats_msg_base = (f"📊 Bot Statistics:\n\n"
                      f"👥 Total Users: {total_users}\n"
                      f"📂 Total File Records: {total_files_records}\n"
                      f"🟢 Total Active Bots: {running_bots_count}\n")

    if user_id in admin_ids:
        stats_msg_admin = (f"🔒 Bot Status: {'🔴 Locked' if bot_locked else '🟢 Unlocked'}\n"
                           f"🤖 Your Running Bots: {user_running_bots}")
        stats_msg = stats_msg_base + stats_msg_admin
    else:
        stats_msg = stats_msg_base + f"🤖 Your Running Bots: {user_running_bots}"

    bot.reply_to(message, stats_msg)

def _logic_subscriptions_panel(message):
    """Subscriptions panel logic"""
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    bot.reply_to(message, "💳 Subscription Management\nUse inline buttons from /start or admin command menu.", reply_markup=create_subscription_menu())

def _logic_broadcast_init(message):
    """Broadcast init logic"""
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    msg = bot.reply_to(message, "📢 Send message to broadcast to all active users.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_broadcast_message)

def _logic_toggle_lock_bot(message):
    """Toggle lock bot logic"""
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    global bot_locked
    bot_locked = not bot_locked
    status = "locked" if bot_locked else "unlocked"
    logger.warning(f"Bot {status} by Admin {message.from_user.id} via command/button.")
    bot.reply_to(message, f"🔒 Bot has been {status}.")

def _logic_admin_panel(message):
    """Admin panel logic"""
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    bot.reply_to(message, "👑 Admin Panel\nManage admins. Use inline buttons from /start or admin menu.",
                 reply_markup=create_admin_panel())

def _logic_run_all_scripts(message_or_call):
    """Run all scripts logic"""
    if isinstance(message_or_call, telebot.types.Message):
        admin_user_id = message_or_call.from_user.id
        admin_chat_id = message_or_call.chat.id
        reply_func = lambda text, **kwargs: bot.reply_to(message_or_call, text, **kwargs)
        admin_message_obj_for_script_runner = message_or_call
    elif isinstance(message_or_call, telebot.types.CallbackQuery):
        admin_user_id = message_or_call.from_user.id
        admin_chat_id = message_or_call.message.chat.id
        bot.answer_callback_query(message_or_call.id)
        reply_func = lambda text, **kwargs: bot.send_message(admin_chat_id, text, **kwargs)
        admin_message_obj_for_script_runner = message_or_call.message
    else:
        logger.error("Invalid argument for _logic_run_all_scripts")
        return

    if admin_user_id not in admin_ids:
        reply_func("⚠️ Admin permissions required.")
        return

    reply_func("⏳ Starting process to run all user scripts. This may take a while...")
    logger.info(f"Admin {admin_user_id} initiated 'run all scripts' from chat {admin_chat_id}.")

    started_count = 0; attempted_users = 0; skipped_files = 0; error_files_details = []

    # Use a copy of user_files keys and values to avoid modification issues during iteration
    all_user_files_snapshot = dict(user_files)

    for target_user_id, files_for_user in all_user_files_snapshot.items():
        if not files_for_user: continue
        attempted_users += 1
        logger.info(f"Processing scripts for user {target_user_id}...")
        user_folder = get_user_folder(target_user_id)

        for file_name, file_type in files_for_user:
            if not is_bot_running(target_user_id, file_name):
                file_path = os.path.join(user_folder, file_name)
                if os.path.exists(file_path):
                    logger.info(f"Admin {admin_user_id} attempting to start '{file_name}' ({file_type}) for user {target_user_id}.")
                    try:
                        if file_type == 'py':
                            threading.Thread(target=run_script, args=(file_path, target_user_id, user_folder, file_name, admin_message_obj_for_script_runner)).start()
                            started_count += 1
                        elif file_type == 'js':
                            threading.Thread(target=run_js_script, args=(file_path, target_user_id, user_folder, file_name, admin_message_obj_for_script_runner)).start()
                            started_count += 1
                        else:
                            logger.warning(f"Unknown file type '{file_type}' for {file_name} (user {target_user_id}). Skipping.")
                            error_files_details.append(f"`{file_name}` (User {target_user_id}) - Unknown type")
                            skipped_files += 1
                        time.sleep(0.7)
                    except Exception as e:
                        logger.error(f"Error queueing start for '{file_name}' (user {target_user_id}): {e}")
                        error_files_details.append(f"`{file_name}` (User {target_user_id}) - Start error")
                        skipped_files += 1
                else:
                    logger.warning(f"File '{file_name}' for user {target_user_id} not found at '{file_path}'. Skipping.")
                    error_files_details.append(f"`{file_name}` (User {target_user_id}) - File not found")
                    skipped_files += 1

    summary_msg = (f"✅ All Users' Scripts - Processing Complete:\n\n"
                   f"▶️ Attempted to start: {started_count} scripts.\n"
                   f"👥 Users processed: {attempted_users}.\n")
    if skipped_files > 0:
        summary_msg += f"⚠️ Skipped/Error files: {skipped_files}\n"
        if error_files_details:
             summary_msg += "Details (first 5):\n" + "\n".join([f"  - {err}" for err in error_files_details[:5]])
             if len(error_files_details) > 5: summary_msg += "\n  ... and more (check logs)."

    reply_func(summary_msg, parse_mode='Markdown')
    logger.info(f"Run all scripts finished. Admin: {admin_user_id}. Started: {started_count}. Skipped/Errors: {skipped_files}")

# --- Button Text to Logic Mapping ---
BUTTON_TEXT_TO_LOGIC = {
    "📢 Updates Channel": _logic_updates_channel,
    "📤 Upload File": _logic_upload_file,
    "📂 Check Files": _logic_check_files,
    "⚡ Bot Speed": _logic_bot_speed,
    "📞 Contact Owner": _logic_contact_owner,
    "📊 Statistics": _logic_statistics,
    "💳 Subscriptions": _logic_subscriptions_panel,
    "📢 Broadcast": _logic_broadcast_init,
    "🔒 Lock Bot": _logic_toggle_lock_bot,
    "🟢 Running All Code": _logic_run_all_scripts,
    "👑 Admin Panel": _logic_admin_panel,
    # New features - these will be handled separately
    "💳 Buy Subscription": lambda msg: buy_subscription_button(msg),
    "📝 Submit Problem": lambda msg: submit_problem_button(msg),
    "👥 User Management": lambda msg: user_management_button(msg),
    "💰 Payment Settings": lambda msg: payment_settings_button(msg),
    "🔗 Edit Links": lambda msg: edit_links_button(msg),
    "📝 View Problems": lambda msg: view_problems_button(msg),
}

# Button wrapper functions
def buy_subscription_button(message):
    """Wrapper for buy subscription button"""
    if is_user_banned(message.from_user.id):
        bot.reply_to(message, "🚫 You are banned from using this bot.")
        return
    upi_id = payment_config.get('upi_id', 'yourupi@bank')
    response = (f"💳 Buy Subscription\n\n"
                f"📱 UPI ID: `{upi_id}`\n\n"
                f"Select plan:\n"
                f"• 30 Days - ₹{payment_config.get('subscription_price_30_days', 299)}\n"
                f"• 90 Days - ₹{payment_config.get('subscription_price_90_days', 799)}\n"
                f"• 180 Days - ₹{payment_config.get('subscription_price_180_days', 1499)}\n\n"
                f"After payment, send screenshot to @{YOUR_USERNAME.replace('@', '')}")
    bot.send_message(message.chat.id, response, reply_markup=create_payment_menu(), parse_mode='Markdown')

def submit_problem_button(message):
    """Wrapper for submit problem button"""
    if is_user_banned(message.from_user.id):
        bot.reply_to(message, "🚫 You are banned from using this bot.")
        return
    msg = bot.send_message(message.chat.id, 
                          "📝 Please describe your problem or issue.\n\n"
                          "Type /cancel to abort.")
    bot.register_next_step_handler(msg, process_problem_submission)

def user_management_button(message):
    """Wrapper for user management button"""
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    bot.send_message(message.chat.id, "👥 User Management Panel\nSelect an action:", reply_markup=create_user_management_menu())

def payment_settings_button(message):
    """Wrapper for payment settings button"""
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    bot.send_message(message.chat.id, "💰 Payment Settings\nConfigure payment options:", reply_markup=create_payment_settings_menu())

def edit_links_button(message):
    """Wrapper for edit links button"""
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    bot.send_message(message.chat.id, "🔗 Edit Inline Button Links\nSelect link to edit:", reply_markup=create_edit_links_menu())

def view_problems_button(message):
    """Wrapper for view problems button"""
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    problems = get_pending_problems()
    if not problems:
        bot.send_message(message.chat.id, "📝 No pending problems found.")
        return
    for problem in problems[:5]:
        problem_id, user_id, user_name, problem_text, created_at = problem
        created_date = datetime.fromisoformat(created_at).strftime("%Y-%m-%d %H:%M")
        markup = create_problems_menu(problem_id)
        bot.send_message(message.chat.id,
                        f"📝 Problem #{problem_id}\n"
                        f"👤 User: {user_name} (`{user_id}`)\n"
                        f"🕐 Date: {created_date}\n"
                        f"📄 Issue:\n{problem_text[:300]}...",
                        reply_markup=markup,
                        parse_mode='Markdown')

# --- Command Handlers ---
@bot.message_handler(commands=['start', 'help'])
def command_send_welcome(message):
    """Welcome handler with ban check"""
    user_id = message.from_user.id
    
    if is_user_banned(user_id):
        reason = get_ban_reason(user_id)
        bot.send_message(message.chat.id,
                        f"🚫 You are banned from using this bot.\n"
                        f"Reason: {reason}\n\n"
                        f"Contact admin if you think this is a mistake.")
        return
    
    _logic_send_welcome(message)

@bot.message_handler(func=lambda message: message.text in BUTTON_TEXT_TO_LOGIC)
def handle_button_text(message):
    """Button text handler"""
    user_id = message.from_user.id
    
    if is_user_banned(user_id) and message.text not in ["📞 Contact Owner", "📝 Submit Problem"]:
        bot.reply_to(message, "🚫 You are banned from using this bot.")
        return
    
    logic_func = BUTTON_TEXT_TO_LOGIC.get(message.text)
    if logic_func:
        logic_func(message)
    else:
        logger.warning(f"Button text '{message.text}' matched but no logic func.")

# --- Command Handlers for specific commands ---
@bot.message_handler(commands=['updateschannel'])
def command_updates_channel(message): _logic_updates_channel(message)
@bot.message_handler(commands=['uploadfile'])
def command_upload_file(message): _logic_upload_file(message)
@bot.message_handler(commands=['checkfiles'])
def command_check_files(message): _logic_check_files(message)
@bot.message_handler(commands=['botspeed'])
def command_bot_speed(message): _logic_bot_speed(message)
@bot.message_handler(commands=['contactowner'])
def command_contact_owner(message): _logic_contact_owner(message)
@bot.message_handler(commands=['subscriptions'])
def command_subscriptions(message): _logic_subscriptions_panel(message)
@bot.message_handler(commands=['statistics'])
def command_statistics(message): _logic_statistics(message)
@bot.message_handler(commands=['broadcast'])
def command_broadcast(message): _logic_broadcast_init(message)
@bot.message_handler(commands=['lockbot'])
def command_lock_bot(message): _logic_toggle_lock_bot(message)
@bot.message_handler(commands=['adminpanel'])
def command_admin_panel(message): _logic_admin_panel(message)
@bot.message_handler(commands=['runningallcode'])
def command_run_all_code(message): _logic_run_all_scripts(message)

@bot.message_handler(commands=['ping'])
def ping(message):
    start_ping_time = time.time()
    msg = bot.reply_to(message, "Pong!")
    latency = round((time.time() - start_ping_time) * 1000, 2)
    bot.edit_message_text(f"Pong! Latency: {latency} ms", message.chat.id, msg.message_id)

# --- Document Handler ---
@bot.message_handler(content_types=['document'])
def handle_file_upload_doc(message):
    """Handle file uploads"""
    user_id = message.from_user.id
    
    if is_user_banned(user_id):
        bot.reply_to(message, "🚫 You are banned from uploading files.")
        return
    
    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "⚠️ Bot locked by admin, cannot accept files.")
        return

    # File limit check
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
        bot.reply_to(message, f"⚠️ File limit ({current_files}/{limit_str}) reached.")
        return

    doc = message.document
    file_name = doc.file_name
    if not file_name:
        bot.reply_to(message, "⚠️ No file name. Ensure file has a name.")
        return
    
    file_ext = os.path.splitext(file_name)[1].lower()
    if file_ext not in ['.py', '.js', '.zip']:
        bot.reply_to(message, "⚠️ Unsupported type! Only `.py`, `.js`, `.zip` allowed.")
        return
    
    max_file_size = 20 * 1024 * 1024
    if doc.file_size > max_file_size:
        bot.reply_to(message, f"⚠️ File too large (Max: {max_file_size // 1024 // 1024} MB).")
        return

    try:
        # Download file
        download_wait_msg = bot.reply_to(message, f"⏳ Downloading `{file_name}`...")
        file_info = bot.get_file(doc.file_id)
        downloaded_file_content = bot.download_file(file_info.file_path)
        bot.edit_message_text(f"✅ Downloaded `{file_name}`. Processing...", 
                             message.chat.id, download_wait_msg.message_id)
        
        user_folder = get_user_folder(user_id)
        
        if file_ext == '.zip':
            handle_zip_file(downloaded_file_content, file_name, message, user_id, user_folder)
        else:
            file_path = os.path.join(user_folder, file_name)
            with open(file_path, 'wb') as f:
                f.write(downloaded_file_content)
            
            if file_ext == '.js':
                save_user_file(user_id, file_name, 'js')
                bot.reply_to(message, f"✅ JS file `{file_name}` saved successfully!")
            elif file_ext == '.py':
                save_user_file(user_id, file_name, 'py')
                bot.reply_to(message, f"✅ Python file `{file_name}` saved successfully!")
            
    except Exception as e:
        logger.error(f"Error handling file for {user_id}: {e}")
        bot.reply_to(message, f"❌ Error processing file: {str(e)}")

def handle_zip_file(downloaded_file_content, file_name, message, user_id, user_folder):
    """Handle ZIP file extraction"""
    temp_dir = None
    try:
        temp_dir = tempfile.mkdtemp(prefix=f"user_{user_id}_zip_")
        zip_path = os.path.join(temp_dir, file_name)
        
        with open(zip_path, 'wb') as new_file:
            new_file.write(downloaded_file_content)
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        
        # Find main script
        extracted_items = os.listdir(temp_dir)
        py_files = [f for f in extracted_items if f.endswith('.py')]
        js_files = [f for f in extracted_items if f.endswith('.js')]
        
        main_script_name = None
        file_type = None
        preferred_py = ['main.py', 'bot.py', 'app.py']
        preferred_js = ['index.js', 'main.js', 'bot.js', 'app.js']
        
        for p in preferred_py:
            if p in py_files:
                main_script_name = p
                file_type = 'py'
                break
        
        if not main_script_name:
            for p in preferred_js:
                if p in js_files:
                    main_script_name = p
                    file_type = 'js'
                    break
        
        if not main_script_name:
            if py_files:
                main_script_name = py_files[0]
                file_type = 'py'
            elif js_files:
                main_script_name = js_files[0]
                file_type = 'js'
        
        if not main_script_name:
            bot.reply_to(message, "❌ No `.py` or `.js` script found in archive!")
            return
        
        # Move files to user folder
        for item_name in os.listdir(temp_dir):
            src_path = os.path.join(temp_dir, item_name)
            dest_path = os.path.join(user_folder, item_name)
            if os.path.isdir(dest_path):
                shutil.rmtree(dest_path)
            elif os.path.exists(dest_path):
                os.remove(dest_path)
            shutil.move(src_path, dest_path)
        
        save_user_file(user_id, main_script_name, file_type)
        bot.reply_to(message, f"✅ Files extracted. Main script: `{main_script_name}`", parse_mode='Markdown')
        
    except zipfile.BadZipFile:
        bot.reply_to(message, "❌ Invalid ZIP file.")
    except Exception as e:
        logger.error(f"Error processing zip: {e}")
        bot.reply_to(message, f"❌ Error processing ZIP: {str(e)}")
    finally:
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except:
                pass

# --- Callback Query Handlers for NEW FEATURES ---
@bot.callback_query_handler(func=lambda call: call.data == 'submit_problem')
def submit_problem_callback(call):
    """Handle problem submission initiation"""
    if is_user_banned(call.from_user.id):
        bot.answer_callback_query(call.id, "❌ You are banned from using this bot.", show_alert=True)
        return
    
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, 
                          "📝 Please describe your problem or issue.\n\n"
                          "Type /cancel to abort.")
    bot.register_next_step_handler(msg, process_problem_submission)

def process_problem_submission(message):
    """Process problem submission"""
    if message.text and message.text.lower() == '/cancel':
        bot.reply_to(message, "Problem submission cancelled.")
        return
    
    user_id = message.from_user.id
    user_name = message.from_user.first_name
    problem_text = message.text
    
    problem_id = save_problem(user_id, user_name, problem_text)
    
    if problem_id:
        bot.reply_to(message, f"✅ Problem submitted successfully! (ID: #{problem_id})")
        
        # Notify admins
        for admin_id in admin_ids:
            try:
                bot.send_message(admin_id, 
                                f"📝 New Problem Submitted!\n\n"
                                f"ID: #{problem_id}\n"
                                f"User: {user_name} ({user_id})\n"
                                f"Preview: {problem_text[:200]}...")
            except Exception as e:
                logger.error(f"Failed to notify admin {admin_id}: {e}")
    else:
        bot.reply_to(message, "❌ Failed to submit problem.")

@bot.callback_query_handler(func=lambda call: call.data == 'view_problems')
def view_problems_callback(call):
    """View all pending problems"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    problems = get_pending_problems()
    
    if not problems:
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, "📝 No pending problems found.")
        return
    
    bot.answer_callback_query(call.id)
    
    for problem in problems[:5]:
        problem_id, user_id, user_name, problem_text, created_at = problem
        created_date = datetime.fromisoformat(created_at).strftime("%Y-%m-%d %H:%M")
        
        markup = create_problems_menu(problem_id)
        
        bot.send_message(call.message.chat.id,
                        f"📝 Problem #{problem_id}\n"
                        f"👤 User: {user_name} (`{user_id}`)\n"
                        f"🕐 Date: {created_date}\n"
                        f"📄 Issue:\n{problem_text[:300]}...",
                        reply_markup=markup,
                        parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data.startswith('resolve_'))
def resolve_problem_callback(call):
    """Mark problem as resolved"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    try:
        problem_id = int(call.data.split('_')[1])
        if update_problem_status(problem_id, 'resolved', call.from_user.id):
            bot.answer_callback_query(call.id, "✅ Problem marked as resolved.")
            bot.edit_message_text(f"✅ Problem #{problem_id} marked as resolved.",
                                call.message.chat.id,
                                call.message.message_id)
        else:
            bot.answer_callback_query(call.id, "❌ Failed to update problem status.")
    except Exception as e:
        logger.error(f"Error resolving problem: {e}")
        bot.answer_callback_query(call.id, "❌ Error processing request.")

@bot.callback_query_handler(func=lambda call: call.data == 'user_management')
def user_management_callback(call):
    """Show user management menu"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    bot.answer_callback_query(call.id)
    bot.edit_message_text("👥 User Management Panel\nSelect an action:",
                         call.message.chat.id,
                         call.message.message_id,
                         reply_markup=create_user_management_menu())

@bot.callback_query_handler(func=lambda call: call.data == 'view_user_files')
def view_user_files_callback(call):
    """View files of a specific user"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, 
                          "👤 Enter User ID to view their files:\n"
                          "/cancel to abort.")
    bot.register_next_step_handler(msg, process_view_user_files)

def process_view_user_files(message):
    """Process user files view request"""
    if message.text and message.text.lower() == '/cancel':
        bot.reply_to(message, "Cancelled.")
        return
    
    try:
        target_user_id = int(message.text.strip())
        user_files_list = user_files.get(target_user_id, [])
        
        if not user_files_list:
            bot.reply_to(message, f"📂 User `{target_user_id}` has no files.")
            return
        
        # Count running bots
        running_count = 0
        for file_name, _ in user_files_list:
            if is_bot_running(target_user_id, file_name):
                running_count += 1
        
        # Create message
        response = (f"👤 User ID: `{target_user_id}`\n"
                   f"📁 Total Files: {len(user_files_list)}\n"
                   f"🟢 Running Bots: {running_count}\n\n"
                   f"📂 Files:\n")
        
        for i, (file_name, file_type) in enumerate(user_files_list[:20], 1):
            status = "🟢 Running" if is_bot_running(target_user_id, file_name) else "🔴 Stopped"
            response += f"{i}. `{file_name}` ({file_type}) - {status}\n"
        
        if len(user_files_list) > 20:
            response += f"\n... and {len(user_files_list) - 20} more files."
        
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🗑️ Delete All Files", 
                                            callback_data=f"delete_all_{target_user_id}"))
        markup.add(types.InlineKeyboardButton("🔙 Back to User Management", 
                                            callback_data="user_management"))
        
        bot.reply_to(message, response, reply_markup=markup, parse_mode='Markdown')
        
    except ValueError:
        bot.reply_to(message, "❌ Invalid User ID.")
    except Exception as e:
        logger.error(f"Error viewing user files: {e}")
        bot.reply_to(message, "❌ Error viewing user files.")

@bot.callback_query_handler(func=lambda call: call.data.startswith('delete_all_'))
def delete_all_files_callback(call):
    """Delete all files of a user"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    try:
        target_user_id = int(call.data.split('_')[2])
        user_files_list = user_files.get(target_user_id, [])
        
        if not user_files_list:
            bot.answer_callback_query(call.id, "User has no files.", show_alert=True)
            return
        
        # Confirm deletion
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton("✅ Confirm Delete", 
                                     callback_data=f"confirm_delete_all_{target_user_id}"),
            types.InlineKeyboardButton("❌ Cancel", 
                                     callback_data=f"cancel_delete_{target_user_id}")
        )
        
        bot.answer_callback_query(call.id)
        bot.edit_message_text(f"⚠️ Delete ALL files for user `{target_user_id}`?\n"
                             f"This will delete {len(user_files_list)} files.",
                             call.message.chat.id,
                             call.message.message_id,
                             reply_markup=markup,
                             parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error in delete_all_files_callback: {e}")
        bot.answer_callback_query(call.id, "❌ Error processing request.")

@bot.callback_query_handler(func=lambda call: call.data.startswith('confirm_delete_all_'))
def confirm_delete_all_callback(call):
    """Confirm deletion of all user files"""
    try:
        target_user_id = int(call.data.split('_')[3])
        
        # Stop all running bots
        deleted_count = 0
        for file_name, _ in user_files.get(target_user_id, []):
            script_key = f"{target_user_id}_{file_name}"
            if script_key in bot_scripts:
                kill_process_tree(bot_scripts[script_key])
                del bot_scripts[script_key]
            
            # Remove file from disk
            user_folder = get_user_folder(target_user_id)
            file_path = os.path.join(user_folder, file_name)
            log_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
            
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                if os.path.exists(log_path):
                    os.remove(log_path)
                deleted_count += 1
            except:
                pass
        
        # Remove from database
        remove_all_user_files_db(target_user_id)
        
        bot.answer_callback_query(call.id, f"✅ Deleted {deleted_count} files.")
        bot.edit_message_text(f"✅ Deleted all files for user `{target_user_id}`\n"
                             f"Removed {deleted_count} files.",
                             call.message.chat.id,
                             call.message.message_id)
        
    except Exception as e:
        logger.error(f"Error confirming delete all: {e}")
        bot.answer_callback_query(call.id, "❌ Error deleting files.")

@bot.callback_query_handler(func=lambda call: call.data == 'ban_user')
def ban_user_callback(call):
    """Initiate user ban process"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id,
                          "🚫 Enter User ID to ban:\n"
                          "Format: `ID reason`\n"
                          "Example: `12345678 Spamming`\n\n"
                          "/cancel to abort.",
                          parse_mode='Markdown')
    bot.register_next_step_handler(msg, process_ban_user)

def process_ban_user(message):
    """Process user ban request"""
    if message.text and message.text.lower() == '/cancel':
        bot.reply_to(message, "Ban cancelled.")
        return
    
    try:
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            bot.reply_to(message, "❌ Format: `ID reason`")
            return
        
        user_id = int(parts[0].strip())
        reason = parts[1].strip()
        
        if user_id in admin_ids:
            bot.reply_to(message, "❌ Cannot ban admins.")
            return
        
        if ban_user(user_id, reason, message.from_user.id):
            bot.reply_to(message, f"✅ User `{user_id}` banned.\nReason: {reason}")
        else:
            bot.reply_to(message, "❌ Failed to ban user.")
            
    except ValueError:
        bot.reply_to(message, "❌ Invalid User ID.")
    except Exception as e:
        logger.error(f"Error banning user: {e}")
        bot.reply_to(message, "❌ Error banning user.")

@bot.callback_query_handler(func=lambda call: call.data == 'unban_user')
def unban_user_callback(call):
    """Initiate user unban process"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id,
                          "✅ Enter User ID to unban:\n"
                          "/cancel to abort.")
    bot.register_next_step_handler(msg, process_unban_user)

def process_unban_user(message):
    """Process user unban request"""
    if message.text and message.text.lower() == '/cancel':
        bot.reply_to(message, "Unban cancelled.")
        return
    
    try:
        user_id = int(message.text.strip())
        
        if unban_user(user_id):
            bot.reply_to(message, f"✅ User `{user_id}` unbanned.")
        else:
            bot.reply_to(message, f"❌ User `{user_id}` is not banned.")
            
    except ValueError:
        bot.reply_to(message, "❌ Invalid User ID.")
    except Exception as e:
        logger.error(f"Error unbanning user: {e}")
        bot.reply_to(message, "❌ Error unbanning user.")

@bot.callback_query_handler(func=lambda call: call.data == 'list_banned_users')
def list_banned_users_callback(call):
    """List all banned users"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    if not banned_users:
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, "📋 No banned users found.")
        return
    
    response = "🚫 Banned Users:\n\n"
    for user_id in list(banned_users)[:50]:
        reason = get_ban_reason(user_id)
        response += f"• `{user_id}` - {reason[:50]}...\n"
    
    if len(banned_users) > 50:
        response += f"\n... and {len(banned_users) - 50} more banned users."
    
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, response, parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data == 'buy_subscription')
def buy_subscription_callback(call):
    """Show subscription purchase options"""
    if is_user_banned(call.from_user.id):
        bot.answer_callback_query(call.id, "❌ You are banned from using this bot.", show_alert=True)
        return
    
    bot.answer_callback_query(call.id)
    
    upi_id = payment_config.get('upi_id', 'yourupi@bank')
    
    response = (f"💳 Buy Subscription\n\n"
                f"📱 UPI ID: `{upi_id}`\n\n"
                f"Select plan:\n"
                f"• 30 Days - ₹{payment_config.get('subscription_price_30_days', 299)}\n"
                f"• 90 Days - ₹{payment_config.get('subscription_price_90_days', 799)}\n"
                f"• 180 Days - ₹{payment_config.get('subscription_price_180_days', 1499)}\n\n"
                f"After payment, send screenshot to @{YOUR_USERNAME.replace('@', '')}")
    
    bot.send_message(call.message.chat.id, 
                    response,
                    reply_markup=create_payment_menu(),
                    parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data.startswith('payment_'))
def payment_plan_callback(call):
    """Handle payment plan selection"""
    plan = call.data.split('_')[1]
    
    prices = {
        '30': payment_config.get('subscription_price_30_days', 299),
        '90': payment_config.get('subscription_price_90_days', 799),
        '180': payment_config.get('subscription_price_180_days', 1499)
    }
    
    if plan in prices:
        amount = prices[plan]
        upi_id = payment_config.get('upi_id', 'yourupi@bank')
        
        # Generate QR code
        qr_bytes = generate_payment_qr(upi_id, amount)
        
        if qr_bytes:
            bot.answer_callback_query(call.id)
            bot.send_photo(call.message.chat.id,
                          qr_bytes,
                          caption=f"💳 Payment QR - {plan} Days\n"
                                 f"Amount: ₹{amount}\n"
                                 f"UPI ID: `{upi_id}`\n\n"
                                 f"After payment, send screenshot to @{YOUR_USERNAME.replace('@', '')}",
                          parse_mode='Markdown')
        else:
            bot.answer_callback_query(call.id, "❌ Failed to generate QR code.", show_alert=True)
    else:
        bot.answer_callback_query(call.id, "❌ Invalid plan.", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data == 'generate_qr')
def generate_qr_callback(call):
    """Generate payment QR without amount"""
    upi_id = payment_config.get('upi_id', 'yourupi@bank')
    qr_bytes = generate_payment_qr(upi_id)
    
    if qr_bytes:
        bot.answer_callback_query(call.id)
        bot.send_photo(call.message.chat.id,
                      qr_bytes,
                      caption=f"📱 Payment QR\n"
                             f"UPI ID: `{upi_id}`\n\n"
                             f"Send any amount and contact admin.",
                      parse_mode='Markdown')
    else:
        bot.answer_callback_query(call.id, "❌ Failed to generate QR code.", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data == 'payment_settings')
def payment_settings_callback(call):
    """Show payment settings menu"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    bot.answer_callback_query(call.id)
    bot.edit_message_text("💰 Payment Settings\nConfigure payment options:",
                         call.message.chat.id,
                         call.message.message_id,
                         reply_markup=create_payment_settings_menu())

@bot.callback_query_handler(func=lambda call: call.data == 'edit_upi_id')
def edit_upi_id_callback(call):
    """Edit UPI ID"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id,
                          "✏️ Enter new UPI ID:\n"
                          "Example: `yourname@upi`\n\n"
                          "/cancel to abort.",
                          parse_mode='Markdown')
    bot.register_next_step_handler(msg, process_edit_upi_id)

def process_edit_upi_id(message):
    """Process UPI ID update"""
    if message.text and message.text.lower() == '/cancel':
        bot.reply_to(message, "Cancelled.")
        return
    
    new_upi_id = message.text.strip()
    
    if '@' not in new_upi_id:
        bot.reply_to(message, "❌ Invalid UPI ID format. Should contain @")
        return
    
    payment_config['upi_id'] = new_upi_id
    if save_payment_config(payment_config):
        bot.reply_to(message, f"✅ UPI ID updated to: `{new_upi_id}`", parse_mode='Markdown')
    else:
        bot.reply_to(message, "❌ Failed to save UPI ID.")

@bot.callback_query_handler(func=lambda call: call.data == 'edit_prices')
def edit_prices_callback(call):
    """Edit subscription prices"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    bot.answer_callback_query(call.id)
    
    current_prices = (f"Current Prices:\n"
                     f"• 30 Days: ₹{payment_config.get('subscription_price_30_days', 299)}\n"
                     f"• 90 Days: ₹{payment_config.get('subscription_price_90_days', 799)}\n"
                     f"• 180 Days: ₹{payment_config.get('subscription_price_180_days', 1499)}\n\n"
                     f"Enter new prices in format:\n"
                     f"`30 299 90 799 180 1499`\n\n"
                     f"/cancel to abort.")
    
    msg = bot.send_message(call.message.chat.id, current_prices, parse_mode='Markdown')
    bot.register_next_step_handler(msg, process_edit_prices)

def process_edit_prices(message):
    """Process price updates"""
    if message.text and message.text.lower() == '/cancel':
        bot.reply_to(message, "Cancelled.")
        return
    
    try:
        parts = message.text.strip().split()
        if len(parts) != 6:
            bot.reply_to(message, "❌ Invalid format. Need 6 values.")
            return
        
        # Parse prices
        prices = {}
        for i in range(0, 6, 2):
            days = parts[i]
            price = int(parts[i+1])
            
            if days == '30':
                prices['subscription_price_30_days'] = price
            elif days == '90':
                prices['subscription_price_90_days'] = price
            elif days == '180':
                prices['subscription_price_180_days'] = price
        
        # Update config
        payment_config.update(prices)
        if save_payment_config(payment_config):
            bot.reply_to(message, f"✅ Prices updated:\n"
                                 f"• 30 Days: ₹{payment_config.get('subscription_price_30_days', 299)}\n"
                                 f"• 90 Days: ₹{payment_config.get('subscription_price_90_days', 799)}\n"
                                 f"• 180 Days: ₹{payment_config.get('subscription_price_180_days', 1499)}")
        else:
            bot.reply_to(message, "❌ Failed to save prices.")
            
    except ValueError:
        bot.reply_to(message, "❌ Invalid price values. Use numbers only.")
    except Exception as e:
        logger.error(f"Error updating prices: {e}")
        bot.reply_to(message, "❌ Error updating prices.")

@bot.callback_query_handler(func=lambda call: call.data == 'admin_generate_qr')
def admin_generate_qr_callback(call):
    """Generate QR for admin preview"""
    upi_id = payment_config.get('upi_id', 'yourupi@bank')
    qr_bytes = generate_payment_qr(upi_id)
    
    if qr_bytes:
        bot.answer_callback_query(call.id)
        bot.send_photo(call.message.chat.id,
                      qr_bytes,
                      caption=f"📱 Payment QR Preview\n"
                             f"UPI ID: `{upi_id}`",
                      parse_mode='Markdown')
    else:
        bot.answer_callback_query(call.id, "❌ Failed to generate QR code.", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data == 'view_payment_config')
def view_payment_config_callback(call):
    """View current payment configuration"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    config_text = (f"💰 Payment Configuration\n\n"
                   f"📱 UPI ID: `{payment_config.get('upi_id', 'Not set')}`\n"
                   f"🔄 QR Enabled: {payment_config.get('qr_code_enabled', True)}\n\n"
                   f"💵 Prices:\n"
                   f"• 30 Days: ₹{payment_config.get('subscription_price_30_days', 299)}\n"
                   f"• 90 Days: ₹{payment_config.get('subscription_price_90_days', 799)}\n"
                   f"• 180 Days: ₹{payment_config.get('subscription_price_180_days', 1499)}")
    
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, config_text, parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data == 'edit_links')
def edit_links_callback(call):
    """Show edit links menu"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    bot.answer_callback_query(call.id)
    bot.edit_message_text("🔗 Edit Inline Button Links\nSelect link to edit:",
                         call.message.chat.id,
                         call.message.message_id,
                         reply_markup=create_edit_links_menu())

@bot.callback_query_handler(func=lambda call: call.data.startswith('edit_link_'))
def edit_specific_link_callback(call):
    """Edit specific link"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    link_type = call.data.replace('edit_link_', '')
    link_names = {
        'updates': 'Updates Channel',
        'support': 'Support Group',
        'tutorial': 'Tutorial Channel',
        'github': 'GitHub Repository',
        'donation': 'Donation Link'
    }
    
    if link_type in link_names:
        current_link = inline_links.get(f'{link_type}_channel' if link_type == 'updates' else f'{link_type}_group' if link_type == 'support' else f'{link_type}_{"channel" if link_type == "tutorial" else "repo" if link_type == "github" else "link"}', 'Not set')
        
        bot.answer_callback_query(call.id)
        msg = bot.send_message(call.message.chat.id,
                              f"✏️ Edit {link_names[link_type]}\n"
                              f"Current: {current_link}\n\n"
                              f"Enter new URL:\n"
                              f"/cancel to abort.")
        bot.register_next_step_handler(msg, process_edit_link, link_type)

def process_edit_link(message, link_type):
    """Process link update"""
    if message.text and message.text.lower() == '/cancel':
        bot.reply_to(message, "Cancelled.")
        return
    
    new_url = message.text.strip()
    
    if not new_url.startswith(('http://', 'https://', 't.me/')):
        bot.reply_to(message, "❌ Invalid URL format. Should start with http://, https://, or t.me/")
        return
    
    key_map = {
        'updates': 'updates_channel',
        'support': 'support_group',
        'tutorial': 'tutorial_channel',
        'github': 'github_repo',
        'donation': 'donation_link'
    }
    
    if link_type in key_map:
        inline_links[key_map[link_type]] = new_url
        if save_inline_links(inline_links):
            bot.reply_to(message, f"✅ Link updated to: {new_url}")
        else:
            bot.reply_to(message, "❌ Failed to save link.")
    else:
        bot.reply_to(message, "❌ Invalid link type.")

@bot.callback_query_handler(func=lambda call: call.data == 'view_links')
def view_links_callback(call):
    """View all inline button links"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    links_text = "🔗 Current Inline Button Links:\n\n"
    for key, value in inline_links.items():
        links_text += f"• {key.replace('_', ' ').title()}: {value}\n"
    
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, links_text)

# --- Callback Query Handlers for ORIGINAL FEATURES ---
@bot.callback_query_handler(func=lambda call: call.data == 'upload')
def upload_callback(call):
    if is_user_banned(call.from_user.id):
        bot.answer_callback_query(call.id, "❌ You are banned from using this bot.", show_alert=True)
        return
    
    user_id = call.from_user.id
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
        bot.answer_callback_query(call.id, f"⚠️ File limit ({current_files}/{limit_str}) reached.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, "📤 Send your Python (`.py`), JS (`.js`), or ZIP (`.zip`) file.")

@bot.callback_query_handler(func=lambda call: call.data == 'check_files')
def check_files_callback(call):
    user_id = call.from_user.id
    user_files_list = user_files.get(user_id, [])
    if not user_files_list:
        bot.answer_callback_query(call.id, "⚠️ No files uploaded.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    markup = types.InlineKeyboardMarkup(row_width=1)
    for file_name, file_type in sorted(user_files_list):
        is_running = is_bot_running(user_id, file_name)
        status_icon = "🟢 Running" if is_running else "🔴 Stopped"
        btn_text = f"{file_name} ({file_type}) - {status_icon}"
        markup.add(types.InlineKeyboardButton(btn_text, callback_data=f'file_{user_id}_{file_name}'))
    markup.add(types.InlineKeyboardButton("🔙 Back to Main", callback_data='back_to_main'))
    bot.edit_message_text("📂 Your files:\nClick to manage.", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data.startswith('file_'))
def file_control_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id

        # Allow owner/admin to control any file, or user to control their own
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ You can only manage your own files.", show_alert=True)
            return

        user_files_list = user_files.get(script_owner_id, [])
        if not any(f[0] == file_name for f in user_files_list):
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True)
            return

        bot.answer_callback_query(call.id)
        is_running = is_bot_running(script_owner_id, file_name)
        status_text = '🟢 Running' if is_running else '🔴 Stopped'
        file_type = next((f[1] for f in user_files_list if f[0] == file_name), '?')
        bot.edit_message_text(
            f"⚙️ Controls for: `{file_name}` ({file_type})\nStatus: {status_text}",
            call.message.chat.id, call.message.message_id,
            reply_markup=create_control_buttons(script_owner_id, file_name, is_running),
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Error in file_control_callback: {e}")
        bot.answer_callback_query(call.id, "Error processing request.")

@bot.callback_query_handler(func=lambda call: call.data.startswith('start_'))
def start_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id

        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True)
            return

        user_files_list = user_files.get(script_owner_id, [])
        file_info = next((f for f in user_files_list if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True)
            return

        file_type = file_info[1]
        user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name)

        if not os.path.exists(file_path):
            bot.answer_callback_query(call.id, f"⚠️ Error: File `{file_name}` missing!", show_alert=True)
            remove_user_file_db(script_owner_id, file_name)
            return

        if is_bot_running(script_owner_id, file_name):
            bot.answer_callback_query(call.id, f"⚠️ Script already running.", show_alert=True)
            return

        bot.answer_callback_query(call.id, f"⏳ Starting {file_name}...")

        if file_type == 'py':
            threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        elif file_type == 'js':
            threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()

        time.sleep(1)
        is_now_running = is_bot_running(script_owner_id, file_name)
        status_text = '🟢 Running' if is_now_running else '🟡 Starting'
        bot.edit_message_text(
            f"⚙️ Controls for: `{file_name}` ({file_type})\nStatus: {status_text}",
            call.message.chat.id, call.message.message_id,
            reply_markup=create_control_buttons(script_owner_id, file_name, is_now_running),
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Error in start_bot_callback: {e}")
        bot.answer_callback_query(call.id, "Error starting script.")

@bot.callback_query_handler(func=lambda call: call.data.startswith('stop_'))
def stop_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id

        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True)
            return

        user_files_list = user_files.get(script_owner_id, [])
        file_info = next((f for f in user_files_list if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True)
            return

        file_type = file_info[1]
        script_key = f"{script_owner_id}_{file_name}"

        if not is_bot_running(script_owner_id, file_name):
            bot.answer_callback_query(call.id, f"⚠️ Script already stopped.", show_alert=True)
            return

        bot.answer_callback_query(call.id, f"⏳ Stopping {file_name}...")
        process_info = bot_scripts.get(script_key)
        if process_info:
            kill_process_tree(process_info)
            if script_key in bot_scripts:
                del bot_scripts[script_key]

        bot.edit_message_text(
            f"⚙️ Controls for: `{file_name}` ({file_type})\nStatus: 🔴 Stopped",
            call.message.chat.id, call.message.message_id,
            reply_markup=create_control_buttons(script_owner_id, file_name, False),
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Error in stop_bot_callback: {e}")
        bot.answer_callback_query(call.id, "Error stopping script.")

@bot.callback_query_handler(func=lambda call: call.data.startswith('restart_'))
def restart_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id

        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True)
            return

        user_files_list = user_files.get(script_owner_id, [])
        file_info = next((f for f in user_files_list if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True)
            return

        file_type = file_info[1]
        user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name)
        script_key = f"{script_owner_id}_{file_name}"

        if not os.path.exists(file_path):
            bot.answer_callback_query(call.id, f"⚠️ Error: File missing!", show_alert=True)
            remove_user_file_db(script_owner_id, file_name)
            return

        bot.answer_callback_query(call.id, f"⏳ Restarting {file_name}...")
        
        # Stop if running
        if is_bot_running(script_owner_id, file_name):
            process_info = bot_scripts.get(script_key)
            if process_info:
                kill_process_tree(process_info)
            if script_key in bot_scripts:
                del bot_scripts[script_key]
            time.sleep(1)

        # Start again
        if file_type == 'py':
            threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        elif file_type == 'js':
            threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()

        time.sleep(1)
        is_now_running = is_bot_running(script_owner_id, file_name)
        status_text = '🟢 Running' if is_now_running else '🟡 Starting'
        bot.edit_message_text(
            f"⚙️ Controls for: `{file_name}` ({file_type})\nStatus: {status_text}",
            call.message.chat.id, call.message.message_id,
            reply_markup=create_control_buttons(script_owner_id, file_name, is_now_running),
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Error in restart_bot_callback: {e}")
        bot.answer_callback_query(call.id, "Error restarting script.")

@bot.callback_query_handler(func=lambda call: call.data.startswith('delete_'))
def delete_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id

        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True)
            return

        user_files_list = user_files.get(script_owner_id, [])
        if not any(f[0] == file_name for f in user_files_list):
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True)
            return

        bot.answer_callback_query(call.id, f"🗑️ Deleting {file_name}...")
        script_key = f"{script_owner_id}_{file_name}"
        
        # Stop if running
        if is_bot_running(script_owner_id, file_name):
            process_info = bot_scripts.get(script_key)
            if process_info:
                kill_process_tree(process_info)
            if script_key in bot_scripts:
                del bot_scripts[script_key]

        # Delete files
        user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name)
        log_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        
        if os.path.exists(file_path):
            os.remove(file_path)
        if os.path.exists(log_path):
            os.remove(log_path)

        # Remove from database
        remove_user_file_db(script_owner_id, file_name)

        bot.edit_message_text(
            f"🗑️ File `{file_name}` deleted!",
            call.message.chat.id, call.message.message_id
        )
    except Exception as e:
        logger.error(f"Error in delete_bot_callback: {e}")
        bot.answer_callback_query(call.id, "Error deleting file.")

@bot.callback_query_handler(func=lambda call: call.data.startswith('logs_'))
def logs_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id

        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True)
            return

        user_files_list = user_files.get(script_owner_id, [])
        if not any(f[0] == file_name for f in user_files_list):
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True)
            return

        user_folder = get_user_folder(script_owner_id)
        log_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        
        if not os.path.exists(log_path):
            bot.answer_callback_query(call.id, f"⚠️ No logs for '{file_name}'.", show_alert=True)
            return

        bot.answer_callback_query(call.id)
        try:
            with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                log_content = f.read()
            
            if len(log_content) > 4000:
                log_content = log_content[-4000:]
                log_content = "...\n" + log_content
            
            bot.send_message(call.message.chat.id, 
                           f"📜 Logs for `{file_name}`:\n```\n{log_content}\n```", 
                           parse_mode='Markdown')
        except Exception as e:
            bot.send_message(call.message.chat.id, f"❌ Error reading log: {str(e)}")
    except Exception as e:
        logger.error(f"Error in logs_bot_callback: {e}")
        bot.answer_callback_query(call.id, "Error fetching logs.")

@bot.callback_query_handler(func=lambda call: call.data == 'speed')
def speed_callback(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    start_cb_ping_time = time.time()
    try:
        bot.edit_message_text("🏃 Testing speed...", chat_id, call.message.message_id)
        bot.send_chat_action(chat_id, 'typing')
        response_time = round((time.time() - start_cb_ping_time) * 1000, 2)
        status = "🔓 Unlocked" if not bot_locked else "🔒 Locked"
        if user_id == OWNER_ID:
            user_level = "👑 Owner"
        elif user_id in admin_ids:
            user_level = "🛡️ Admin"
        elif user_id in user_subscriptions and user_subscriptions[user_id].get('expiry', datetime.min) > datetime.now():
            user_level = "⭐ Premium"
        else:
            user_level = "🆓 Free User"
        speed_msg = (f"⚡ Bot Speed & Status:\n\n⏱️ API Response Time: {response_time} ms\n"
                     f"🚦 Bot Status: {status}\n"
                     f"👤 Your Level: {user_level}")
        bot.answer_callback_query(call.id)
        bot.edit_message_text(speed_msg, chat_id, call.message.message_id, reply_markup=create_main_menu_inline(user_id))
    except Exception as e:
        logger.error(f"Error during speed test: {e}")
        bot.answer_callback_query(call.id, "Error in speed test.", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data == 'stats')
def stats_callback(call):
    bot.answer_callback_query(call.id)
    _logic_statistics(call.message)

@bot.callback_query_handler(func=lambda call: call.data == 'back_to_main')
def back_to_main_callback(call):
    user_id = call.from_user.id
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
    expiry_info = ""
    if user_id == OWNER_ID:
        user_status = "👑 Owner"
    elif user_id in admin_ids:
        user_status = "🛡️ Admin"
    elif user_id in user_subscriptions:
        expiry_date = user_subscriptions[user_id].get('expiry')
        if expiry_date and expiry_date > datetime.now():
            user_status = "⭐ Premium"
            days_left = (expiry_date - datetime.now()).days
            expiry_info = f"\n⏳ Subscription expires in: {days_left} days"
        else:
            user_status = "🆓 Free User (Expired Sub)"
    else:
        user_status = "🆓 Free User"
    main_menu_text = (f"〽️ Welcome back, {call.from_user.first_name}!\n\n🆔 ID: `{user_id}`\n"
                      f"🔰 Status: {user_status}{expiry_info}\n📁 Files: {current_files} / {limit_str}\n\n"
                      f"👇 Use buttons or type commands.")
    bot.answer_callback_query(call.id)
    bot.edit_message_text(main_menu_text, call.message.chat.id, call.message.message_id,
                          reply_markup=create_main_menu_inline(user_id), parse_mode='Markdown')

# --- Admin Callback Handlers ---
@bot.callback_query_handler(func=lambda call: call.data == 'subscription')
def subscription_management_callback(call):
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    bot.edit_message_text("💳 Subscription Management\nSelect action:",
                         call.message.chat.id, call.message.message_id,
                         reply_markup=create_subscription_menu())

@bot.callback_query_handler(func=lambda call: call.data == 'lock_bot')
def lock_bot_callback(call):
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    global bot_locked
    bot_locked = True
    bot.answer_callback_query(call.id, "🔒 Bot locked.")
    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id,
                                  reply_markup=create_main_menu_inline(call.from_user.id))

@bot.callback_query_handler(func=lambda call: call.data == 'unlock_bot')
def unlock_bot_callback(call):
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    global bot_locked
    bot_locked = False
    bot.answer_callback_query(call.id, "🔓 Bot unlocked.")
    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id,
                                  reply_markup=create_main_menu_inline(call.from_user.id))

@bot.callback_query_handler(func=lambda call: call.data == 'run_all_scripts')
def run_all_scripts_callback(call):
    _logic_run_all_scripts(call)

@bot.callback_query_handler(func=lambda call: call.data == 'broadcast')
def broadcast_init_callback(call):
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "📢 Send message to broadcast.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_broadcast_message)

def process_broadcast_message(message):
    user_id = message.from_user.id
    if user_id not in admin_ids:
        bot.reply_to(message, "⚠️ Not authorized.")
        return
    if message.text and message.text.lower() == '/cancel':
        bot.reply_to(message, "Broadcast cancelled.")
        return

    broadcast_content = message.text
    if not broadcast_content:
        bot.reply_to(message, "⚠️ Cannot broadcast empty message.")
        return

    target_count = len(active_users)
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton("✅ Confirm & Send", callback_data=f"confirm_broadcast_{message.message_id}"),
        types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_broadcast")
    )

    preview_text = broadcast_content[:1000].strip()
    bot.reply_to(message, f"⚠️ Confirm Broadcast:\n\n```\n{preview_text}\n```\n"
                          f"To **{target_count}** users. Sure?", reply_markup=markup, parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data.startswith('confirm_broadcast_'))
def handle_confirm_broadcast(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    if user_id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin only.", show_alert=True)
        return
    try:
        original_message = call.message.reply_to_message
        if not original_message:
            raise ValueError("Could not retrieve original message.")

        broadcast_text = original_message.text
        bot.answer_callback_query(call.id, "🚀 Starting broadcast...")
        bot.edit_message_text(f"📢 Broadcasting to {len(active_users)} users...",
                              chat_id, call.message.message_id, reply_markup=None)
        
        def execute_broadcast():
            sent_count = 0
            failed_count = 0
            for user_id_bc in active_users:
                try:
                    bot.send_message(user_id_bc, broadcast_text, parse_mode='Markdown')
                    sent_count += 1
                except:
                    failed_count += 1
                time.sleep(0.1)
            
            result_msg = (f"📢 Broadcast Complete!\n\n✅ Sent: {sent_count}\n"
                          f"❌ Failed: {failed_count}\n👥 Targets: {len(active_users)}")
            bot.send_message(chat_id, result_msg)
        
        threading.Thread(target=execute_broadcast).start()
    except Exception as e:
        logger.error(f"Error in broadcast: {e}")
        bot.edit_message_text("❌ Error during broadcast.", chat_id, call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data == 'cancel_broadcast')
def handle_cancel_broadcast(call):
    bot.answer_callback_query(call.id, "Broadcast cancelled.")
    bot.delete_message(call.message.chat.id, call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data == 'admin_panel')
def admin_panel_callback(call):
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    bot.edit_message_text("👑 Admin Panel\nManage admins:",
                         call.message.chat.id, call.message.message_id,
                         reply_markup=create_admin_panel())

@bot.callback_query_handler(func=lambda call: call.data == 'add_admin')
def add_admin_init_callback(call):
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Owner permissions required.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "👑 Enter User ID to promote to Admin.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_add_admin_id)

def process_add_admin_id(message):
    if message.from_user.id != OWNER_ID:
        bot.reply_to(message, "⚠️ Owner only.")
        return
    if message.text.lower() == '/cancel':
        bot.reply_to(message, "Admin promotion cancelled.")
        return
    try:
        new_admin_id = int(message.text.strip())
        if new_admin_id <= 0:
            raise ValueError("ID must be positive")
        if new_admin_id == OWNER_ID:
            bot.reply_to(message, "⚠️ Owner is already Owner.")
            return
        if new_admin_id in admin_ids:
            bot.reply_to(message, f"⚠️ User `{new_admin_id}` already Admin.")
            return
        add_admin_db(new_admin_id)
        bot.reply_to(message, f"✅ User `{new_admin_id}` promoted to Admin.")
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid ID. Send numerical ID or /cancel.")
    except Exception as e:
        logger.error(f"Error processing add admin: {e}")
        bot.reply_to(message, "Error.")

@bot.callback_query_handler(func=lambda call: call.data == 'remove_admin')
def remove_admin_init_callback(call):
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Owner permissions required.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "👑 Enter User ID of Admin to remove.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_remove_admin_id)

def process_remove_admin_id(message):
    if message.from_user.id != OWNER_ID:
        bot.reply_to(message, "⚠️ Owner only.")
        return
    if message.text.lower() == '/cancel':
        bot.reply_to(message, "Admin removal cancelled.")
        return
    try:
        admin_id_remove = int(message.text.strip())
        if admin_id_remove <= 0:
            raise ValueError("ID must be positive")
        if admin_id_remove == OWNER_ID:
            bot.reply_to(message, "⚠️ Owner cannot remove self.")
            return
        if remove_admin_db(admin_id_remove):
            bot.reply_to(message, f"✅ Admin `{admin_id_remove}` removed.")
        else:
            bot.reply_to(message, f"❌ Failed to remove admin `{admin_id_remove}`.")
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid ID. Send numerical ID or /cancel.")
    except Exception as e:
        logger.error(f"Error processing remove admin: {e}")
        bot.reply_to(message, "Error.")

@bot.callback_query_handler(func=lambda call: call.data == 'list_admins')
def list_admins_callback(call):
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    admin_list_str = "\n".join(f"- `{aid}` {'(Owner)' if aid == OWNER_ID else ''}" for aid in sorted(list(admin_ids)))
    bot.edit_message_text(f"👑 Current Admins:\n\n{admin_list_str}",
                         call.message.chat.id, call.message.message_id,
                         reply_markup=create_admin_panel(), parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data == 'add_subscription')
def add_subscription_init_callback(call):
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "💳 Enter User ID & days (e.g., `12345678 30`).\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_add_subscription_details)

def process_add_subscription_details(message):
    admin_id_check = message.from_user.id
    if admin_id_check not in admin_ids:
        bot.reply_to(message, "⚠️ Not authorized.")
        return
    if message.text.lower() == '/cancel':
        bot.reply_to(message, "Sub add cancelled.")
        return
    try:
        parts = message.text.split()
        if len(parts) != 2:
            raise ValueError("Incorrect format")
        sub_user_id = int(parts[0].strip())
        days = int(parts[1].strip())
        if sub_user_id <= 0 or days <= 0:
            raise ValueError("User ID/days must be positive")

        current_expiry = user_subscriptions.get(sub_user_id, {}).get('expiry')
        start_date_new_sub = datetime.now()
        if current_expiry and current_expiry > start_date_new_sub:
            start_date_new_sub = current_expiry
        new_expiry = start_date_new_sub + timedelta(days=days)
        save_subscription(sub_user_id, new_expiry)

        bot.reply_to(message, f"✅ Sub for `{sub_user_id}` by {days} days.\nNew expiry: {new_expiry:%Y-%m-%d}")
    except ValueError as e:
        bot.reply_to(message, f"⚠️ Invalid: {e}. Format: `ID days` or /cancel.")
    except Exception as e:
        logger.error(f"Error processing add sub: {e}")
        bot.reply_to(message, "Error.")

@bot.callback_query_handler(func=lambda call: call.data == 'remove_subscription')
def remove_subscription_init_callback(call):
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "💳 Enter User ID to remove sub.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_remove_subscription_id)

def process_remove_subscription_id(message):
    admin_id_check = message.from_user.id
    if admin_id_check not in admin_ids:
        bot.reply_to(message, "⚠️ Not authorized.")
        return
    if message.text.lower() == '/cancel':
        bot.reply_to(message, "Sub removal cancelled.")
        return
    try:
        sub_user_id_remove = int(message.text.strip())
        if sub_user_id_remove <= 0:
            raise ValueError("ID must be positive")
        if sub_user_id_remove not in user_subscriptions:
            bot.reply_to(message, f"⚠️ User `{sub_user_id_remove}` no active sub.")
            return
        remove_subscription_db(sub_user_id_remove)
        bot.reply_to(message, f"✅ Sub for `{sub_user_id_remove}` removed.")
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid ID. Send numerical ID or /cancel.")
    except Exception as e:
        logger.error(f"Error processing remove sub: {e}")
        bot.reply_to(message, "Error.")

@bot.callback_query_handler(func=lambda call: call.data == 'check_subscription')
def check_subscription_init_callback(call):
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "💳 Enter User ID to check sub.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_check_subscription_id)

def process_check_subscription_id(message):
    admin_id_check = message.from_user.id
    if admin_id_check not in admin_ids:
        bot.reply_to(message, "⚠️ Not authorized.")
        return
    if message.text.lower() == '/cancel':
        bot.reply_to(message, "Sub check cancelled.")
        return
    try:
        sub_user_id_check = int(message.text.strip())
        if sub_user_id_check <= 0:
            raise ValueError("ID must be positive")
        if sub_user_id_check in user_subscriptions:
            expiry_dt = user_subscriptions[sub_user_id_check].get('expiry')
            if expiry_dt:
                if expiry_dt > datetime.now():
                    days_left = (expiry_dt - datetime.now()).days
                    bot.reply_to(message, f"✅ User `{sub_user_id_check}` active sub.\nExpires: {expiry_dt:%Y-%m-%d} ({days_left} days left).")
                else:
                    bot.reply_to(message, f"⚠️ User `{sub_user_id_check}` expired sub (On: {expiry_dt:%Y-%m-%d}).")
            else:
                bot.reply_to(message, f"⚠️ User `{sub_user_id_check}` in sub list, but expiry missing.")
        else:
            bot.reply_to(message, f"ℹ️ User `{sub_user_id_check}` no active sub record.")
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid ID. Send numerical ID or /cancel.")
    except Exception as e:
        logger.error(f"Error processing check sub: {e}")
        bot.reply_to(message, "Error.")

# --- Cleanup Function ---
def cleanup():
    logger.warning("Shutdown. Cleaning up processes...")
    script_keys_to_stop = list(bot_scripts.keys())
    for key in script_keys_to_stop:
        if key in bot_scripts:
            kill_process_tree(bot_scripts[key])
    logger.warning("Cleanup finished.")

atexit.register(cleanup)

# --- Main Execution ---
if __name__ == '__main__':
    logger.info("="*40 + "\n🤖 Enhanced Bot Hosting Starting...\n" + 
                f"📱 Payment: {'Enabled' if payment_config.get('qr_code_enabled', True) else 'Disabled'}\n" +
                f"🚫 Banned Users: {len(banned_users)}\n" + "="*40)
    
    keep_alive()
    logger.info("🚀 Starting enhanced bot polling...")
    
    while True:
        try:
            bot.infinity_polling(timeout=60, long_polling_timeout=30)
        except requests.exceptions.ConnectionError:
            logger.warning("Connection error. Retrying in 10s...")
            time.sleep(10)
        except Exception as e:
            logger.error(f"Polling error: {e}")
            time.sleep(5)