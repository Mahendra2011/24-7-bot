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
import random
import string
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# --- Flask Keep Alive ---
from flask import Flask
from threading import Thread

app = Flask('')

@app.route('/')
def home():
    return "I'm Marco File Host - Subscription Based Version"

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
TOKEN = '8738782439:AAEd_MPUuQ7qhd887UN1W4UFderR2rnUYV8'
OWNER_ID = 7855338525
ADMIN_ID = 7855338525
YOUR_USERNAME = '@Deleted0Account'
UPDATE_CHANNEL = 'https://t.me/+ThhtU6Jx0MI2ZGRl'

# Email Configuration (Gmail with app password)
EMAIL_ADDRESS = 'advancehostingTG@gmail.com'          # Replace with your Gmail
EMAIL_PASSWORD = 'arpm lnne pyxb zxzw'                # Fixed: removed invisible character
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587

# Payment Configuration
PAYMENT_CONFIG_FILE = 'payment_config.json'
DEFAULT_PAYMENT_CONFIG = {
    'upi_id': 'robintyagi@fam',
    'qr_code_enabled': True,
    'subscription_price_30_days': 299,
    'subscription_price_90_days': 799,
    'subscription_price_180_days': 1499
}

# Subscription Plans
SUBSCRIPTION_PLANS = {
    '30': {'days': 30, 'price': 299, 'name': '30 Days'},
    '90': {'days': 90, 'price': 799, 'name': '90 Days'},
    '180': {'days': 180, 'price': 1499, 'name': '180 Days'}
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
SUBSCRIPTION_REQUESTS_DIR = os.path.join(IROTECH_DIR, 'subscription_requests')
PAYMENT_SCREENSHOTS_DIR = os.path.join(IROTECH_DIR, 'payment_screenshots')

# File upload limits
FREE_USER_LIMIT = 0  # Free users cannot upload files
SUBSCRIBED_USER_LIMIT = 15
ADMIN_LIMIT = 999
OWNER_LIMIT = float('inf')

# Create necessary directories
os.makedirs(UPLOAD_BOTS_DIR, exist_ok=True)
os.makedirs(IROTECH_DIR, exist_ok=True)
os.makedirs(PROBLEMS_DIR, exist_ok=True)
os.makedirs(SUBSCRIPTION_REQUESTS_DIR, exist_ok=True)
os.makedirs(PAYMENT_SCREENSHOTS_DIR, exist_ok=True)

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
pending_requests = {}  # Store pending subscription requests (status='pending')
code_sent_requests = {}  # Store requests with code sent (status='code_sent')

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
    ["💳 Get Subscription", "📂 Check Files"],
    ["⚡ Bot Speed", "📊 Statistics"],
    ["📝 Submit Problem", "📞 Contact Owner"]
]

SUBSCRIBED_USER_BUTTONS_LAYOUT = [
    ["📢 Updates Channel"],
    ["📤 Upload File", "📂 Check Files"],
    ["⚡ Bot Speed", "📊 Statistics"],
    ["💳 My Subscription", "📝 Submit Problem"],
    ["📞 Contact Owner"]
]

ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC = [
    ["📢 Updates Channel"],
    ["📤 Upload File", "📂 Check Files"],
    ["⚡ Bot Speed", "📊 Statistics"],
    ["💳 Subscription Requests", "📢 Broadcast"],
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
                     (user_id INTEGER PRIMARY KEY, expiry TEXT, subscription_code TEXT, activated_at TEXT)''')
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
        
        # New table for subscription requests (status: pending, code_sent, approved, rejected)
        c.execute('''CREATE TABLE IF NOT EXISTS subscription_requests
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      user_id INTEGER, user_name TEXT, user_email TEXT,
                      plan_days INTEGER, amount INTEGER, utr_number TEXT,
                      screenshot_file_id TEXT, status TEXT DEFAULT 'pending',
                      created_at TEXT, reviewed_at TEXT, reviewed_by INTEGER,
                      activation_code TEXT)''')

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
        c.execute('SELECT user_id, expiry, subscription_code FROM subscriptions')
        for user_id, expiry, code in c.fetchall():
            try:
                user_subscriptions[user_id] = {
                    'expiry': datetime.fromisoformat(expiry),
                    'code': code
                }
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

        # Load pending subscription requests (status='pending')
        c.execute('''SELECT id, user_id, user_name, user_email, plan_days, amount, 
                    utr_number, screenshot_file_id, created_at 
                    FROM subscription_requests WHERE status = 'pending' ''')
        for row in c.fetchall():
            req_id, user_id, user_name, email, days, amount, utr, screenshot, created = row
            pending_requests[req_id] = {
                'id': req_id,
                'user_id': user_id,
                'user_name': user_name,
                'email': email,
                'days': days,
                'amount': amount,
                'utr': utr,
                'screenshot': screenshot,
                'created_at': created
            }

        # Load code-sent requests (status='code_sent') for redemption
        c.execute('''SELECT id, user_id, user_email, plan_days, activation_code
                    FROM subscription_requests WHERE status = 'code_sent' ''')
        for row in c.fetchall():
            req_id, user_id, email, days, code = row
            code_sent_requests[code] = {
                'id': req_id,
                'user_id': user_id,
                'email': email,
                'days': days
            }

        conn.close()
        logger.info(f"Data loaded: {len(active_users)} users, {len(user_subscriptions)} subscriptions, "
                   f"{len(admin_ids)} admins, {len(banned_users)} banned users, "
                   f"{len(pending_requests)} pending requests, {len(code_sent_requests)} codes sent.")
    except Exception as e:
        logger.error(f"❌ Error loading data: {e}", exc_info=True)

# Initialize DB and Load Data at startup
init_db()
load_data()

# --- Email Sending Function (FIXED) ---
def send_activation_email(to_email, activation_code):
    """Send activation code via email using Gmail SMTP"""
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_ADDRESS
        msg['To'] = to_email
        msg['Subject'] = "Your Subscription Activation Code"

        body = f"""
        Hello,

        Thank you for your payment. Your subscription request has been approved.

        Your activation code is: {activation_code}

        To activate your subscription, please send the following command to the bot:
        /redeem {activation_code}

        If you did not request this, please ignore this email.

        Regards,
        Support Team
        """
        # Explicitly set UTF-8 encoding to handle any non-ASCII characters
        msg.attach(MIMEText(body, 'plain', 'utf-8'))

        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        logger.info(f"Activation email sent to {to_email}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email to {to_email}: {e}")
        return False

# --- Helper Functions ---
def generate_activation_code():
    """Generate a 12-digit activation code"""
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))

def is_user_subscribed(user_id):
    """Check if user has active subscription"""
    if user_id in admin_ids or user_id == OWNER_ID:
        return True
    
    if user_id in user_subscriptions:
        expiry = user_subscriptions[user_id].get('expiry')
        if expiry and expiry > datetime.now():
            return True
        else:
            # Remove expired subscription
            remove_subscription_db(user_id)
    
    return False

def get_user_file_limit(user_id):
    """Get the file upload limit for a user"""
    if is_user_banned(user_id):
        return 0

    if user_id == OWNER_ID:
        return OWNER_LIMIT
    if user_id in admin_ids:
        return ADMIN_LIMIT
    if is_user_subscribed(user_id):
        return SUBSCRIBED_USER_LIMIT
    return FREE_USER_LIMIT

def save_subscription_request(user_id, user_name, email, days, amount, utr, screenshot_file_id):
    """Save subscription request to database"""
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        created_at = datetime.now().isoformat()
        
        c.execute('''INSERT INTO subscription_requests 
                    (user_id, user_name, user_email, plan_days, amount, utr_number, screenshot_file_id, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                  (user_id, user_name, email, days, amount, utr, screenshot_file_id, created_at))
        
        conn.commit()
        request_id = c.lastrowid
        conn.close()

        # Save to memory
        pending_requests[request_id] = {
            'id': request_id,
            'user_id': user_id,
            'user_name': user_name,
            'email': email,
            'days': days,
            'amount': amount,
            'utr': utr,
            'screenshot': screenshot_file_id,
            'created_at': created_at
        }

        # Save screenshot info
        request_folder = os.path.join(PAYMENT_SCREENSHOTS_DIR, str(request_id))
        os.makedirs(request_folder, exist_ok=True)
        
        with open(os.path.join(request_folder, 'info.txt'), 'w') as f:
            f.write(f"Request ID: {request_id}\n")
            f.write(f"User ID: {user_id}\n")
            f.write(f"User Name: {user_name}\n")
            f.write(f"Email: {email}\n")
            f.write(f"Plan: {days} days\n")
            f.write(f"Amount: ₹{amount}\n")
            f.write(f"UTR: {utr}\n")
            f.write(f"Date: {created_at}\n")

        logger.info(f"Subscription request saved: ID={request_id}, User={user_id}")
        return request_id
    except Exception as e:
        logger.error(f"Error saving subscription request: {e}")
        return None

def approve_subscription_request(request_id, admin_id):
    """Approve subscription request: generate code, send email, update status to 'code_sent'"""
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        
        # Get request details (ensure it's still pending)
        c.execute('''SELECT user_id, user_email, plan_days FROM subscription_requests 
                    WHERE id = ? AND status = 'pending' ''', (request_id,))
        result = c.fetchone()
        
        if not result:
            logger.warning(f"Request {request_id} not found or not pending.")
            conn.close()
            return None, None, None
        
        user_id, email, days = result
        
        # Generate activation code
        activation_code = generate_activation_code()
        reviewed_at = datetime.now().isoformat()
        
        # Update request status to 'code_sent' and store code
        c.execute('''UPDATE subscription_requests 
                    SET status = 'code_sent', reviewed_at = ?, reviewed_by = ?, activation_code = ?
                    WHERE id = ?''',
                  (reviewed_at, admin_id, activation_code, request_id))
        
        conn.commit()
        conn.close()
        
        # Update in-memory caches
        if request_id in pending_requests:
            del pending_requests[request_id]
        
        code_sent_requests[activation_code] = {
            'id': request_id,
            'user_id': user_id,
            'email': email,
            'days': days
        }
        
        logger.info(f"Request {request_id} approved, code {activation_code} generated for user {user_id}")
        return activation_code, email, user_id
        
    except Exception as e:
        logger.error(f"Error approving subscription request {request_id}: {e}", exc_info=True)
        return None, None, None

def redeem_code(user_id, code):
    """Redeem activation code and activate subscription"""
    code = code.strip().upper()
    if code not in code_sent_requests:
        return False, "Invalid or expired code."
    
    req_info = code_sent_requests[code]
    if req_info['user_id'] != user_id:
        return False, "This code belongs to another user."
    
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        
        # Verify the request is still in 'code_sent' status
        c.execute('''SELECT id, user_id, plan_days FROM subscription_requests 
                    WHERE id = ? AND status = 'code_sent' AND activation_code = ?''',
                  (req_info['id'], code))
        result = c.fetchone()
        if not result:
            conn.close()
            return False, "Code already used or invalid."
        
        req_id, user_id_db, days = result
        
        # Calculate expiry date
        current_expiry = user_subscriptions.get(user_id, {}).get('expiry')
        start_date = datetime.now()
        if current_expiry and current_expiry > start_date:
            start_date = current_expiry
        expiry_date = start_date + timedelta(days=int(days))
        
        # Update request status to 'approved'
        reviewed_at = datetime.now().isoformat()
        c.execute('''UPDATE subscription_requests 
                    SET status = 'approved', reviewed_at = ?
                    WHERE id = ?''',
                  (reviewed_at, req_id))
        
        # Save subscription
        c.execute('''INSERT OR REPLACE INTO subscriptions (user_id, expiry, subscription_code, activated_at)
                    VALUES (?, ?, ?, ?)''',
                  (user_id, expiry_date.isoformat(), code, reviewed_at))
        
        conn.commit()
        conn.close()
        
        # Update memory
        user_subscriptions[user_id] = {
            'expiry': expiry_date,
            'code': code
        }
        
        # Remove from code_sent cache
        del code_sent_requests[code]
        
        logger.info(f"User {user_id} redeemed code {code}, subscription active until {expiry_date}")
        return True, expiry_date
    except Exception as e:
        logger.error(f"Error redeeming code {code} for user {user_id}: {e}", exc_info=True)
        return False, "An error occurred. Please try again later."

def reject_subscription_request(request_id, admin_id, reason=""):
    """Reject subscription request"""
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        
        reviewed_at = datetime.now().isoformat()
        
        c.execute('''UPDATE subscription_requests 
                    SET status = 'rejected', reviewed_at = ?, reviewed_by = ?
                    WHERE id = ? AND status = 'pending' ''',
                  (reviewed_at, admin_id, request_id))
        
        conn.commit()
        rows_affected = c.rowcount
        conn.close()
        
        if rows_affected > 0:
            # Remove from pending requests cache
            if request_id in pending_requests:
                del pending_requests[request_id]
            logger.info(f"Request {request_id} rejected by admin {admin_id}. Reason: {reason}")
            return True
        else:
            logger.warning(f"Request {request_id} not found or already processed.")
            return False
    except Exception as e:
        logger.error(f"Error rejecting subscription request {request_id}: {e}", exc_info=True)
        return False

def get_pending_requests():
    """Get all pending subscription requests"""
    return list(pending_requests.values())

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

def get_user_folder(user_id):
    """Get or create user's folder for storing files"""
    user_folder = os.path.join(UPLOAD_BOTS_DIR, str(user_id))
    os.makedirs(user_folder, exist_ok=True)
    return user_folder

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
    is_subscribed = is_user_subscribed(user_id) or user_id in admin_ids

    # Basic buttons for all users
    buttons = [
        types.InlineKeyboardButton('📢 Updates Channel', url=inline_links['updates_channel']),
        types.InlineKeyboardButton('📂 Check Files', callback_data='check_files'),
        types.InlineKeyboardButton('⚡ Bot Speed', callback_data='speed'),
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
            types.InlineKeyboardButton('💳 Subscription Requests', callback_data='view_requests'),
            types.InlineKeyboardButton('📢 Broadcast', callback_data='broadcast'),
            types.InlineKeyboardButton('🔒 Lock Bot' if not bot_locked else '🔓 Unlock Bot',
                                     callback_data='lock_bot' if not bot_locked else 'unlock_bot'),
            types.InlineKeyboardButton('👑 Admin Panel', callback_data='admin_panel'),
            types.InlineKeyboardButton('🟢 Run All Scripts', callback_data='run_all_scripts')
        ]

        markup.row(buttons[0])
        if is_subscribed:
            markup.row(types.InlineKeyboardButton('📤 Upload File', callback_data='upload'), buttons[1])
        else:
            markup.row(types.InlineKeyboardButton('💳 Get Subscription', callback_data='get_subscription'), buttons[1])
        markup.row(buttons[2], admin_buttons[0])
        markup.row(admin_buttons[1], admin_buttons[2])
        markup.row(admin_buttons[3], admin_buttons[4])
        markup.row(admin_buttons[5], admin_buttons[6])
        markup.row(admin_buttons[7], admin_buttons[8])
        markup.row(buttons[3], buttons[4])
        markup.row(buttons[5])
        
    elif is_subscribed:
        # Subscribed user buttons
        markup.row(buttons[0])
        markup.row(types.InlineKeyboardButton('📤 Upload File', callback_data='upload'), buttons[1])
        markup.row(buttons[2], buttons[5])
        markup.row(types.InlineKeyboardButton('💳 My Subscription', callback_data='my_subscription'), buttons[3])
        markup.row(buttons[4])
    else:
        # Free user buttons
        markup.row(buttons[0])
        markup.row(types.InlineKeyboardButton('💳 Get Subscription', callback_data='get_subscription'), buttons[1])
        markup.row(buttons[2], buttons[5])
        markup.row(buttons[3], buttons[4])

    return markup

def create_reply_keyboard_main_menu(user_id):
    """Create reply keyboard main menu"""
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    
    if user_id in admin_ids:
        layout_to_use = ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC
    elif is_user_subscribed(user_id):
        layout_to_use = SUBSCRIBED_USER_BUTTONS_LAYOUT
    else:
        layout_to_use = COMMAND_BUTTONS_LAYOUT_USER_SPEC
    
    for row_buttons_text in layout_to_use:
        markup.add(*[types.KeyboardButton(text) for text in row_buttons_text])
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

def create_subscription_request_menu():
    """Create menu for subscription request"""
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton('30 Days - ₹299', callback_data='request_30'),
        types.InlineKeyboardButton('90 Days - ₹799', callback_data='request_90')
    )
    markup.row(
        types.InlineKeyboardButton('180 Days - ₹1499', callback_data='request_180'),
        types.InlineKeyboardButton('❌ Cancel', callback_data='back_to_main')
    )
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
    
    # Determine user status
    if user_id == OWNER_ID:
        user_status = "👑 Owner"
    elif user_id in admin_ids:
        user_status = "🛡️ Admin"
    elif is_user_subscribed(user_id):
        expiry_date = user_subscriptions[user_id].get('expiry')
        days_left = (expiry_date - datetime.now()).days
        user_status = f"⭐ Premium (Expires in {days_left} days)"
    else:
        user_status = "🆓 Free User"

    welcome_msg_text = (f"〽️ Welcome, {user_name}!\n\n🆔 Your User ID: `{user_id}`\n"
                        f"✳️ Username: `@{user_username or 'Not set'}`\n"
                        f"🔰 Your Status: {user_status}\n"
                        f"📁 Files Uploaded: {current_files} / {limit_str}\n\n")

    if not is_user_subscribed(user_id) and user_id not in admin_ids:
        welcome_msg_text += (f"❌ You are a Free User and cannot upload files.\n\n"
                            f"💳 To upload and run scripts, you need an active subscription.\n"
                            f"💰 Subscription Plans:\n"
                            f"• 30 Days - ₹{SUBSCRIPTION_PLANS['30']['price']}\n"
                            f"• 90 Days - ₹{SUBSCRIPTION_PLANS['90']['price']}\n"
                            f"• 180 Days - ₹{SUBSCRIPTION_PLANS['180']['price']}\n\n"
                            f"👇 Click 'Get Subscription' to purchase!")
    else:
        welcome_msg_text += f"🤖 You can upload and run Python/JS scripts.\n   Upload single scripts or `.zip` archives.\n\n"

    welcome_msg_text += f"👇 Use buttons or type commands."

    main_reply_markup = create_reply_keyboard_main_menu(user_id)
    try:
        bot.send_message(chat_id, welcome_msg_text, reply_markup=main_reply_markup, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error sending welcome to {user_id}: {e}")

def _logic_upload_file(message):
    """Upload file logic - only for subscribed users"""
    user_id = message.from_user.id
    
    if is_user_banned(user_id):
        bot.reply_to(message, "🚫 You are banned from uploading files.")
        return

    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "⚠️ Bot locked by admin, cannot accept files.")
        return

    if not is_user_subscribed(user_id) and user_id not in admin_ids:
        bot.reply_to(message, 
                    "❌ Only subscribed users can upload files.\n\n"
                    "💳 Please get a subscription first by clicking 'Get Subscription' button.",
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("💳 Get Subscription", callback_data='get_subscription')
                    ))
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
        elif is_user_subscribed(user_id):
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

def _logic_get_subscription(message):
    """Get subscription logic"""
    user_id = message.from_user.id
    
    if is_user_subscribed(user_id) and user_id not in admin_ids:
        expiry = user_subscriptions[user_id].get('expiry')
        days_left = (expiry - datetime.now()).days
        bot.reply_to(message, 
                    f"✅ You already have an active subscription!\n"
                    f"⏳ Expires in: {days_left} days\n"
                    f"📧 Check your email for activation code.",
                    reply_markup=create_main_menu_inline(user_id))
        return

    plans_text = "💳 Get Subscription\n\n"
    plans_text += "Select your plan:\n\n"
    for plan_id, plan in SUBSCRIPTION_PLANS.items():
        plans_text += f"• {plan['name']} - ₹{plan['price']}\n"
    
    plans_text += f"\n📱 UPI ID: `{payment_config.get('upi_id', 'yourupi@bank')}`\n\n"
    plans_text += "After payment, you'll need to provide:\n"
    plans_text += "• Your Email Address\n"
    plans_text += "• UTR Number\n"
    plans_text += "• Payment Screenshot"

    bot.send_message(message.chat.id, plans_text, 
                    reply_markup=create_subscription_request_menu(),
                    parse_mode='Markdown')

def _logic_my_subscription(message):
    """Show user's subscription details"""
    user_id = message.from_user.id
    
    if not is_user_subscribed(user_id):
        bot.reply_to(message, "❌ You don't have an active subscription.",
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("💳 Get Subscription", callback_data='get_subscription')
                    ))
        return
    
    subscription = user_subscriptions[user_id]
    expiry = subscription['expiry']
    days_left = (expiry - datetime.now()).days
    code = subscription.get('code', 'N/A')
    
    bot.reply_to(message,
                f"✅ Your Subscription Details:\n\n"
                f"📧 Email: Check your inbox\n"
                f"🔑 Activation Code: `{code}`\n"
                f"⏳ Expires: {expiry.strftime('%Y-%m-%d')}\n"
                f"📅 Days Left: {days_left}\n\n"
                f"⚠️ Note: The activation code was sent to your email.",
                parse_mode='Markdown')

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

def _logic_view_requests(message):
    """Handle view subscription requests from reply keyboard"""
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return

    requests = get_pending_requests()
    if not requests:
        bot.reply_to(message, "📋 No pending subscription requests.")
        return

    for req in requests[:5]:  # Show first 5
        req_id = req['id']
        created = datetime.fromisoformat(req['created_at']).strftime("%Y-%m-%d %H:%M")
        
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.row(
            types.InlineKeyboardButton("✅ Approve", callback_data=f"approve_{req_id}"),
            types.InlineKeyboardButton("❌ Reject", callback_data=f"reject_{req_id}")
        )
        markup.row(
            types.InlineKeyboardButton("👤 View User", callback_data=f"view_user_{req['user_id']}"),
            types.InlineKeyboardButton("📸 View Screenshot", callback_data=f"view_ss_{req_id}")
        )
        
        text = (f"💳 Request #{req_id}\n"
                f"👤 User: {req['user_name']} (`{req['user_id']}`)\n"
                f"📧 Email: {req['email']}\n"
                f"📅 Plan: {req['days']} days\n"
                f"💰 Amount: ₹{req['amount']}\n"
                f"🔢 UTR: {req['utr']}\n"
                f"🕐 Date: {created}")
        
        bot.send_message(message.chat.id, text, 
                        reply_markup=markup, parse_mode='Markdown')

# --- Button Text to Logic Mapping ---
BUTTON_TEXT_TO_LOGIC = {
    "📢 Updates Channel": lambda msg: bot.send_message(msg.chat.id, 
        "📢 Click below to join our Updates Channel 👇",
        reply_markup=types.InlineKeyboardMarkup().add(
            types.InlineKeyboardButton("📢 Join Now", url=inline_links['updates_channel'])
        )),
    "📤 Upload File": _logic_upload_file,
    "📂 Check Files": _logic_check_files,
    "⚡ Bot Speed": _logic_bot_speed,
    "📞 Contact Owner": _logic_contact_owner,
    "📊 Statistics": _logic_statistics,
    "💳 Get Subscription": _logic_get_subscription,
    "💳 My Subscription": _logic_my_subscription,
    "💳 Subscription Requests": _logic_view_requests,
    "📢 Broadcast": _logic_broadcast_init,
    "🔒 Lock Bot": _logic_toggle_lock_bot,
    "🟢 Running All Code": _logic_run_all_scripts,
    "👑 Admin Panel": _logic_admin_panel,
    "👥 User Management": lambda msg: user_management_button(msg),
    "💰 Payment Settings": lambda msg: payment_settings_button(msg),
    "🔗 Edit Links": lambda msg: edit_links_button(msg),
    "📝 View Problems": lambda msg: view_problems_button(msg),
    "📝 Submit Problem": lambda msg: submit_problem_button(msg)
}

# Button wrapper functions
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

@bot.message_handler(commands=['mysubscription'])
def command_my_subscription(message):
    """Check my subscription"""
    _logic_my_subscription(message)

@bot.message_handler(commands=['getsubscription'])
def command_get_subscription(message):
    """Get subscription"""
    _logic_get_subscription(message)

@bot.message_handler(commands=['redeem'])
def command_redeem(message):
    """Redeem activation code"""
    user_id = message.from_user.id
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "❌ Usage: /redeem <activation_code>")
        return
    
    code = parts[1].strip().upper()
    
    if is_user_subscribed(user_id):
        bot.reply_to(message, "✅ You already have an active subscription.")
        return
    
    success, result = redeem_code(user_id, code)
    if success:
        expiry = result
        bot.reply_to(message, 
                    f"✅ Subscription activated successfully!\n"
                    f"⏳ Expires on: {expiry.strftime('%Y-%m-%d')}\n"
                    f"📁 You can now upload files.")
    else:
        bot.reply_to(message, f"❌ {result}")

@bot.message_handler(commands=['updateschannel'])
def command_updates_channel(message): 
    bot.send_message(message.chat.id, 
        "📢 Click below to join our Updates Channel 👇",
        reply_markup=types.InlineKeyboardMarkup().add(
            types.InlineKeyboardButton("📢 Join Now", url=inline_links['updates_channel'])
        ))

@bot.message_handler(commands=['uploadfile'])
def command_upload_file(message): 
    _logic_upload_file(message)

@bot.message_handler(commands=['checkfiles'])
def command_check_files(message): 
    _logic_check_files(message)

@bot.message_handler(commands=['botspeed'])
def command_bot_speed(message): 
    _logic_bot_speed(message)

@bot.message_handler(commands=['contactowner'])
def command_contact_owner(message): 
    _logic_contact_owner(message)

@bot.message_handler(commands=['statistics'])
def command_statistics(message): 
    _logic_statistics(message)

@bot.message_handler(commands=['broadcast'])
def command_broadcast(message): 
    _logic_broadcast_init(message)

@bot.message_handler(commands=['lockbot'])
def command_lock_bot(message): 
    _logic_toggle_lock_bot(message)

@bot.message_handler(commands=['adminpanel'])
def command_admin_panel(message): 
    _logic_admin_panel(message)

@bot.message_handler(commands=['runningallcode'])
def command_run_all_code(message): 
    _logic_run_all_scripts(message)

@bot.message_handler(commands=['ping'])
def ping(message):
    start_ping_time = time.time()
    msg = bot.reply_to(message, "Pong!")
    latency = round((time.time() - start_ping_time) * 1000, 2)
    bot.edit_message_text(f"Pong! Latency: {latency} ms", message.chat.id, msg.message_id)

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

# --- Document Handler - Only for subscribed users ---
@bot.message_handler(content_types=['document'])
def handle_file_upload_doc(message):
    """Handle file uploads - only for subscribed users"""
    user_id = message.from_user.id

    if is_user_banned(user_id):
        bot.reply_to(message, "🚫 You are banned from uploading files.")
        return

    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "⚠️ Bot locked by admin, cannot accept files.")
        return

    # Check if user is subscribed
    if not is_user_subscribed(user_id) and user_id not in admin_ids:
        bot.reply_to(message, 
                    "❌ Only subscribed users can upload files.\n\n"
                    "💳 Please get a subscription first by clicking 'Get Subscription' button.",
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("💳 Get Subscription", callback_data='get_subscription')
                    ))
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

# --- Callback Query Handlers ---
@bot.callback_query_handler(func=lambda call: call.data == 'get_subscription')
def get_subscription_callback(call):
    bot.answer_callback_query(call.id)
    _logic_get_subscription(call.message)

@bot.callback_query_handler(func=lambda call: call.data == 'my_subscription')
def my_subscription_callback(call):
    bot.answer_callback_query(call.id)
    _logic_my_subscription(call.message)

@bot.callback_query_handler(func=lambda call: call.data.startswith('request_'))
def request_subscription_callback(call):
    """Handle plan selection for subscription"""
    user_id = call.from_user.id
    
    if is_user_subscribed(user_id):
        bot.answer_callback_query(call.id, "✅ You already have an active subscription!", show_alert=True)
        return
    
    plan_days = call.data.split('_')[1]
    if plan_days not in SUBSCRIPTION_PLANS:
        bot.answer_callback_query(call.id, "❌ Invalid plan selected.")
        return
    
    plan = SUBSCRIPTION_PLANS[plan_days]
    
    # Store plan in user data temporarily
    if not hasattr(bot, 'user_temp_data'):
        bot.user_temp_data = {}
    
    bot.user_temp_data[user_id] = {'plan': plan_days, 'amount': plan['price']}
    
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id,
                          f"📧 Please enter your email address:\n\n"
                          f"Selected Plan: {plan['name']}\n"
                          f"Amount: ₹{plan['price']}\n\n"
                          f"This email will receive the activation code.\n"
                          f"Type /cancel to abort.")
    bot.register_next_step_handler(msg, process_subscription_email)

def process_subscription_email(message):
    """Process email for subscription"""
    user_id = message.from_user.id
    
    if message.text and message.text.lower() == '/cancel':
        bot.reply_to(message, "Subscription request cancelled.")
        return
    
    email = message.text.strip()
    
    # Basic email validation
    if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
        bot.reply_to(message, "❌ Invalid email format. Please enter a valid email address:")
        bot.register_next_step_handler(message, process_subscription_email)
        return
    
    # Store email
    if not hasattr(bot, 'user_temp_data'):
        bot.user_temp_data = {}
    
    if user_id not in bot.user_temp_data:
        bot.user_temp_data[user_id] = {}
    
    bot.user_temp_data[user_id]['email'] = email
    
    msg = bot.reply_to(message,
                      f"✅ Email saved: {email}\n\n"
                      f"📱 Please send your payment screenshot with the UTR number.\n"
                      f"UPI ID: `{payment_config.get('upi_id', 'yourupi@bank')}`\n\n"
                      f"Send the screenshot as a photo or document.\n"
                      f"Type /cancel to abort.")
    bot.register_next_step_handler(msg, process_payment_screenshot)

def process_payment_screenshot(message):
    """Process payment screenshot for subscription"""
    user_id = message.from_user.id
    
    if message.text and message.text.lower() == '/cancel':
        bot.reply_to(message, "Subscription request cancelled.")
        if user_id in bot.user_temp_data:
            del bot.user_temp_data[user_id]
        return
    
    # Check if message contains photo or document
    if message.photo:
        # Get the largest photo
        file_id = message.photo[-1].file_id
    elif message.document:
        file_id = message.document.file_id
    else:
        msg = bot.reply_to(message, 
                          "❌ Please send a photo or document of your payment screenshot.\n"
                          "Type /cancel to abort.")
        bot.register_next_step_handler(msg, process_payment_screenshot)
        return
    
    # Store screenshot file_id
    if not hasattr(bot, 'user_temp_data'):
        bot.user_temp_data = {}
    
    if user_id not in bot.user_temp_data:
        bot.user_temp_data[user_id] = {}
    
    bot.user_temp_data[user_id]['screenshot'] = file_id
    
    msg = bot.reply_to(message,
                      "✅ Screenshot received!\n\n"
                      "📝 Please enter the UTR (Transaction Reference) number:\n"
                      "Type /cancel to abort.")
    bot.register_next_step_handler(msg, process_utr_number)

def process_utr_number(message):
    """Process UTR number for subscription"""
    user_id = message.from_user.id
    
    if message.text and message.text.lower() == '/cancel':
        bot.reply_to(message, "Subscription request cancelled.")
        if user_id in bot.user_temp_data:
            del bot.user_temp_data[user_id]
        return
    
    utr = message.text.strip()
    
    if len(utr) < 4:
        bot.reply_to(message, "❌ Invalid UTR number. Please enter a valid UTR:")
        bot.register_next_step_handler(message, process_utr_number)
        return
    
    # Get stored data
    if not hasattr(bot, 'user_temp_data') or user_id not in bot.user_temp_data:
        bot.reply_to(message, "❌ Session expired. Please start over.")
        return
    
    user_data = bot.user_temp_data[user_id]
    plan_days = user_data.get('plan')
    amount = user_data.get('amount')
    email = user_data.get('email')
    screenshot = user_data.get('screenshot')
    
    if not all([plan_days, amount, email, screenshot]):
        bot.reply_to(message, "❌ Missing information. Please start over.")
        if user_id in bot.user_temp_data:
            del bot.user_temp_data[user_id]
        return
    
    # Save subscription request
    request_id = save_subscription_request(
        user_id=user_id,
        user_name=message.from_user.first_name,
        email=email,
        days=int(plan_days),
        amount=amount,
        utr=utr,
        screenshot_file_id=screenshot
    )
    
    if request_id:
        # Forward screenshot to admins
        caption = (f"💳 New Subscription Request #{request_id}\n"
                  f"👤 User: {message.from_user.first_name} (`{user_id}`)\n"
                  f"📧 Email: {email}\n"
                  f"📅 Plan: {plan_days} days\n"
                  f"💰 Amount: ₹{amount}\n"
                  f"🔢 UTR: {utr}")
        
        for admin_id in admin_ids:
            try:
                if message.photo:
                    bot.send_photo(admin_id, screenshot, caption=caption, parse_mode='Markdown')
                else:
                    bot.send_document(admin_id, screenshot, caption=caption, parse_mode='Markdown')
            except Exception as e:
                logger.error(f"Failed to forward to admin {admin_id}: {e}")
        
        bot.reply_to(message,
                    f"✅ Subscription request #{request_id} submitted successfully!\n\n"
                    f"Your request is pending admin approval.\n"
                    f"You will receive an email with the activation code once approved.\n"
                    f"Then use /redeem <code> to activate your subscription.")
    else:
        bot.reply_to(message, "❌ Failed to submit request. Please try again.")
    
    # Clear temporary data
    if user_id in bot.user_temp_data:
        del bot.user_temp_data[user_id]

@bot.callback_query_handler(func=lambda call: call.data == 'view_requests')
def view_requests_callback(call):
    """View pending subscription requests (inline)"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    requests = get_pending_requests()
    
    if not requests:
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, "📋 No pending subscription requests.")
        return
    
    bot.answer_callback_query(call.id)
    
    for req in requests[:5]:
        req_id = req['id']
        created = datetime.fromisoformat(req['created_at']).strftime("%Y-%m-%d %H:%M")
        
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.row(
            types.InlineKeyboardButton("✅ Approve", callback_data=f"approve_{req_id}"),
            types.InlineKeyboardButton("❌ Reject", callback_data=f"reject_{req_id}")
        )
        markup.row(
            types.InlineKeyboardButton("👤 View User", callback_data=f"view_user_{req['user_id']}"),
            types.InlineKeyboardButton("📸 View Screenshot", callback_data=f"view_ss_{req_id}")
        )
        
        text = (f"💳 Request #{req_id}\n"
                f"👤 User: {req['user_name']} (`{req['user_id']}`)\n"
                f"📧 Email: {req['email']}\n"
                f"📅 Plan: {req['days']} days\n"
                f"💰 Amount: ₹{req['amount']}\n"
                f"🔢 UTR: {req['utr']}\n"
                f"🕐 Date: {created}")
        
        bot.send_message(call.message.chat.id, text, 
                        reply_markup=markup, parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data.startswith('view_ss_'))
def view_screenshot_callback(call):
    """View payment screenshot"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    try:
        req_id = int(call.data.split('_')[2])
        
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute('SELECT screenshot_file_id FROM subscription_requests WHERE id = ?', (req_id,))
        result = c.fetchone()
        conn.close()
        
        if result and result[0]:
            bot.answer_callback_query(call.id)
            bot.send_photo(call.message.chat.id, result[0], 
                          caption=f"📸 Payment Screenshot for Request #{req_id}")
        else:
            bot.answer_callback_query(call.id, "❌ Screenshot not found.", show_alert=True)
    except Exception as e:
        logger.error(f"Error viewing screenshot: {e}")
        bot.answer_callback_query(call.id, "❌ Error retrieving screenshot.", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith('approve_'))
def approve_request_callback(call):
    """Approve subscription request: generate code, send email"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    try:
        req_id = int(call.data.split('_')[1])
        
        # Approve request
        code, email, user_id = approve_subscription_request(req_id, call.from_user.id)
        
        if code and email and user_id:
            # Send activation email
            if send_activation_email(email, code):
                # Notify admin that email was sent
                bot.answer_callback_query(call.id, f"✅ Request #{req_id} approved! Email sent.", show_alert=True)
                bot.send_message(call.message.chat.id,
                               f"✅ Request #{req_id} approved.\n"
                               f"Activation code `{code}` sent to {email}.")
                
                # Notify user via Telegram (optional)
                try:
                    bot.send_message(user_id,
                                   f"✅ Your payment has been verified!\n\n"
                                   f"An activation code has been sent to your email: {email}\n"
                                   f"Please check your inbox (and spam folder).\n"
                                   f"Then use /redeem <code> to activate your subscription.")
                except:
                    pass
            else:
                bot.answer_callback_query(call.id, f"⚠️ Code generated but email failed. Code: {code}", show_alert=True)
                bot.send_message(call.message.chat.id,
                               f"⚠️ Request #{req_id} approved but email failed.\n"
                               f"Activation code: `{code}`\n"
                               f"Please send it manually to {email}.")
            
            # Update the request message
            bot.edit_message_text(f"✅ Request #{req_id} has been approved and email sent.",
                                 call.message.chat.id,
                                 call.message.message_id)
        else:
            bot.answer_callback_query(call.id, "❌ Failed to approve request. Check logs.", show_alert=True)
            logger.error(f"Approval failed for request {req_id} - returned None.")
            
    except Exception as e:
        logger.error(f"Error approving request: {e}", exc_info=True)
        bot.answer_callback_query(call.id, f"❌ Error: {str(e)[:50]}", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith('reject_'))
def reject_request_callback(call):
    """Reject subscription request"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    try:
        req_id = int(call.data.split('_')[1])
        
        # Ask for rejection reason
        msg = bot.send_message(call.message.chat.id,
                              f"Please enter reason for rejecting request #{req_id}:\n"
                              f"Type /cancel to abort.")
        bot.register_next_step_handler(msg, process_rejection_reason, req_id, call)
        
    except Exception as e:
        logger.error(f"Error in reject callback: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "❌ Error.", show_alert=True)

def process_rejection_reason(message, req_id, original_call):
    """Process rejection reason"""
    if message.text and message.text.lower() == '/cancel':
        bot.reply_to(message, "Rejection cancelled.")
        return
    
    reason = message.text.strip()
    
    if reject_subscription_request(req_id, message.from_user.id, reason):
        # Get user_id from request
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute('SELECT user_id FROM subscription_requests WHERE id = ?', (req_id,))
        result = c.fetchone()
        conn.close()
        
        if result:
            user_id = result[0]
            try:
                bot.send_message(user_id,
                               f"❌ Your subscription request #{req_id} was rejected.\n"
                               f"Reason: {reason}\n\n"
                               f"Please contact admin if you have questions.")
            except Exception as e:
                logger.error(f"Failed to notify user {user_id}: {e}")
        
        bot.reply_to(message, f"✅ Request #{req_id} rejected.\nReason: {reason}")
        
        # Update original message
        bot.edit_message_text(f"❌ Request #{req_id} has been rejected.\nReason: {reason}",
                            original_call.message.chat.id,
                            original_call.message.message_id)
    else:
        bot.reply_to(message, f"❌ Failed to reject request #{req_id}. It may have been already processed.")

# ... (include all other callback handlers from the original code, such as upload, check_files, file control, etc.) ...

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
    logger.info("="*40 + "\n🤖 Subscription-Based Bot Hosting Starting...\n" +
                f"📱 Payment: {'Enabled' if payment_config.get('qr_code_enabled', True) else 'Disabled'}\n" +
                f"🚫 Banned Users: {len(banned_users)}\n" +
                f"💳 Pending Requests: {len(pending_requests)}\n" + "="*40)

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