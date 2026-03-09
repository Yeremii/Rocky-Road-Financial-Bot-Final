#!/usr/bin/env python3
"""
Rocky Road Financial Bot
A Discord bot for managing monthly invoicing for a transportation company.
"""

import discord
from discord import app_commands
from discord.ext import commands, tasks
import os
import asyncio
import logging
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime, timezone, timedelta, time
import gspread
from google.oauth2.service_account import Credentials
import aiosqlite
from typing import Optional, List, Dict, Any
import re
import unicodedata
from zoneinfo import ZoneInfo
import shutil
import json
from aiohttp import web
import secrets
import aiohttp

# Setup logging — writes to console AND bot.log (max 5MB, keeps 3 backups)
from logging.handlers import RotatingFileHandler

_log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

_file_handler = RotatingFileHandler('bot.log', maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')
_file_handler.setFormatter(_log_formatter)

_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_log_formatter)

logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _console_handler])
logger = logging.getLogger('RockyRoadBot')

# Reduce noise from third-party libraries in the console
logging.getLogger('discord').setLevel(logging.ERROR)
logging.getLogger('discord.gateway').setLevel(logging.ERROR)


# ══════════════════════════════════════════════════════════════════════════════
# WEBHOOK LOGGER — sends rich embeds to dedicated Discord log channels
# ══════════════════════════════════════════════════════════════════════════════
class WebhookLogger:
    """Async webhook logger. Fire-and-forget — never raises, never blocks."""

    @staticmethod
    async def _send(url: str, payload: dict):
        try:
            async with aiohttp.ClientSession() as session:
                await session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=5))
        except Exception as e:
            logger.warning(f"Webhook send failed: {e}")

    @staticmethod
    def _ts() -> str:
        """Current timestamp in EST, formatted."""
        from zoneinfo import ZoneInfo as _ZI
        return datetime.now(_ZI('America/New_York')).strftime('%Y-%m-%d %H:%M:%S EST')

    @classmethod
    async def invoice_sent(cls, name: str, period: str, total_due: float, previous_debt: float = 0, dm_ok: bool = True):
        debt_line = f"\n> 📦 Older debt included: **${previous_debt:,.0f}**" if previous_debt > 0 else ""
        status = "✅ DM delivered" if dm_ok else "❌ DM failed"
        await cls._send(WH_INVOICE, {"embeds": [{"title": "📄 Invoice Sent", "description":
            f"> 👤 **{name}**\n> 📅 Period: **{period}**\n> 💰 Total Due: **${total_due:,.0f}**{debt_line}\n> {status}",
            "color": 0x5865F2, "footer": {"text": cls._ts()}}]})

    @classmethod
    async def invoice_batch(cls, period: str, success: int, failed: int, total_owed: float, not_found: list = None):
        nf = f"\n> ⚠️ Not found: {', '.join(not_found[:10])}" if not_found else ""
        await cls._send(WH_INVOICE, {"embeds": [{"title": "📨 Invoice Batch Complete", "description":
            f"> 📅 Period: **{period}**\n> ✅ Sent: **{success}** | ❌ Failed: **{failed}**\n> 💵 Total owed: **${total_owed:,.0f}**{nf}",
            "color": 0x57F287 if failed == 0 else 0xFEE75C, "footer": {"text": cls._ts()}}]})

    @classmethod
    async def payment_submitted(cls, name: str, period: str, total_due: float, screenshot_url: str):
        await cls._send(WH_INVOICE, {"embeds": [{"title": "💳 Payment Submitted", "description":
            f"> 👤 **{name}**\n> 📅 Period: **{period}**\n> 💰 Amount: **${total_due:,.0f}**\n> 🖼️ [Screenshot]({screenshot_url})",
            "color": 0xEB459E, "footer": {"text": cls._ts()}}]})

    @classmethod
    async def payment_verified(cls, name: str, period: str, total_due: float, verified_by: str):
        await cls._send(WH_INVOICE, {"embeds": [{"title": "✅ Payment Verified", "description":
            f"> 👤 **{name}**\n> 📅 Period: **{period}**\n> 💰 Amount: **${total_due:,.0f}**\n> 🔑 Verified by: **{verified_by}**",
            "color": 0x57F287, "footer": {"text": cls._ts()}}]})

    @classmethod
    async def manual_paid(cls, target: str, admin: str, period: str = "All pending"):
        await cls._send(WH_ADMIN, {"embeds": [{"title": "💳 Manual Paid", "description":
            f"> 👤 **{target}** marked as paid\n> 📅 Period: **{period}**\n> 🔑 By: **{admin}**",
            "color": 0x57F287, "footer": {"text": cls._ts()}}]})

    @classmethod
    async def admin_action(cls, admin: str, command: str, details: str = ""):
        await cls._send(WH_ADMIN, {"embeds": [{"title": f"🛡️ Admin: /{command}", "description":
            f"> 👤 **{admin}**\n> 📋 {details}" if details else f"> 👤 **{admin}**",
            "color": 0xED4245, "footer": {"text": cls._ts()}}]})

    @classmethod
    async def sync_complete(cls, period: str, staff_count: int, debt_rollovers: list = None, skipped: list = None):
        rollovers = ""
        if debt_rollovers:
            rollovers = "\n> 💸 Debt rolled:\n" + "\n".join(f"> • {n}: ${a:,.0f}" for n, a in debt_rollovers[:10])
        skips = f"\n> ⚠️ Not found: {', '.join(skipped[:5])}" if skipped else ""
        await cls._send(WH_SYNC, {"embeds": [{"title": "🔄 Weekly Sync Complete", "description":
            f"> 📅 Period: **{period}**\n> 👥 Staff synced: **{staff_count}**{rollovers}{skips}",
            "color": 0x5865F2, "footer": {"text": cls._ts()}}]})

    @classmethod
    async def backup_saved(cls, filename: str, size_kb: float, file_path: str = None):
        await cls._send(WH_BACKUP, {"embeds": [{"title": "💾 DB Backup Saved", "description":
            f"> 📁 **{filename}**\n> 📦 Size: **{size_kb:.1f} KB**",
            "color": 0x5865F2, "footer": {"text": cls._ts()}}]})

    @classmethod
    async def backup_file(cls, file_path: str):
        """Send the actual .db file as attachment to backup channel."""
        try:
            import aiofiles
            webhook_url = WH_BACKUP
            async with aiohttp.ClientSession() as session:
                async with aiofiles.open(file_path, 'rb') as f:
                    data = aiohttp.FormData()
                    data.add_field('file', await f.read(), filename=os.path.basename(file_path), content_type='application/octet-stream')
                    data.add_field('payload_json', '{"content": "📦 **DB Backup** — ' + cls._ts() + '"}')
                    await session.post(webhook_url, data=data, timeout=aiohttp.ClientTimeout(total=30))
        except Exception as e:
            logger.warning(f"Backup file webhook failed: {e}")

    @classmethod
    async def error(cls, context: str, error: str, traceback_str: str = ""):
        tb = f"\n```\n{traceback_str[:800]}\n```" if traceback_str else ""
        await cls._send(WH_ERROR, {"embeds": [{"title": "❌ Bot Error", "description":
            f"> 📍 **{context}**\n> ⚠️ `{str(error)[:300]}`{tb}",
            "color": 0xED4245, "footer": {"text": cls._ts()}}]})

whl = WebhookLogger()
logging.getLogger('discord.http').setLevel(logging.ERROR)
logging.getLogger('gspread').setLevel(logging.ERROR)
logging.getLogger('google').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)
logging.getLogger('requests').setLevel(logging.ERROR)

# Load environment variables
ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# Configuration
DISCORD_TOKEN = os.environ.get('DISCORD_BOT_TOKEN')
DRIVER_SHEET_ID = os.environ.get('DRIVER_TRACKER_SHEET_ID')
TRAINER_SHEET_ID = os.environ.get('TRAINER_TRACKER_SHEET_ID')
MANAGEMENT_SHEET_ID = os.environ.get('MANAGEMENT_TRACKER_SHEET_ID', '1eJ_JI-3bHP0uJb_sGtXsSOrr6gKIhih_RWNY97gIfbA')
PAYROLL_CHANNEL_ID = int(os.environ.get('PAYROLL_CHANNEL_ID', '1467222984367538417'))
DRIVER_COMMISSION = float(os.environ.get('DRIVER_COMMISSION_RATE', '0.20'))
TRAINER_COMMISSION = float(os.environ.get('TRAINER_COMMISSION_RATE', '0.15'))
MANAGEMENT_COMMISSION = float(os.environ.get('MANAGEMENT_COMMISSION_RATE', '0.10'))
MINIMUM_FEE = float(os.environ.get('MINIMUM_FEE', '5000'))
BOT_DESCRIPTION = os.environ.get('BOT_DESCRIPTION', 'Rocky Road Financial Bot helps manage weekly invoices and payments. Use /pay to submit your payment proof after receiving your invoice.\n\nMade by notyeremi')
CLEAR_MODE = os.environ.get('CLEAR_MODE', 'weekly').lower()

# Authorized roles (case-insensitive)
AUTHORIZED_ROLES = ['owner', 'investors', 'chief financial officer', 'operations manager', 'admin']

# LOA Role ID - members with this role won't be charged
LOA_ROLE_ID = int(os.environ.get('LOA_ROLE_ID', '1194000225401049098'))

# ── Brand colors ────────────────────────────────────────────────────────────
class Colors:
    PRIMARY   = 0x1E1F22   # dark embed background tone
    SUCCESS   = 0x2ECC71   # green  – paid / confirmed
    WARNING   = 0xE67E22   # orange – overdue / reminder
    ERROR     = 0xE74C3C   # red    – errors / access denied
    INFO      = 0x5865F2   # blurple – informational
    GOLD      = 0xF1C40F   # gold   – leaderboard / revenue
    INVOICE   = 0x2F3136   # near-black – invoice DMs

# ── Bot branding ─────────────────────────────────────────────────────────────
BOT_THUMBNAIL = os.environ.get('BOT_THUMBNAIL', 'https://cdn.discordapp.com/attachments/1083854427703742565/1366522487218901183/Rocky_Road_Logo.png.png?ex=699b713d&is=699a1fbd&hm=abf29de10db9bdf2dbb12460be952bfb66001e2625604c6853465e1a08b05918&')   # optional logo URL
ADMIN_ERROR_DM_ID = int(os.environ.get('ADMIN_ERROR_DM_ID', '0'))  # user ID to DM on auto-invoice failure

# ── Webhook URLs ──────────────────────────────────────────────────────────────
WH_INVOICE = "https://discord.com/api/webhooks/1480360888421711952/aJ1JVO1MGT3zC8WZknPUPWhF6AyrdUB73tX0vT-zGMFxuDFN9bXXtLlN6NDwEztyWN8c"
WH_BACKUP  = "https://discord.com/api/webhooks/1480361890730213436/xjXb9cHc2rObhgW0zeFDcaaGC0tkf54l3-FOUpO5dQnCcf3zu5CFQ0qP_wwBDbxvTjgh"
WH_ERROR   = "https://discord.com/api/webhooks/1480362268938993849/Juhph1jTpzm4ilaiW5r5IPHjxhwDBn9MVhl1HVipRtvCqvel8LvsRY2BCCRq143Y6LaD"
WH_ADMIN   = "https://discord.com/api/webhooks/1480362472803139765/HxNBcfwq2_1jvcPWix4iLCKq8N8YOqDMUlAzq-H7nozPLDEs1lm84YttfnSwUSSHC9N0"
WH_SYNC    = "https://discord.com/api/webhooks/1480362675501400218/4OCcUl4MGT4Kmejy-OVl8zIzPRpNeVHk_0N4vWbzX20KUQEIX5tSI2tw97OUHx4-JrTQ"

# Database path
DB_PATH = ROOT_DIR / 'rocky_road.db'

# Google Sheets setup
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]


class Database:
    """SQLite database handler for payment tracking."""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
    
    async def initialize(self):
        """Create database tables if they don't exist."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                CREATE TABLE IF NOT EXISTS invoices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    discord_user_id TEXT NOT NULL,
                    discord_username TEXT,
                    month TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    staff_type TEXT NOT NULL,
                    revenue REAL DEFAULT 0,
                    commission_amount REAL DEFAULT 0,
                    profit_from_sheet REAL DEFAULT 0,
                    previous_debt REAL DEFAULT 0,
                    total_due REAL NOT NULL,
                    status TEXT DEFAULT 'Pending',
                    created_at TEXT NOT NULL,
                    paid_at TEXT,
                    verified_by TEXT,
                    dm_sent BOOLEAN DEFAULT 0,
                    UNIQUE(discord_user_id, month, year)
                )
            ''')
            
            await db.execute('''
                CREATE TABLE IF NOT EXISTS payment_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    invoice_id INTEGER,
                    discord_user_id TEXT NOT NULL,
                    screenshot_url TEXT,
                    message_id TEXT,
                    submitted_at TEXT NOT NULL,
                    verified BOOLEAN DEFAULT 0,
                    verified_by TEXT,
                    verified_at TEXT,
                    FOREIGN KEY (invoice_id) REFERENCES invoices(id)
                )
            ''')
            
            # New tables for Dashboard
            await db.execute('''
                CREATE TABLE IF NOT EXISTS vehicles (
                    id TEXT PRIMARY KEY,
                    current_user_id TEXT,
                    current_user_name TEXT,
                    inventory_json TEXT DEFAULT '{}'
                )
            ''')

            await db.execute('''
                CREATE TABLE IF NOT EXISTS user_roles (
                    discord_user_id TEXT PRIMARY KEY,
                    role TEXT DEFAULT 'driver'
                )
            ''')

            await db.execute('''
                CREATE TABLE IF NOT EXISTS activity_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    type TEXT NOT NULL, -- payment, invoice, status, system, vehicle
                    created_at TEXT NOT NULL
                )
            ''')

            # Migrations — add columns that may be missing in older DBs
            try:
                await db.execute("ALTER TABLE invoices ADD COLUMN profit_from_sheet REAL DEFAULT 0")
                await db.commit()
            except Exception:
                pass  # Column already exists
            try:
                await db.execute("ALTER TABLE invoices ADD COLUMN dm_sent BOOLEAN DEFAULT 0")
                await db.commit()
            except Exception:
                pass  # Column already exists

            # Initialize 20 vehicles if empty
            cursor = await db.execute("SELECT COUNT(*) FROM vehicles")
            count = await cursor.fetchone()
            if count[0] == 0:
                for i in range(1, 21):
                    v_id = f"RR{i:02d}"
                    initial_inv = json.dumps({
                        "Jerky (5x)": True,
                        "Buckshot Juices (5x)": True,
                        "Repair Kits (2x)": True,
                        "Tire Repair Kits (2x)": True,
                        "Fire Extinguisher (1x)": True,
                        "Jerry Can (1x)": True,
                        "Traffic Cones (5x)": True
                    })
                    await db.execute("INSERT INTO vehicles (id, inventory_json) VALUES (?, ?)", (v_id, initial_inv))
            
            await db.commit()

    async def get_all_vehicles(self):
        """Get all vehicles from the database."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM vehicles")
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def update_vehicle(self, v_id: str, user_id: str, user_name: str, inventory: dict):
        """Update vehicle status and inventory."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                UPDATE vehicles 
                SET current_user_id = ?, current_user_name = ?, inventory_json = ?
                WHERE id = ?
            ''', (user_id, user_name, json.dumps(inventory), v_id))
            await db.commit()

    async def get_user_role(self, discord_user_id: str):
        """Get user role from the database."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT role FROM user_roles WHERE discord_user_id = ?", (discord_user_id,))
            row = await cursor.fetchone()
            return row[0] if row else 'driver'

    async def set_user_role(self, discord_user_id: str, role: str):
        """Set user role in the database."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("INSERT OR REPLACE INTO user_roles (discord_user_id, role) VALUES (?, ?)", (discord_user_id, role))
            await db.commit()

    async def log_activity(self, action: str, subject: str, activity_type: str):
        """Log an activity to the database."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                INSERT INTO activity_log (action, subject, type, created_at)
                VALUES (?, ?, ?, ?)
            ''', (action, subject, activity_type, datetime.now(timezone.utc).isoformat()))
            await db.commit()

    async def get_recent_activities(self, limit: int = 20):
        """Get recent activities from the database."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM activity_log ORDER BY created_at DESC LIMIT ?", (limit,))
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_paid_this_month(self, year: int, month: int) -> list:
        """Return all PAID invoices for a month — used for leaderboard.
        Returns list of {discord_username, total_due} sorted by total_due DESC.
        """
        month_name = datetime(year, month, 1).strftime('%B')
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT discord_username, SUM(total_due) as total_due
                FROM invoices
                WHERE month LIKE ? AND year = ?
                  AND status = 'Paid'
                  AND staff_type IN ('driver', 'trainer', 'management')
                GROUP BY discord_username
                ORDER BY total_due DESC
            ''', (f'%{month_name}%', year))
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

    async def get_top_earners(self, month: str, year: int, limit: int = 10) -> list:
        """Get top earners for a specific period."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT discord_username, SUM(revenue) as revenue
                FROM invoices 
                WHERE month = ? AND year = ?
                GROUP BY discord_username
                ORDER BY revenue DESC
                LIMIT ?
            ''', (month, year, limit))
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_recent_periods(self, limit: int = 10) -> list:
        """Get list of recent invoice periods — only real weeks, no Manual Add/Import junk."""
        import re as _re
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute('''
                SELECT DISTINCT month, year FROM invoices
                WHERE staff_type IN ('driver', 'trainer', 'management')
                  AND discord_user_id NOT LIKE 'MANUAL_ADD_%'
                  AND discord_user_id NOT LIKE 'MANUAL_IMPORT_%'
                  AND month LIKE 'Week %'
                ORDER BY year DESC, month DESC
                LIMIT ?
            ''', (limit,))
            rows = await cursor.fetchall()
            # Sort by week number descending
            def week_sort(row):
                m = _re.search(r'Week (\d+)', row[0])
                return (row[1], int(m.group(1)) if m else 0)
            sorted_rows = sorted(rows, key=week_sort, reverse=True)
            seen = set()
            results = []
            for row in sorted_rows:
                key = (row[0], row[1])
                if key not in seen:
                    seen.add(key)
                    results.append({'month': row[0], 'year': row[1]})
            return results

    async def get_all_invoices_for_period(self, period_id: str, year: int) -> list:
        """Get real invoices for a specific period (Paid and Pending).
        Excludes MANUAL_ADD_, MANUAL_IMPORT_ fake IDs and manual_add/manual_import staff types
        which are legacy migration artifacts that would double-count revenue.
        MIGRATED_ IDs are included — they are the only record for some people in older weeks.
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT * FROM invoices 
                WHERE month = ? AND year = ?
                  AND staff_type IN ('driver', 'trainer', 'management', 'Manual')
                  AND discord_user_id NOT LIKE 'MANUAL_ADD_%'
                  AND discord_user_id NOT LIKE 'MANUAL_IMPORT_%'
                  AND (
                      -- Keep real numeric IDs and MIGRATED_ IDs
                      -- Exclude MANUAL_DANIELRYDER style fake IDs (no underscore after MANUAL)
                      discord_user_id NOT LIKE 'MANUAL_%'
                      OR discord_user_id LIKE 'MIGRATED_%'
                  )
                ORDER BY 
                    CASE WHEN status = 'Paid' THEN 1 ELSE 0 END, 
                    discord_username
            ''', (period_id, year))
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def delete_invoice(self, invoice_id: int):
        """Delete an invoice from the database."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM invoices WHERE id = ?", (invoice_id,))
            await db.commit()

    async def update_invoice_period_amount(self, invoice_id: int, period: str, amount: float):
        """Update an invoice's period and amount."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                UPDATE invoices 
                SET month = ?, total_due = ?
                WHERE id = ?
            ''', (period, amount, invoice_id))
            await db.commit()

    async def toggle_invoice_status(self, invoice_id: int, status: str):
        """Toggle an invoice's status between Paid and Pending."""
        async with aiosqlite.connect(self.db_path) as db:
            if status.lower() == 'paid':
                await db.execute('''
                    UPDATE invoices 
                    SET status = 'Paid', paid_at = ?, verified_by = 'Dashboard'
                    WHERE id = ?
                ''', (datetime.now(timezone.utc).isoformat(), invoice_id))
            else:
                await db.execute('''
                    UPDATE invoices 
                    SET status = 'Pending', paid_at = NULL, verified_by = NULL
                    WHERE id = ?
                ''', (invoice_id,))
            await db.commit()

    async def get_monthly_revenue(self, year: int, month: int) -> Dict[str, float]:
        """Get total revenue per user for a specific month (aggregating all invoices)."""
        month_name = datetime(year, month, 1).strftime('%B')
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute('''
                SELECT discord_username, COALESCE(SUM(revenue), 0) as total_rev
                FROM invoices 
                WHERE month LIKE ? AND year = ?
                GROUP BY discord_username
            ''', (f"%{month_name}%", year)) as cursor:
                rows = await cursor.fetchall()
                return {row[0]: float(row[1] or 0) for row in rows if row[0]}

    async def get_monthly_revenue_excluding_period(self, year: int, month: int, exclude_period: str) -> Dict[str, float]:
        """Get total revenue per user for a month, excluding the current active period (to avoid double-counting with live sheet data)."""
        month_name = datetime(year, month, 1).strftime('%B')
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute('''
                SELECT discord_username, COALESCE(SUM(revenue), 0) as total_rev
                FROM invoices 
                WHERE month LIKE ? AND year = ? AND month != ?
                GROUP BY discord_username
            ''', (f"%{month_name}%", year, exclude_period)) as cursor:
                rows = await cursor.fetchall()
                return {row[0]: float(row[1] or 0) for row in rows if row[0]}

    async def get_all_period_revenues_this_month(self, year: int, month: int) -> dict:
        """Return revenue grouped by period then by person for all periods in a given month.

        Returns: { period_id: { normalized_name: {'name': str, 'revenue': float} } }

        Rules:
        - Only staff_type: driver, trainer, management (weekly invoices only)
        - manual_add records are debt adjustments, NOT revenue — excluded
        - Excludes MANUAL_IMPORT_* (legacy migration duplicates of real weekly records)
        - Excludes MANUAL_DANIELRYDER style fake IDs (old migration artifacts)
        - Accepts real numeric Discord IDs and MIGRATED_* IDs
        - Merges small-caps and normal name variants via normalize_name key
        """
        month_name = datetime(year, month, 1).strftime('%B')
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute('''
                SELECT month, discord_username, discord_user_id, revenue
                FROM invoices
                WHERE month LIKE ? AND year = ?
                  AND staff_type IN ('driver', 'trainer', 'management')
                  AND discord_user_id NOT LIKE 'MANUAL_IMPORT_%'
                  AND NOT (
                      discord_user_id LIKE 'MANUAL_%'
                      AND discord_user_id NOT LIKE 'MIGRATED_%'
                  )
                  AND revenue > 0
            ''', (f"%{month_name}%", year)) as cursor:
                rows = await cursor.fetchall()

        # Group by period, then by normalized name — SUM all revenue for same person
        result = {}
        for period, username, user_id, rev in rows:
            if not username or not user_id:
                continue
            if period not in result:
                result[period] = {}
            key = normalize_name(username)
            rev_float = float(rev or 0)
            if key in result[period]:
                result[period][key]['revenue'] += rev_float
                # Prefer clean ASCII name over small-caps unicode
                if username.isascii() and not result[period][key]['name'].isascii():
                    result[period][key]['name'] = username
            else:
                result[period][key] = {'name': username, 'revenue': rev_float}
        return result

    async def get_invoice_by_id(self, invoice_id: int):
        """Get a single invoice by ID."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM invoices WHERE id = ?", (invoice_id,))
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_pending_invoices_for_period(self, month: str, year: int) -> list:
        """Get all pending invoices for a specific period (all staff types)."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT * FROM invoices 
                WHERE month = ? AND year = ? AND status = 'Pending'
                  AND staff_type IN ('driver', 'trainer', 'management')
                ORDER BY discord_username
            ''', (month, year))
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_all_pending_invoices(self) -> list:
        """Get all pending invoices."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT * FROM invoices WHERE status = 'Pending' ORDER BY created_at DESC
            ''')
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_user_invoices(self, discord_user_id: str) -> list:
        """Get all invoices for a user."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT * FROM invoices WHERE discord_user_id = ? ORDER BY year DESC, id DESC
            ''', (discord_user_id,))
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_paid_invoices_by_username(self, username: str) -> list:
        """Get all paid invoices for a specific username."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT * FROM invoices 
                WHERE discord_username = ? AND status = 'Paid'
                ORDER BY year DESC, id DESC
            ''', (username,))
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_invoice_status_for_period(self, discord_user_id: str, month: str, year: int) -> Optional[str]:
        """Return invoice status for a given period, or None if not exists."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT status FROM invoices 
                WHERE discord_user_id = ? AND month = ? AND year = ?
                ORDER BY id DESC LIMIT 1
            ''', (discord_user_id, month, year))
            row = await cursor.fetchone()
            return row['status'] if row else None

    async def get_invoice_for_period(self, discord_user_id: str, month: str, year: int, status: Optional[str] = None):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            if status:
                cursor = await db.execute('''
                    SELECT * FROM invoices 
                    WHERE discord_user_id = ? AND month = ? AND year = ? AND status = ?
                    ORDER BY id DESC LIMIT 1
                ''', (discord_user_id, month, year, status))
            else:
                cursor = await db.execute('''
                    SELECT * FROM invoices 
                    WHERE discord_user_id = ? AND month = ? AND year = ?
                    ORDER BY id DESC LIMIT 1
                ''', (discord_user_id, month, year))
            return await cursor.fetchone()

    async def save_invoice(self, discord_user_id: str, discord_username: str, month: str, year: int,
                           staff_type: str, revenue: float, commission_amount: float,
                           profit_from_sheet: float, previous_debt: float, total_due: float) -> int:
        """Save or update an invoice, returning its ID."""
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.now(timezone.utc).isoformat()
            cursor = await db.execute('''
                INSERT INTO invoices 
                    (discord_user_id, discord_username, month, year, staff_type, revenue,
                     commission_amount, profit_from_sheet, previous_debt, total_due, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Pending', ?)
                ON CONFLICT(discord_user_id, month, year) DO UPDATE SET
                    discord_username = excluded.discord_username,
                    revenue = excluded.revenue,
                    commission_amount = excluded.commission_amount,
                    profit_from_sheet = excluded.profit_from_sheet,
                    previous_debt = excluded.previous_debt,
                    total_due = excluded.total_due,
                    staff_type = excluded.staff_type
            ''', (discord_user_id, discord_username, month, year, staff_type, revenue,
                  commission_amount, profit_from_sheet, previous_debt, total_due, now))
            await db.commit()
            if cursor.lastrowid:
                return cursor.lastrowid
            row = await (await db.execute(
                "SELECT id FROM invoices WHERE discord_user_id=? AND month=? AND year=?",
                (discord_user_id, month, year)
            )).fetchone()
            return row[0] if row else -1

    async def mark_as_paid(self, invoice_id: int, verified_by: str):
        """Mark an invoice as paid."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                UPDATE invoices 
                SET status = 'Paid', paid_at = ?, verified_by = ?
                WHERE id = ?
            ''', (datetime.now(timezone.utc).isoformat(), verified_by, invoice_id))
            await db.commit()

    async def mark_manual_adjustments_paid(self, discord_user_id: str, verified_by: str):
        """Mark all pending manual adjustments as paid for a user."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                UPDATE invoices 
                SET status = 'Paid', paid_at = ?, verified_by = ?
                WHERE discord_user_id = ? AND status = 'Pending' AND staff_type = 'Manual'
            ''', (datetime.now(timezone.utc).isoformat(), verified_by, discord_user_id))
            await db.commit()

    async def mark_all_pending_invoices_paid(self, discord_user_id: str, verified_by: str, period: str = None, year: int = None):
        """Mark pending invoices as paid for a user.
        If period+year provided: marks only that specific week.
        If no period: marks ALL pending (used after payment verification — debt already rolled up).
        """
        async with aiosqlite.connect(self.db_path) as db:
            if period and year:
                await db.execute(
                    "UPDATE invoices SET status = 'Paid', paid_at = ?, verified_by = ? WHERE discord_user_id = ? AND status = 'Pending' AND month = ? AND year = ?",
                    [datetime.now(timezone.utc).isoformat(), verified_by, discord_user_id, period, year]
                )
            else:
                await db.execute(
                    "UPDATE invoices SET status = 'Paid', paid_at = ?, verified_by = ? WHERE discord_user_id = ? AND status = 'Pending'",
                    [datetime.now(timezone.utc).isoformat(), verified_by, discord_user_id]
                )
            await db.commit()

    async def get_total_debt(self, discord_user_id: str) -> float:
        """Get total unpaid amount for a user."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute('''
                SELECT COALESCE(SUM(total_due), 0) FROM invoices
                WHERE discord_user_id = ? AND status = 'Pending' AND staff_type != 'Manual'
            ''', (discord_user_id,))
            row = await cursor.fetchone()
            return row[0] if row else 0.0

    async def log_payment(self, invoice_id: int, discord_user_id: str, screenshot_url: str, message_id: str):
        """Log a payment submission."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                INSERT INTO payment_logs (invoice_id, discord_user_id, screenshot_url, message_id, submitted_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (invoice_id, discord_user_id, screenshot_url, message_id, datetime.now(timezone.utc).isoformat()))
            await db.commit()

    async def update_dm_sent(self, invoice_id: int):
        """Mark DM as sent for an invoice."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE invoices SET dm_sent = 1 WHERE id = ?", (invoice_id,))
            await db.commit()

    async def add_manual_adjustment(self, discord_user_id: str, discord_username: str,
                                    month: str, year: int, amount: float, reason: str = '') -> int:
        """Add a manual invoice adjustment."""
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.now(timezone.utc).isoformat()
            cursor = await db.execute('''
                INSERT INTO invoices 
                    (discord_user_id, discord_username, month, year, staff_type, revenue,
                     commission_amount, profit_from_sheet, previous_debt, total_due, status, created_at)
                VALUES (?, ?, ?, ?, 'Manual', 0, 0, 0, 0, ?, 'Pending', ?)
            ''', (discord_user_id, discord_username, month, year, amount, now))
            await db.commit()
            return cursor.lastrowid

    async def get_distinct_usernames(self) -> list:
        """Get all distinct usernames that have invoices."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute('''
                SELECT DISTINCT discord_username FROM invoices 
                WHERE discord_username IS NOT NULL 
                ORDER BY discord_username
            ''')
            rows = await cursor.fetchall()
            return [row[0] for row in rows]

    async def update_invoice_amount(self, invoice_id: int, new_amount: float) -> bool:
        """Update invoice total amount."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT id FROM invoices WHERE id = ?", (invoice_id,))
            row = await cursor.fetchone()
            if not row:
                return False
            await db.execute('''
                UPDATE invoices SET total_due = ?, commission_amount = ?, revenue = ? WHERE id = ?
            ''', (new_amount, new_amount, new_amount, invoice_id))
            await db.commit()
            return True

    async def get_all_pending_manual_adjustments(self, discord_user_id: str) -> float:
        """Get sum of ALL pending manual adjustments for a user."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute('''
                SELECT COALESCE(SUM(total_due), 0) 
                FROM invoices 
                WHERE discord_user_id = ? 
                AND status = 'Pending' 
                AND staff_type = 'Manual'
            ''', (discord_user_id,))
            row = await cursor.fetchone()
            return float(row[0]) if row else 0.0

    async def create_or_update_invoice_preserve_status(self, discord_user_id: str, discord_username: str,
                             month: str, year: int, staff_type: str,
                             revenue: float, commission_amount: float,
                             previous_debt: float, total_due: float, profit_from_sheet: float = 0) -> Dict[str, Any]:
        """Create or update invoice for the period without overwriting Paid status."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute('''
                SELECT * FROM invoices 
                WHERE discord_user_id = ? AND month = ? AND year = ?
                  AND staff_type != 'Manual'
            ''', (discord_user_id, month, year))
            existing = await cur.fetchone()
            if existing:
                if existing['status'] == 'Paid':
                    return {'id': existing['id'], 'status': 'Paid'}
                await db.execute('''
                    UPDATE invoices 
                    SET staff_type = ?, revenue = ?, commission_amount = ?, previous_debt = ?, total_due = ?, profit_from_sheet = ?
                    WHERE id = ?
                ''', (staff_type, revenue, commission_amount, previous_debt, total_due, profit_from_sheet, existing['id']))
                await db.commit()
                return {'id': existing['id'], 'status': 'Pending'}
            else:
                cursor = await db.execute('''
                    INSERT INTO invoices 
                    (discord_user_id, discord_username, month, year, staff_type, 
                     revenue, commission_amount, previous_debt, total_due, status, created_at, profit_from_sheet)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'Pending', ?, ?)
                ''', (discord_user_id, discord_username, month, year, staff_type,
                      revenue, commission_amount, previous_debt, total_due,
                      datetime.now(timezone.utc).isoformat(), profit_from_sheet))
                await db.commit()
                return {'id': cursor.lastrowid, 'status': 'Pending'}

    async def adjust_debt(self, discord_user_id: str, discord_username: str, amount: float, reason: str, adjusted_by: str):
        """Adjust debt for a user (create or update Manual invoice only)."""
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.now(timezone.utc)
            # Calculate start of current week (Monday)
            start_of_week = now - timedelta(days=now.weekday())
            start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Check for existing MANUAL pending invoice created THIS WEEK
            # IMPORTANT: Only look for Manual type invoices, not driver/trainer invoices
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT * FROM invoices 
                WHERE discord_user_id = ? AND status = 'Pending' AND staff_type = 'Manual' AND created_at >= ?
                ORDER BY id DESC LIMIT 1
            ''', (discord_user_id, start_of_week.isoformat()))
            invoice = await cursor.fetchone()
            
            if invoice:
                # Update existing Manual invoice
                new_total = invoice['total_due'] + amount
                new_prev_debt = invoice['previous_debt'] + amount
                
                await db.execute('''
                    UPDATE invoices 
                    SET total_due = ?, previous_debt = ?
                    WHERE id = ?
                ''', (new_total, new_prev_debt, invoice['id']))
            else:
                # Create new Manual invoice
                month = now.strftime('%B')
                year = now.year
                
                # Create unique month key to avoid constraint collisions and ensure separation
                special_month = f"{month}-Week-{now.strftime('%W')}-Manual-{int(now.timestamp())}"
                
                await db.execute('''
                    INSERT INTO invoices 
                    (discord_user_id, discord_username, month, year, staff_type, 
                     revenue, commission_amount, previous_debt, total_due, status, created_at)
                    VALUES (?, ?, ?, ?, 'Manual', 0, 0, ?, ?, 'Pending', ?)
                ''', (discord_user_id, discord_username, special_month, year, amount, amount, now.isoformat()))
            
            await db.commit()

    async def create_payment_log(self, invoice_id: int, discord_user_id: str,
                               screenshot_url: str, message_id: str) -> int:
        """Create a payment log entry."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute('''
                INSERT INTO payment_logs 
                (invoice_id, discord_user_id, screenshot_url, message_id, submitted_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (invoice_id, discord_user_id, screenshot_url, message_id,
                  datetime.now(timezone.utc).isoformat()))
            await db.commit()
            return cursor.lastrowid

    async def get_all_staff_names(self) -> list:
        """Get a unique list of all staff names from the invoices table."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT DISTINCT discord_username FROM invoices WHERE discord_username IS NOT NULL ORDER BY discord_username")
            rows = await cursor.fetchall()
            return [row[0] for row in rows]

    async def get_all_unpaid_invoice_debt(self, discord_user_id: str, exclude_period: str = None, exclude_year: int = None) -> float:
        """Get sum of ALL pending non-Manual invoices (accumulated debt from unpaid weeks)."""
        async with aiosqlite.connect(self.db_path) as db:
            if exclude_period and exclude_year:
                # Exclude current period to avoid double counting when calculating for new invoice
                cursor = await db.execute('''
                    SELECT COALESCE(SUM(total_due), 0) 
                    FROM invoices 
                    WHERE discord_user_id = ? 
                    AND status = 'Pending' 
                    AND staff_type != 'Manual'
                    AND NOT (month = ? AND year = ?)
                ''', (discord_user_id, exclude_period, exclude_year))
            else:
                cursor = await db.execute('''
                    SELECT COALESCE(SUM(total_due), 0) 
                    FROM invoices 
                    WHERE discord_user_id = ? 
                    AND status = 'Pending' 
                    AND staff_type != 'Manual'
                ''', (discord_user_id,))
            row = await cursor.fetchone()
            return row[0] if row else 0.0

    async def get_current_invoice(self, discord_user_id: str):
        """Get the most recent pending invoice for a user (any staff type)."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT * FROM invoices 
                WHERE discord_user_id = ? AND status = 'Pending'
                  AND staff_type IN ('driver', 'trainer', 'management')
                ORDER BY year DESC, id DESC
                LIMIT 1
            ''', (discord_user_id,))
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_all_pending_for_user(self, discord_user_id: str) -> list:
        """All pending invoices for a user, newest first."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT * FROM invoices
                WHERE discord_user_id = ? AND status = 'Pending'
                  AND staff_type IN ('driver', 'trainer', 'management')
                ORDER BY year DESC, id DESC
            ''', (discord_user_id,))
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

    async def get_older_debt(self, discord_user_id: str, exclude_month: str, exclude_year: int) -> float:
        """Most recent pending invoice total_due EXCLUDING current period — rolled into new invoice."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT total_due FROM invoices
                WHERE discord_user_id = ? AND status = 'Pending'
                  AND staff_type IN ('driver', 'trainer', 'management')
                  AND NOT (month = ? AND year = ?)
                ORDER BY id DESC
                LIMIT 1
            ''', (discord_user_id, exclude_month, exclude_year))
            row = await cursor.fetchone()
            return float(row['total_due']) if row else 0.0

    async def get_payment_log_by_invoice(self, invoice_id: int):
        """Get most recent payment log for an invoice (duplicate guard)."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM payment_logs WHERE invoice_id = ? ORDER BY id DESC LIMIT 1",
                (invoice_id,)
            )
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_payment_log_by_message(self, message_id: str):
        """Get payment log by message ID."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT pl.id as log_id, pl.invoice_id, pl.discord_user_id, pl.screenshot_url,
                       pl.message_id, pl.submitted_at, pl.verified, pl.verified_by, pl.verified_at,
                       i.total_due, i.discord_username, i.month, i.year
                FROM payment_logs pl
                JOIN invoices i ON pl.invoice_id = i.id
                WHERE pl.message_id = ?
            ''', (message_id,))
            return await cursor.fetchone()

    async def get_unpaid_invoices(self):
        """Get all unpaid invoices."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT * FROM invoices WHERE status = 'Pending'
                ORDER BY total_due DESC
            ''')
            return await cursor.fetchall()

    async def mark_dm_sent(self, invoice_id: int):
        """Mark an invoice as successfully sent via DM."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE invoices SET dm_sent = 1 WHERE id = ?", (invoice_id,))
            await db.commit()

    async def mark_invoice_unpaid(self, discord_user_id: str, month: str, year: int) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT id FROM invoices
                WHERE discord_user_id = ? AND month = ? AND year = ?
                ORDER BY id DESC
                LIMIT 1
            ''', (discord_user_id, month, year))
            row = await cursor.fetchone()
            if not row:
                return False
            invoice_id = row['id']
            await db.execute('''
                UPDATE invoices 
                SET status = 'Pending', paid_at = NULL, verified_by = NULL
                WHERE id = ?
            ''', (invoice_id,))
            await db.execute('''
                UPDATE payment_logs
                SET verified = 0, verified_by = NULL, verified_at = NULL
                WHERE invoice_id = ?
            ''', (invoice_id,))
            await db.commit()
            return True

    async def verify_payment(self, message_id: str, verified_by: str):
        """Mark a payment as verified."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                UPDATE payment_logs 
                SET verified = 1, verified_by = ?, verified_at = ?
                WHERE message_id = ?
            ''', (verified_by, datetime.now(timezone.utc).isoformat(), message_id))
            await db.commit()

# --- Web Server ---
class DashboardAPI:
    def __init__(self, bot_instance, db_instance):
        self.bot = bot_instance
        self.db = db_instance
        self.app = web.Application()
        self.setup_routes()

    def setup_routes(self):
        self.app.router.add_get('/api/vehicles', self.get_vehicles)
        self.app.router.add_post('/api/vehicles/update', self.update_vehicle)
        self.app.router.add_get('/api/invoices', self.get_invoices)
        self.app.router.add_get('/api/drivers', self.get_drivers)
        self.app.router.add_get('/api/activities', self.get_activities)
        self.app.router.add_get('/api/periods', self.get_periods)
        self.app.router.add_get('/api/leaderboard', self.get_leaderboard)
        self.app.router.add_get('/api/auth/me', self.get_me)
        self.app.router.add_post('/api/auth/discord', self.handle_discord_auth)
        self.app.router.add_post('/api/invoices/delete', self.delete_invoice)
        self.app.router.add_post('/api/invoices/update', self.update_invoice)
        self.app.router.add_post('/api/invoices/toggle-status', self.toggle_status)
        self.app.router.add_post('/api/invoices/create', self.create_invoice)
        self.app.router.add_get('/api/totals', self.get_totals)
        self.app.router.add_post('/api/staff/remind', self.send_staff_remind)
        self.app.router.add_post('/api/staff/adjust-debt', self.api_adjust_debt)
        
        # --- ZMĚNĚNO A PŘIDÁNO ZDE ---
        # Dashboard (přidáme i přesnou URL s .html, kam tě vrací Discord)
        self.app.router.add_get('/', self.handle_index)
        self.app.router.add_get('/dashboard.html', self.handle_index)
        
        # Přihlašovací stránka
        self.app.router.add_get('/login.html', self.handle_login)
        
        # Složka s obrázky (logo a náklaďáky) z podsložky Dashboard
        assets_path = ROOT_DIR / 'Dashboard' / 'assets'
        if assets_path.exists():
            self.app.router.add_static('/assets/', path=assets_path, name='assets')

    async def handle_index(self, request):
        dashboard_path = ROOT_DIR / 'Dashboard' / 'dashboard.html'
        if dashboard_path.exists():
            return web.FileResponse(dashboard_path)
        return web.Response(text=f"dashboard.html not found at {dashboard_path}", status=404)

    # --- NOVÁ FUNKCE PRO LOGIN ---
    async def handle_login(self, request):
        login_path = ROOT_DIR / 'Dashboard' / 'login.html'
        if login_path.exists():
            return web.FileResponse(login_path)
        return web.Response(text="login.html not found", status=404)

    async def get_me(self, request):
        # Mock auth for now - in production this would use Discord OAuth2
        return web.json_response({
            "id": "123456789",
            "name": "Admin User",
            "role": "admin"
        })
        
    async def handle_discord_auth(self, request):
        try:
            data = await request.json()
            code = data.get('code')
            
            if not code:
                return web.json_response({"error": "Missing authorization code"}, status=400)

            # Načtení z .env souboru
            client_id = os.environ.get('DISCORD_CLIENT_ID')
            client_secret = os.environ.get('DISCORD_CLIENT_SECRET')
            redirect_uri = os.environ.get('DISCORD_REDIRECT_URI')

            if not client_id or not client_secret:
                logger.error("Chybí Discord OAuth2 klíče v .env souboru!")
                return web.json_response({"error": "Server configuration error"}, status=500)

            # 1. Výměna kódu za Access Token
            token_url = 'https://discord.com/api/v10/oauth2/token'
            payload = {
                'client_id': client_id,
                'client_secret': client_secret,
                'grant_type': 'authorization_code',
                'code': code,
                'redirect_uri': redirect_uri
            }
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}

            async with aiohttp.ClientSession() as session:
                async with session.post(token_url, data=payload, headers=headers) as resp:
                    if resp.status != 200:
                        error_text = await resp.text()
                        logger.error(f"Discord token exchange selhalo: {error_text}")
                        return web.json_response({"error": "Neplatný přihlašovací kód"}, status=400)
                    
                    token_data = await resp.json()
                    access_token = token_data['access_token']
                    expires_in = token_data['expires_in']

                # 2. Získání informací o uživateli pomocí získaného tokenu
                user_url = 'https://discord.com/api/v10/users/@me'
                auth_headers = {'Authorization': f'Bearer {access_token}'}
                
                async with session.get(user_url, headers=auth_headers) as resp:
                    if resp.status != 200:
                        return web.json_response({"error": "Nepodařilo se načíst data o uživateli"}, status=400)
                    
                    user_data = await resp.json()

            user_id = user_data['id']
            username = user_data['username']
            avatar = user_data.get('avatar')

            # 3. Určení role (automaticky podle serverových rolí definovaných v tvém bot.py)
            role = 'driver' # Výchozí role
            if self.bot.guilds:
                guild = self.bot.guilds[0]
                member = guild.get_member(int(user_id))
                if member:
                    user_roles = [r.name.lower() for r in member.roles]
                    if any(r in user_roles for r in ['owner', 'admin', 'chief financial officer']):
                        role = 'admin'
                    elif any(r in user_roles for r in ['operations manager', 'investors']):
                        role = 'mgmt'
            
            # Uložíme roli do DB
            await self.db.set_user_role(user_id, role)

            # 4. Odeslání dat zpět do frontend dashboardu
            return web.json_response({
                "user": {
                    "id": user_id,
                    "username": username,
                    "avatar": avatar
                },
                "user_id": user_id,
                "user_name": username,
                "avatar": avatar,
                "role": role,
                "access_token": access_token,
                "expires_in": expires_in
            })

        except Exception as e:
            logger.error(f"Chyba při přihlašování: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def get_vehicles(self, request):
        try:
            vehicles_data = await self.db.get_all_vehicles()
            for v in vehicles_data:
                v['items'] = json.loads(v['inventory_json'])
            return web.json_response(vehicles_data)
        except Exception as e:
            logger.error(f"Error in get_vehicles: {e}")
            return web.json_response([])

    async def update_vehicle(self, request):
        try:
            data = await request.json()
            v_id = data.get('id')
            user_id = data.get('user_id')
            user_name = data.get('user_name')
            inventory = data.get('items')
            await self.db.update_vehicle(v_id, user_id, user_name, inventory)
            
            # Log activity instead of sending to Discord
            action = "started using" if user_id else "stopped using"
            await self.db.log_activity(
                action=f"{action} vehicle",
                subject=f"{user_name if user_name else 'Someone'} — {v_id}",
                activity_type="vehicle"
            )
            
            return web.json_response({"status": "ok"})
        except Exception as e:
            logger.error(f"API Error: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def get_activities(self, request):
        try:
            activities = await self.db.get_recent_activities()
            if not activities:
                # Mock activity
                return web.json_response([{
                    "id": 0,
                    "action": "system started",
                    "subject": "System",
                    "type": "system",
                    "created_at": datetime.now(timezone.utc).isoformat()
                }])
            return web.json_response(activities)
        except Exception as e:
            logger.error(f"Error in get_activities: {e}")
            return web.json_response([])

    async def get_invoices(self, request):
        try:
            async with aiosqlite.connect(self.db.db_path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute("SELECT * FROM invoices ORDER BY created_at DESC LIMIT 100")
                rows = await cursor.fetchall()
                data = [dict(row) for row in rows]
                
                # Remove mock data
                return web.json_response(data)
        except Exception as e:
            logger.error(f"Error in get_invoices: {e}")
            return web.json_response([])

    async def get_periods(self, request):
        try:
            periods = await self.db.get_recent_periods(20)
            # Remove mock data
            return web.json_response(periods)
        except Exception as e:
            logger.error(f"Error in get_periods: {e}")
            return web.json_response([])

    async def delete_invoice(self, request):
        try:
            data = await request.json()
            invoice_id = data.get('id')
            await self.db.delete_invoice(invoice_id)
            await self.db.log_activity(
                action="deleted invoice",
                subject=f"ID: {invoice_id}",
                activity_type="invoice"
            )
            return web.json_response({"status": "ok"})
        except Exception as e:
            logger.error(f"Error in delete_invoice: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def update_invoice(self, request):
        try:
            data = await request.json()
            invoice_id = data.get('id')
            period = data.get('period')
            amount = float(data.get('amount'))
            await self.db.update_invoice_period_amount(invoice_id, period, amount)
            await self.db.log_activity(
                action="updated invoice",
                subject=f"ID: {invoice_id} to ${amount}",
                activity_type="invoice"
            )
            return web.json_response({"status": "ok"})
        except Exception as e:
            logger.error(f"Error in update_invoice: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def toggle_status(self, request):
        try:
            data = await request.json()
            invoice_id = data.get('id')
            status = data.get('status')
            await self.db.toggle_invoice_status(invoice_id, status)
            await self.db.log_activity(
                action=f"marked invoice as {status}",
                subject=f"ID: {invoice_id}",
                activity_type="invoice"
            )
            return web.json_response({"status": "ok"})
        except Exception as e:
            logger.error(f"Error in toggle_status: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def create_invoice(self, request):
        try:
            data = await request.json()
            user_id = data.get('user_id')
            name = data.get('name')
            period = data.get('period')
            amount = float(data.get('amount'))
            now = datetime.now(timezone.utc)
            
            async with aiosqlite.connect(self.db.db_path) as db_conn:
                cursor = await db_conn.execute('''
                    INSERT INTO invoices 
                    (discord_user_id, discord_username, month, year, staff_type, 
                     revenue, commission_amount, previous_debt, total_due, status, created_at)
                    VALUES (?, ?, ?, ?, 'Manual', 0, 0, 0, ?, 'Pending', ?)
                ''', (user_id, name, period, now.year, amount, now.isoformat()))
                await db_conn.commit()
                invoice_id = cursor.lastrowid
                
            await self.db.log_activity(
                action="created manual invoice",
                subject=f"For {name} — ${amount}",
                activity_type="invoice"
            )
            return web.json_response({"status": "ok", "id": invoice_id})
        except Exception as e:
            logger.error(f"Error in create_invoice: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def get_leaderboard(self, request):
        try:
            now = datetime.now(timezone.utc)
            period_id = get_period_id(now)
            
            # 1. Fetch Weekly Top Earners from DB
            top_earners_weekly = await self.db.get_top_earners(period_id, now.year, 10)
            
            # 2. Fetch Monthly Top Earners from DB
            db_revenue_map = await self.db.get_monthly_revenue(now.year, now.month)
            top_earners_monthly = sorted(
                [{"name": name, "revenue": rev} for name, rev in db_revenue_map.items()],
                key=lambda x: x['revenue'],
                reverse=True
            )[:10]
            
            # If no data for current period, we still return empty lists instead of mock
            # to avoid confusion, but user wants "perfect" data. 
            # If DB is empty, maybe they haven't synced yet.
            
            return web.json_response({
                "weekly": [{"name": e['discord_username'], "revenue": e['revenue']} for e in top_earners_weekly],
                "monthly": top_earners_monthly
            })
        except Exception as e:
            logger.error(f"Error in get_leaderboard: {e}")
            return web.json_response({"weekly": [], "monthly": []})

    async def get_drivers(self, request):
        """Live staff data from ALL sheets (drivers + trainers + management) + DB debt."""
        try:
            all_staff = sheets.get_driver_data() + sheets.get_trainer_data() + sheets.get_management_data()

            results = []
            async with aiosqlite.connect(self.db.db_path) as db_conn:
                db_conn.row_factory = aiosqlite.Row
                for d in all_staff:
                    name = (d.get('name') or '').strip()
                    if not name:
                        continue
                    revenue = float(d.get('total_to_business') or 0)
                    profit = float(d.get('profit_from_sheet') or 0)
                    staff_type = d.get('type', 'driver')

                    # Pending debt from DB (by name — sheet doesn't have discord_id)
                    cursor = await db_conn.execute(
                        "SELECT SUM(total_due) FROM invoices WHERE discord_username = ? AND status = 'Pending' AND staff_type != 'Manual'",
                        (name,)
                    )
                    row = await cursor.fetchone()
                    debt = float(row[0]) if row and row[0] else 0.0

                    # All pending invoices for this person
                    cursor2 = await db_conn.execute(
                        """SELECT id, month, year, total_due, status, created_at FROM invoices
                           WHERE discord_username = ? ORDER BY year DESC, id DESC LIMIT 20""",
                        (name,)
                    )
                    inv_rows = await cursor2.fetchall()
                    inv_history = [dict(r) for r in inv_rows]

                    results.append({
                        "id": normalize_name_api(name),
                        "name": name,
                        "staff_type": staff_type,
                        "revenue": revenue,
                        "profit": profit,
                        "projected_debt": debt,
                        "invoices": inv_history
                    })

            return web.json_response(results)
        except Exception as e:
            logger.error(f"Error in get_drivers: {e}")
            return web.json_response([], status=500)

    async def get_totals(self, request):
        """Dashboard totals — weekly revenue (current period) and monthly revenue."""
        try:
            now = datetime.now(timezone.utc)
            period_id = get_period_id(now)

            # Weekly: sum of all pending+paid invoices for current period
            async with aiosqlite.connect(self.db.db_path) as db_conn:
                cursor = await db_conn.execute(
                    """SELECT COALESCE(SUM(total_due),0) FROM invoices
                       WHERE month = ? AND year = ? AND staff_type != 'Manual'
                         AND discord_user_id NOT LIKE 'MANUAL_ADD_%'""",
                    (period_id, now.year)
                )
                weekly_row = await cursor.fetchone()
                weekly = float(weekly_row[0]) if weekly_row else 0.0

                # Monthly: all invoices this calendar month
                cursor2 = await db_conn.execute(
                    """SELECT COALESCE(SUM(total_due),0) FROM invoices
                       WHERE year = ? AND staff_type != 'Manual'
                         AND discord_user_id NOT LIKE 'MANUAL_ADD_%'
                         AND created_at >= date('now','start of month')""",
                    (now.year,)
                )
                monthly_row = await cursor2.fetchone()
                monthly = float(monthly_row[0]) if monthly_row else 0.0

                # Active vehicles
                cursor3 = await db_conn.execute(
                    "SELECT COUNT(*) FROM vehicles WHERE current_user_id IS NOT NULL"
                )
                active_vehicles_row = await cursor3.fetchone()
                active_vehicles = active_vehicles_row[0] if active_vehicles_row else 0

                # Pending invoices count
                cursor4 = await db_conn.execute(
                    "SELECT COUNT(*) FROM invoices WHERE status = 'Pending' AND staff_type != 'Manual'"
                )
                pending_row = await cursor4.fetchone()
                pending = pending_row[0] if pending_row else 0

            return web.json_response({
                "weekly": weekly,
                "monthly": monthly,
                "active_vehicles": active_vehicles,
                "pending_invoices": pending
            })
        except Exception as e:
            logger.error(f"Error in get_totals: {e}")
            return web.json_response({"weekly": 0, "monthly": 0, "active_vehicles": 0, "pending_invoices": 0})

    async def send_staff_remind(self, request):
        """Send reminder DM to a specific staff member via dashboard."""
        try:
            data = await request.json()
            username = data.get('username', '')
            invoice_id = data.get('invoice_id')

            guild = bot.guilds[0] if bot.guilds else None
            if not guild:
                return web.json_response({"error": "Bot not in guild"}, status=400)

            # Find member
            member = None
            for m in guild.members:
                if names_match(m.display_name, username) or names_match(m.name, username):
                    member = m
                    break

            if not member:
                return web.json_response({"error": f"Member '{username}' not found"}, status=404)

            # Get their latest pending invoice
            async with aiosqlite.connect(self.db.db_path) as db_conn:
                db_conn.row_factory = aiosqlite.Row
                if invoice_id:
                    cursor = await db_conn.execute(
                        "SELECT * FROM invoices WHERE id = ?", (invoice_id,)
                    )
                else:
                    cursor = await db_conn.execute(
                        "SELECT * FROM invoices WHERE discord_username = ? AND status = 'Pending' AND staff_type != 'Manual' ORDER BY id DESC LIMIT 1",
                        (username,)
                    )
                inv_row = await cursor.fetchone()

            if not inv_row:
                return web.json_response({"error": "No pending invoice found"}, status=404)

            inv = dict(inv_row)
            period_id = inv['month']
            profit_val = inv.get('profit_from_sheet') or max(0.0, (inv['revenue'] or 0) - (inv['commission_amount'] or 0))

            reminder_embed = create_invoice_embed(member.display_name, inv['total_due'], profit_val, period_id, inv.get('previous_debt', 0) or 0)
            reminder_embed.title = "🔔 Payment Reminder — Invoice Outstanding"
            reminder_embed.color = Colors.WARNING
            reminder_embed.description = (
                f"⚠️ **This is a reminder.** Your payment for **{period_id}** is still outstanding.\n\n"
                f"Please use `/pay` to submit your payment proof."
            )

            await member.send(embed=reminder_embed)
            await self.db.log_activity(action="sent reminder", subject=f"{username} — {period_id}", activity_type="invoice")

            return web.json_response({"status": "ok", "message": f"Reminder sent to {member.display_name}"})
        except Exception as e:
            logger.error(f"Error in send_staff_remind: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def api_adjust_debt(self, request):
        """Add manual debt adjustment for a staff member via dashboard."""
        try:
            data = await request.json()
            username = data.get('username', '')
            amount = float(data.get('amount', 0))
            reason = data.get('reason', 'Dashboard adjustment')

            # Find discord_user_id by name
            guild = bot.guilds[0] if bot.guilds else None
            member = None
            if guild:
                for m in guild.members:
                    if names_match(m.display_name, username) or names_match(m.name, username):
                        member = m
                        break

            if not member:
                return web.json_response({"error": f"Member '{username}' not found"}, status=404)

            await db.adjust_debt(str(member.id), member.display_name, amount, reason)
            await self.db.log_activity(action="adjusted debt", subject=f"{username} — ${amount:+,.0f}", activity_type="invoice")

            return web.json_response({"status": "ok"})
        except Exception as e:
            logger.error(f"Error in api_adjust_debt: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def start(self):
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()
        logger.info("Dashboard API started on http://localhost:8080")
    

# Worksheet tabs to skip when reading staff data
SKIP_SHEETS = {'template', 'instructions', 'summary', 'totals', 'sheet1'}

class GoogleSheetsManager:
    """Handles Google Sheets operations."""
    
    def __init__(self):
        self.gc = None
        self.last_error = None
        self._initialize()
    
    def _initialize(self):
        """Initialize Google Sheets client."""
        creds_path = ROOT_DIR / 'google_credentials.json'
        if creds_path.exists():
            try:
                creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
                self.gc = gspread.authorize(creds)
                logger.info("Google Sheets client initialized successfully")
            except Exception as e:
                self.last_error = f"Failed to initialize Google Sheets: {e}"
                logger.error(self.last_error)
        else:
            self.last_error = "Google credentials file not found"
            logger.error(self.last_error)
    
    def _parse_currency(self, value) -> float:
        """Parse currency value handling all formats from Google Sheets.
        
        Handles:
        - European thousands separator with dot: '8.333' -> 8333, '1.234,56' -> 1234.56
        - US thousands separator with comma: '8,333' -> 8333, '1,234.56' -> 1234.56
        - Sheet-formatted values like ' $ 8.333 ' or ' $ -   '
        
        Key rule: a single dot followed by exactly 3 digits = thousands separator (e.g. 8.333 = 8333).
        A dot followed by 1-2 digits = decimal separator (e.g. 8.50 = 8.50).
        """
        try:
            val_str = str(value).strip()
            # Remove currency symbols, spaces, special chars
            val_str = val_str.replace('$', '').replace('€', '').replace(' ', '').replace('\'', '').replace('’', '')
            
            if not val_str or val_str in ['-', '–', '—', '-']:
                return 0.0
            
            # Sheet formula errors
            if val_str.startswith('#'):
                return 0.0
            
            # Case 1: Both comma and dot present
            if ',' in val_str and '.' in val_str:
                if val_str.rfind(',') > val_str.rfind('.'):
                    # European: 1.234,56 -> 1234.56
                    val_str = val_str.replace('.', '').replace(',', '.')
                else:
                    # US: 1,234.56 -> 1234.56
                    val_str = val_str.replace(',', '')
            
            # Case 2: Only comma present
            elif ',' in val_str:
                parts = val_str.split(',')
                if len(parts) == 2 and len(parts[1]) <= 2:
                    # European decimal: 8,50 -> 8.50
                    val_str = val_str.replace(',', '.')
                else:
                    # US thousands separator: 8,333 -> 8333
                    val_str = val_str.replace(',', '')
            
            # Case 3: Only dot present
            elif '.' in val_str:
                if val_str.count('.') > 1:
                    # Multiple dots = European thousands separators: 1.234.567 -> 1234567
                    val_str = val_str.replace('.', '')
                else:
                    # Single dot — check digits after it
                    parts = val_str.split('.')
                    after_dot = parts[1] if len(parts) > 1 else ''
                    if len(after_dot) == 3:
                        # Exactly 3 digits after dot = European thousands separator: 8.333 -> 8333
                        val_str = val_str.replace('.', '')
                    else:
                        # 1 or 2 digits after dot = decimal: 8.50 -> 8.50
                        pass
            
            return float(val_str) if val_str else 0.0
        except:
            return 0.0
    
    def _fetch_sheet_data(self, sheet_id: str, staff_type: str, commission_rate: float) -> List[Dict[str, Any]]:
        """
        Fetch all worksheet data for a spreadsheet using a single batched API call.
        """
        if not self.gc:
            self.last_error = "Google Sheets not initialized"
            return []

        try:
            spreadsheet = self.gc.open_by_key(sheet_id)

            # Step 1: Get worksheet metadata
            worksheets = spreadsheet.worksheets()
            valid_sheets = [ws for ws in worksheets
                            if ws.title.strip().lower() not in SKIP_SHEETS]

            if not valid_sheets:
                return []

            # Step 2: Build batch ranges — fetch I4 (invoice amount, merged I4:J4) and I6 (driver profit, merged I6:J6)
            ranges = []
            for ws in valid_sheets:
                title = ws.title
                escaped = title.replace("'", "''")
                ranges.append(f"\'{escaped}\'!I4")        # invoice amount to charge (merged cell I4:J4)
                ranges.append(f"\'{escaped}\'!I6")        # driver profit (merged cell I6:J6)

            # Step 3: Single batched API call
            result = spreadsheet.values_batch_get(ranges)
            value_ranges = result.get('valueRanges', [])

            # Step 4: Parse results
            data = []
            for i, ws in enumerate(valid_sheets):
                name = ws.title.strip()

                i4_idx = i * 2      # I4 = invoice amount (merged cell I4:J4)
                i6_idx = i * 2 + 1  # I6 = driver profit (merged cell I6:J6)

                # Parse I4 — final invoice amount to charge
                total_to_business = 0
                if i4_idx < len(value_ranges):
                    i4_values = value_ranges[i4_idx].get('values', [])
                    if i4_values and i4_values[0]:
                        total_to_business = self._parse_currency(i4_values[0][0])

                # Parse I6 — driver/staff profit (informational only)
                profit_to_driver = 0
                if i6_idx < len(value_ranges):
                    i6_values = value_ranges[i6_idx].get('values', [])
                    if i6_values and i6_values[0]:
                        profit_to_driver = self._parse_currency(i6_values[0][0])

                data.append({
                    'name': name,
                    'total_to_business': total_to_business,  # I4 (merged I4:J4) = final invoice amount
                    'type': staff_type,
                    'commission_rate': 1.0,  # I4 is already final, no further multiplication
                    'profit_from_sheet': profit_to_driver,   # I6 (merged I6:J6) = driver profit shown in invoice
                })

            return data

        except Exception as e:
            self.last_error = f"{staff_type.capitalize()} sheet error: {e}"
            logger.error(self.last_error)
            return []

    def get_driver_data(self) -> List[Dict[str, Any]]:
        """Fetch driver data — single batched API call."""
        return self._fetch_sheet_data(DRIVER_SHEET_ID, 'driver', DRIVER_COMMISSION)

    def get_trainer_data(self) -> List[Dict[str, Any]]:
        """Fetch trainer data — single batched API call."""
        return self._fetch_sheet_data(TRAINER_SHEET_ID, 'trainer', TRAINER_COMMISSION)

    def get_management_data(self) -> List[Dict[str, Any]]:
        """Fetch management data — single batched API call."""
        return self._fetch_sheet_data(MANAGEMENT_SHEET_ID, 'management', MANAGEMENT_COMMISSION)

    def backup_spreadsheet(self, sheet_id: str, label: str = "Backup") -> dict:
        """Create a backup of the spreadsheet in Google Drive."""
        if not self.gc:
            return {'success': False, 'error': 'Google Sheets not initialized'}
            
        try:
            # Open the source spreadsheet
            spreadsheet = self.gc.open_by_key(sheet_id)
            
            # Use only the label (e.g. "Week 08_Drivers") as the name for clarity
            backup_name = label
            
            # Copy the file using Drive API
            backup_spreadsheet = self.gc.copy(sheet_id, title=backup_name)
            
            logger.info(f"Backup created successfully: {backup_name}")
            return {'success': True, 'name': backup_name, 'id': backup_spreadsheet.id}
            
        except Exception as e:
            logger.error(f"Error creating backup for {sheet_id}: {e}")
            return {'success': False, 'error': str(e)}

    def clear_weekly_data(self, sheet_id: str) -> dict:
        """Clear weekly data from a sheet after invoices are sent.

        Clears ONLY column D from row 6 downward (the actual data entry cells).
        Formulas in E, F, I, J auto-recalculate to $0 — no need to touch them.

        Uses a single batch_get to read row counts, then batch_update to clear —
        avoids per-worksheet API calls that trigger rate limits.
        """
        if not self.gc:
            return {'success': False, 'error': 'Google Sheets not initialized'}

        try:
            spreadsheet = self.gc.open_by_key(sheet_id)

            # Step 1: get worksheet list (1 API call)
            worksheets = spreadsheet.worksheets()
            valid_sheets = [ws for ws in worksheets
                            if ws.title.strip().lower() not in SKIP_SHEETS]

            if not valid_sheets:
                return {'success': True, 'cleared': []}

            # Step 2: build batch update — clear D6:D304 for every sheet (fixed range)
            data = []
            cleared_sheets = []
            CLEAR_ROWS = 304
            CLEAR_START = 6
            rows_to_clear = CLEAR_ROWS - CLEAR_START + 1  # 299 rows

            for ws in valid_sheets:
                name = ws.title.strip()
                escaped = name.replace("'", "''")
                data.append({
                    'range': f"'{escaped}'!D{CLEAR_START}:D{CLEAR_ROWS}",
                    'values': [[''] for _ in range(rows_to_clear)]
                })
                cleared_sheets.append(name)

            if data:
                # Single batch write for all sheets (1 API call)
                spreadsheet.values_batch_update({
                    'valueInputOption': 'RAW',
                    'data': data
                })
                for name in cleared_sheets:
                    logger.info(f"Cleared data for sheet: {name}")

            return {'success': True, 'cleared': cleared_sheets}

        except Exception as e:
            logger.error(f"Error clearing sheet data: {e}")
            return {'success': False, 'error': str(e)}


# Initialize database and sheets
db = Database(DB_PATH)
sheets = GoogleSheetsManager()

# ==================== ENUMS / CONSTANTS ====================
class StaffType:
    DRIVER = 'driver'
    TRAINER = 'trainer'
    MANAGEMENT = 'management'
    MANUAL = 'Manual'

class InvoiceStatus:
    PENDING = 'Pending'
    PAID = 'Paid'

# ==================== AUTH DECORATOR ====================
def admin_only():
    """App command check that restricts to authorized roles."""
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            return False
        user_roles = [role.name.lower() for role in interaction.user.roles]
        if not any(role in user_roles for role in AUTHORIZED_ROLES):
            embed = discord.Embed(
                title="🔒 Access Denied",
                description="You need one of these roles: Owner, Investors, CFO, Operations Manager, or Admin.",
                color=Colors.ERROR
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return False
        return True
    return app_commands.check(predicate)

def apply_thumbnail(embed: discord.Embed) -> discord.Embed:
    """Attach the bot thumbnail/logo to an embed if configured."""
    if BOT_THUMBNAIL:
        embed.set_thumbnail(url=BOT_THUMBNAIL)
    return embed


async def send_loading(interaction: discord.Interaction, text: str = "⏳ Fetching data from sheets, please wait...") -> None:
    """Send an ephemeral loading message then defer – call BEFORE defer."""
    # NOTE: interaction must not have been responded to yet.
    await interaction.response.send_message(text, ephemeral=True)


# Small caps unicode to regular letters mapping
SMALL_CAPS_MAP = {
    'ᴀ': 'a', 'ʙ': 'b', 'ᴄ': 'c', 'ᴅ': 'd', 'ᴇ': 'e', 'ꜰ': 'f', 'ɢ': 'g',
    'ʜ': 'h', 'ɪ': 'i', 'ᴊ': 'j', 'ᴋ': 'k', 'ʟ': 'l', 'ᴍ': 'm', 'ɴ': 'n',
    'ᴏ': 'o', 'ᴘ': 'p', 'ǫ': 'q', 'ʀ': 'r', 'ꜱ': 's', 'ᴛ': 't', 'ᴜ': 'u',
    'ᴠ': 'v', 'ᴡ': 'w', 'x': 'x', 'ʏ': 'y', 'ᴢ': 'z',
    # Also handle some other common fancy characters
    'ₐ': 'a', 'ₑ': 'e', 'ₒ': 'o', 'ₓ': 'x', 'ₔ': 'e',
}


def normalize_name_api(name: str) -> str:
    """Normalize a name to a consistent format for IDs used in the dashboard API."""
    if not name:
        return ""
    name = name.strip().lower()
    # Replace spaces and special characters with underscores
    name = re.sub(r'[^a-z0-9]', '_', name)
    # Remove consecutive underscores
    name = re.sub(r'_+', '_', name)
    return name.strip('_')


def normalize_name(name: str) -> str:
    """Normalize a name by converting unicode/fancy characters to ASCII equivalents."""
    if not name:
        return ""
    result = []
    for char in name.lower():
        # Check small caps map first
        if char in SMALL_CAPS_MAP:
            result.append(SMALL_CAPS_MAP[char])
        elif ord(char) < 128:
            result.append(char)
        else:
            # Try unicode normalization for other characters
            normalized = unicodedata.normalize('NFKD', char)
            ascii_char = ''.join(c for c in normalized if ord(c) < 128)
            if ascii_char:
                result.append(ascii_char)
    return ''.join(result).strip()


def names_match(name1: str, name2: str) -> bool:
    """Check if two names match, accounting for unicode variations and minor formatting differences."""
    if not name1 or not name2:
        return False
        
    # Standard normalization
    n1 = normalize_name(name1)
    n2 = normalize_name(name2)
    if n1 == n2:
        return True
        
    # Alphanumeric only match (handles space differences like "Jay Dee" vs "JayDee")
    an1 = "".join(c for c in n1 if c.isalnum())
    an2 = "".join(c for c in n2 if c.isalnum())
    if an1 == an2 and an1 != "":
        return True
        
    # Substring match (handles suffixes like "Name | Driver")
    if len(an1) > 2 and len(an2) > 2:
        if an1 in an2 or an2 in an1:
            return True
            
    return False


def format_name_for_email(name: str) -> str:
    """Convert a display name to email format (e.g., 'John Smith' -> 'JohnSmith')."""
    # Normalize and remove special characters
    normalized = normalize_name(name)
    # Capitalize each word and remove spaces
    parts = normalized.split()
    return ''.join(word.capitalize() for word in parts)


def create_invoice_embed(member_name: str, amount_owed: float, profit_made: float, period_id: str = None, previous_debt: float = 0) -> discord.Embed:
    """Create a professional invoice embed for DMs."""
    now = datetime.now(timezone.utc)
    display_period = period_id if period_id else f"Week {now.isocalendar()[1]:02d}"
    email_name = format_name_for_email(member_name)

    embed = discord.Embed(
        title="Incoming Invoice",
        color=Colors.INVOICE,
        timestamp=now
    )

    # The Body Text — original email-style format
    description_text = (
        f"To: {email_name}@rockyroad.com\n"
        f"From: payroll@rockyroad.com\n\n"
        f"Subject: Invoice {display_period}\n\n"
        f"Dear {member_name}\n\n"
        f"I trust you are well. Please find below the details for {display_period}'s payment to Rocky Road Tow, amounting to **${amount_owed:,.0f}**.\n\n"
        f"Please transfer the payment to account #4966714 and ensure the reference includes the week. For example: \"Weekly payment {display_period}\"\n\n"
        f"We greatly appreciate your prompt processing of this payment. Should you have any questions, please don't hesitate to get in touch.\n\n"
        f"Please do not reply to this email with anything besides `/pay`\n"
        f"Please also upload a screenshot of the bank transaction when running `/pay`.\n\n"
        f"Best regards,\n"
        f"Rocky Road Payroll\n"
        f"13 Rocky Road Tow / Great Ocean Highway\n\n"
        f"**\u201cThe Pros To Know When You Need A Tow!\u201d**"
    )

    embed.description = description_text

    # Summary box
    summary_text = f"**Amount Owed**\n`${amount_owed:,.0f}`\n"
    if previous_debt > 0:
        this_week = amount_owed - previous_debt
        summary_text += f"**This Week**\n`${this_week:,.0f}`\n"
        summary_text += f"**Older Debt**\n`${previous_debt:,.0f}`\n"
    summary_text += (
        f"**Profit Made**\n`${profit_made:,.0f}`\n"
        f"**Bank Account #**\n```\n4966714\n```"
    )

    embed.add_field(name="Summary Details", value=summary_text, inline=False)
    return embed

def has_authorized_role(interaction: discord.Interaction) -> bool:
    """Check if user has an authorized role."""
    if not interaction.guild:
        return False
    user_roles = [role.name.lower() for role in interaction.user.roles]
    return any(role in user_roles for role in AUTHORIZED_ROLES)


def calculate_amount_owed(commission_amount: float, previous_debt: float, staff_type: str = 'driver') -> tuple:
    """Returns (base_amount, total_due).
    base_amount = this week's commission (with minimum fee applied).
    total_due   = base_amount + previous_debt (unpaid older invoices).
    """
    if staff_type == 'management':
        base_amount = commission_amount
    else:
        base_amount = max(commission_amount, MINIMUM_FEE) if commission_amount > 0 else MINIMUM_FEE

    total_due = max(0, base_amount + previous_debt)
    return base_amount, total_due


# Create bot with intents
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.reactions = True
intents.guilds = True

bot = commands.Bot(command_prefix='!', intents=intents)

# Initialize Dashboard API
dashboard_api = DashboardAPI(bot, db)

# CET and EST timezones
CET = ZoneInfo('Europe/Berlin')
EST = ZoneInfo('America/New_York')

@bot.command()
async def sync(ctx):
    """Sync commands globally."""
    # Check if user has admin role
    user_roles = [role.name.lower() for role in ctx.author.roles]
    if not any(role in user_roles for role in AUTHORIZED_ROLES):
        await ctx.send("❌ You don't have permission to use this command.")
        return

    try:
        await ctx.send("🔄 Syncing commands globally... (This may take a few minutes to update in Discord)")
        # Sync globally instead of to guild to avoid duplicates
        synced = await bot.tree.sync()
        await ctx.send(f"✅ Successfully synced {len(synced)} commands globally!")
    except Exception as e:
        await ctx.send(f"❌ Failed to sync: {e}")

# Auto-invoice time: Sunday at 2:00 PM EST
AUTO_INVOICE_TIME = time(hour=14, minute=0, tzinfo=EST)
# Sync time: 5 minutes before invoicing (1:55 PM EST)
AUTO_SYNC_TIME = time(hour=13, minute=55, tzinfo=EST)


def get_period_id(date_obj: datetime = None) -> str:
    """Get the standardized period ID (Week X - Month).
    
    Uses ISO week number so it's consistent and doesn't drift.
    The business week label matches the ISO week number of the date.
    ISO weeks start Monday. Week 08 = the week containing Feb 16-22, 2026.
    """
    if date_obj is None:
        date_obj = datetime.now(timezone.utc)
    iso_week = date_obj.isocalendar()[1]  # ISO week number, stable and no offset needed
    month_name = date_obj.strftime('%B')
    return f"Week {iso_week:02d} - {month_name}"

@tasks.loop(time=AUTO_SYNC_TIME)
async def auto_weekly_sync():
    """Sync data from sheets to database and create backups 5 minutes before invoicing."""
    # Only run on Sundays (weekday 6)
    now_est = datetime.now(EST)
    if now_est.weekday() != 6:
        return

    logger.info("Starting automatic weekly sync and backup...")

    try:
        debt_rollovers: list = []
        skipped_members: list = []
        now_utc = datetime.now(timezone.utc)
        period_id = get_period_id(now_utc)
        year = now_utc.year

        # 1. CREATE BACKUPS
        for sid, label in [(DRIVER_SHEET_ID, "Drivers"), (TRAINER_SHEET_ID, "Trainers"), (MANAGEMENT_SHEET_ID, "Management")]:
            if sid:
                # This will name it exactly "Week 08_Drivers" etc.
                sheets.backup_spreadsheet(sid, f"{period_id}_{label}")

        # 2. Fetch data from sheets
        drivers = sheets.get_driver_data()
        trainers = sheets.get_trainer_data()
        management = sheets.get_management_data()
        all_staff = drivers + trainers + management

        if not all_staff:
            logger.error("Auto-sync: Could not fetch data from Google Sheets")
            return

        # 3. Update database
        if not bot.guilds:
            logger.error("Auto-sync: Bot is not in any guilds")
            return
            
        guild = bot.guilds[0]
        # Ensure members are cached
        if not guild.chunked:
            try:
                await guild.chunk()
            except:
                pass

        for staff in all_staff:
            name = staff.get('name')
            revenue = staff.get('total_to_business', 0)
            staff_type = staff.get('type', 'driver')
            profit_from_sheet = staff.get('profit_from_sheet', 0)

            # Find member
            member = None
            for m in guild.members:
                if names_match(m.display_name, name) or names_match(m.name, name):
                    member = m
                    break
            
            if not member:
                logger.warning(f"Auto-sync: Could not find Discord member for sheet name '{name}' — skipping")
                skipped_members.append(name)
                continue

            # Skip if LOA
            if LOA_ROLE_ID in [r.id for r in member.roles]:
                logger.info(f"Auto-sync: {name} is on LOA — skipping")
                continue

            # J4 cell already contains the final invoice amount — use directly
            # Minimum fee ($5,000) always applies even if revenue = 0
            commission_val = revenue

            # Accumulate unpaid older invoices into previous_debt
            previous_debt = await db.get_older_debt(str(member.id), period_id, year)
            if previous_debt > 0:
                debt_rollovers.append((member.display_name, previous_debt))
            base_amount, total_due = calculate_amount_owed(commission_val, previous_debt, staff_type)

            # Save invoice to DB (preserves Paid status if already paid)
            await db.create_or_update_invoice_preserve_status(
                str(member.id), member.display_name, period_id, year,
                staff_type, revenue, base_amount, previous_debt, total_due, profit_from_sheet
            )
        
        logger.info("Auto-sync: Database and backups updated successfully")
        asyncio.ensure_future(WebhookLogger.sync_complete(
            period_id, len(all_staff), debt_rollovers, skipped_members
        ))

    except Exception as e:
        logger.error(f"Auto-sync error: {e}")
        asyncio.ensure_future(WebhookLogger.error("auto_weekly_sync", e))

@tasks.loop(time=AUTO_INVOICE_TIME)
async def auto_weekly_invoice():
    """Automatically send weekly invoices from the database and clear sheets."""
    # Only run on Sundays (weekday 6)
    now_est = datetime.now(EST)
    if now_est.weekday() != 6:  # 0=Monday, 6=Sunday
        return
    
    logger.info("Starting automatic weekly invoice process from database...")
    
    if not bot.guilds:
        logger.error("Auto-invoice: Bot is not in any guilds")
        return
    
    guild = bot.guilds[0]
    payroll_channel = bot.get_channel(PAYROLL_CHANNEL_ID)
    
    if not payroll_channel:
        logger.error("Auto-invoice: Payroll channel not found")
        return
    
    try:
        # Get current period info
        now_utc = datetime.now(timezone.utc)
        period_id = get_period_id(now_utc)
        year = now_utc.year
        
        # 1. Fetch data from DB (Source of Truth)
        pending_invoices = await db.get_pending_invoices_for_period(period_id, year)
        
        if not pending_invoices:
            logger.info(f"Auto-invoice: No pending invoices found in DB for {period_id}")
            return
        
        success = 0
        failed = 0
        total_owed = 0.0
        results = []
        
        # 2. Send invoices from DB
        for inv in pending_invoices:
            # Allow sending if specifically in DB as pending, regardless of staff_type
            # unless we want to keep the general skip but the user says they WANT to send these
            name = inv['discord_username']
            user_id = inv['discord_user_id']
            total_due = inv['total_due']
            revenue = inv['revenue']
            base_amount = inv['commission_amount']
            profit_val    = inv.get('profit_from_sheet', 0)
            previous_debt = inv.get('previous_debt', 0) or 0

            if profit_val <= 0:
                profit_val = max(0.0, revenue - base_amount)

            # MIGRATED_ IDs are not numeric — skip them for DM sending
            try:
                member_id = int(user_id)
            except ValueError:
                results.append(f"⚠️ {name} - Skipped (legacy MIGRATED ID)")
                failed += 1
                continue

            member = guild.get_member(member_id)
            if not member:
                results.append(f"⚠️ {name} - Not in server")
                failed += 1
                continue

            invoice_embed = create_invoice_embed(member.display_name, total_due, profit_val, period_id, previous_debt)
            
            try:
                await member.send(embed=invoice_embed)
                await db.mark_dm_sent(inv['id']) # Track successful DM
                results.append(f"✅ {name} - ${total_due:,.0f}")
                success += 1
                total_owed += total_due
            except:
                results.append(f"❌ {name} - DM failed")
                failed += 1
        
        # 3. CLEAR SHEETS AFTER SUCCESSFUL INVOICING
        sheets_cleared = []
        if CLEAR_MODE == 'weekly':
            driver_clear = sheets.clear_weekly_data(DRIVER_SHEET_ID)
            trainer_clear = sheets.clear_weekly_data(TRAINER_SHEET_ID)
            mgmt_clear = sheets.clear_weekly_data(MANAGEMENT_SHEET_ID)
            
            if driver_clear.get('success'):
                sheets_cleared.extend(driver_clear.get('cleared', []))
            if trainer_clear.get('success'):
                sheets_cleared.extend(trainer_clear.get('cleared', []))
            if mgmt_clear.get('success'):
                sheets_cleared.extend(mgmt_clear.get('cleared', []))
        
        # 4. SEND SUMMARY
        summary = discord.Embed(
            title="📊 Automatic Invoicing Summary",
            description=f"✅ {success} sent | ❌ {failed} failed\n**Period:** {period_id}",
            color=Colors.SUCCESS if failed == 0 else Colors.WARNING,
            timestamp=datetime.now(timezone.utc)
        )
        apply_thumbnail(summary)
        
        summary.add_field(
            name="💰 TOTAL OWED THIS WEEK",
            value=f"```fix\n${total_owed:,.2f}\n```",
            inline=False
        )
        
        # Results detail
        details_text = "\n".join(results[:15])
        if len(results) > 15:
            details_text += f"\n... +{len(results) - 15} more"
        summary.add_field(name="Details", value=details_text or "None", inline=False)
        
        # Sheets cleared info
        if sheets_cleared:
            summary.add_field(
                name="🧹 Sheets Cleared",
                value=f"{len(sheets_cleared)} worksheets reset for new week",
                inline=True
            )
        
        await payroll_channel.send(embed=summary)
        logger.info(f"Auto-invoice completed: {success} sent, {failed} failed, ${total_owed:,.2f} total owed")
        
    except Exception as e:
        logger.error(f"Auto-invoice error: {e}")
        import traceback as _tb
        asyncio.ensure_future(WebhookLogger.error("auto_weekly_invoice", e, _tb.format_exc()))
        error_embed = discord.Embed(
            title="❌ Auto-Invoice Error",
            description=f"**The automatic weekly invoice failed.**\n```\n{e}\n```",
            color=Colors.ERROR,
            timestamp=datetime.now(timezone.utc)
        )
        if payroll_channel:
            await payroll_channel.send(embed=error_embed)
        # DM the designated admin if configured
        if ADMIN_ERROR_DM_ID:
            try:
                admin_user = await bot.fetch_user(ADMIN_ERROR_DM_ID)
                if admin_user:
                    await admin_user.send(embed=error_embed)
            except Exception:
                pass


@auto_weekly_sync.before_loop
async def before_auto_sync():
    """Wait until the bot is ready before starting the task."""
    await bot.wait_until_ready()


@auto_weekly_invoice.before_loop
async def before_auto_invoice():
    """Wait until the bot is ready before starting the task."""
    await bot.wait_until_ready()


@bot.event
async def on_ready():
    """Called when bot is ready."""
    logger.info(f'Logged in as {bot.user} (ID: {bot.user.id})')
    logger.info(f'Connected to {len(bot.guilds)} guild(s)')
    
    # Initialize database
    await db.initialize()
    
    # Start dashboard API
    await dashboard_api.start()
    
    # Start auto-invoice tasks
    if not auto_weekly_sync.is_running():
        auto_weekly_sync.start()
        logger.info("Auto-sync task started (Sunday 1:55 PM EST)")

    if not auto_weekly_invoice.is_running():
        auto_weekly_invoice.start()
        logger.info("Auto-invoice task started (Sunday 2:00 PM EST)")
    
    if not monthly_clear_task.is_running():
        monthly_clear_task.start()
        logger.info("Monthly clear task started (23:55 CET daily, clears at month end)")
        
    if not sheets_backup_task.is_running():
        sheets_backup_task.start()
        logger.info("Sheets backup task started (every 1 hour)")

    if not db_backup_task.is_running():
        db_backup_task.start()
        logger.info("DB backup task started (every 6 hours)")

    if not backup_task.is_running():
        backup_task.start()

    if not monthly_leaderboard_reset_task.is_running():
        monthly_leaderboard_reset_task.start()
        logger.info("Monthly leaderboard reset task started (1st of each month at 2:00 PM EST)")
    
    # Sync commands
    try:
        # To fix duplicate commands: Clear guild-specific commands for each guild the bot is in
        # and only use global sync.
        for guild in bot.guilds:
            bot.tree.clear_commands(guild=guild)
            await bot.tree.sync(guild=guild)
            logger.info(f"Cleared guild commands for {guild.name}")

        # Global sync
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} global command(s)")
    except Exception as e:
        logger.error(f"Failed to sync commands: {e}")

    # Set status
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name="Rocky Road Finances"
        )
    )

MONTHLY_CLEAR_TIME = time(hour=23, minute=55, tzinfo=CET)

@tasks.loop(time=MONTHLY_CLEAR_TIME)
async def monthly_clear_task():
    now_cet = datetime.now(CET)
    tomorrow = now_cet + timedelta(days=1)
    if tomorrow.month != now_cet.month:
        if not bot.guilds:
            return
        guild = bot.guilds[0]
        payroll_channel = bot.get_channel(PAYROLL_CHANNEL_ID)
        if not payroll_channel:
            return
        cleared = []
        driver_result = sheets.clear_weekly_data(DRIVER_SHEET_ID) if CLEAR_MODE == 'monthly' else {'success': False}
        trainer_result = sheets.clear_weekly_data(TRAINER_SHEET_ID) if CLEAR_MODE == 'monthly' else {'success': False}
        mgmt_result = sheets.clear_weekly_data(MANAGEMENT_SHEET_ID) if CLEAR_MODE == 'monthly' else {'success': False}
        
        if driver_result.get('success'):
            cleared.extend(driver_result.get('cleared', []))
        if trainer_result.get('success'):
            cleared.extend(trainer_result.get('cleared', []))
        if mgmt_result.get('success'):
            cleared.extend(mgmt_result.get('cleared', []))
        embed = discord.Embed(
            title="🧹 Monthly Sheets Cleared",
            description=f"Cleared {len(cleared)} worksheets for new month",
            color=Colors.SUCCESS,
            timestamp=datetime.now(timezone.utc)
        )
        await payroll_channel.send(embed=embed)


# ==================== BACKUP TASKS ====================

@tasks.loop(hours=1)
async def sheets_backup_task():
    """Backup Google Sheets data every hour as JSON snapshot."""
    await bot.wait_until_ready()

    now = datetime.now(ZoneInfo('America/New_York'))
    # e.g. "2026-02-28_14-00_EST"
    timestamp = now.strftime('%Y-%m-%d_%H-%M_EST')
    # Week label for grouping, e.g. "Week 09 - February"
    period_label = get_period_id(datetime.now(timezone.utc))

    backup_dir = ROOT_DIR / 'backups' / 'sheets'
    backup_dir.mkdir(parents=True, exist_ok=True)

    try:
        drivers = sheets.get_driver_data()
        trainers = sheets.get_trainer_data()
        management = sheets.get_management_data()

        data = {
            'timestamp': timestamp,
            'period': period_label,
            'snapshot_utc': datetime.now(timezone.utc).isoformat(),
            'drivers': drivers,
            'trainers': trainers,
            'management': management,
        }

        filename = f'{period_label.replace(" ", "_").replace("-", "-")}_{timestamp}.json'
        json_path = backup_dir / filename
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Sheets backup saved: {json_path.name}")

        # Keep only the last 168 hourly snapshots (7 days) to avoid filling disk
        all_snapshots = sorted(backup_dir.glob('*.json'), key=lambda p: p.stat().st_mtime)
        for old_file in all_snapshots[:-168]:
            old_file.unlink()
            logger.info(f"Old sheets backup removed: {old_file.name}")

    except Exception as e:
        logger.error(f"Sheets backup failed: {e}")


@tasks.loop(hours=6)
async def db_backup_task():
    """Backup SQLite database every 6 hours."""
    await bot.wait_until_ready()

    now = datetime.now(ZoneInfo('America/New_York'))
    timestamp = now.strftime('%Y-%m-%d_%H-%M_EST')
    period_label = get_period_id(datetime.now(timezone.utc))

    backup_dir = ROOT_DIR / 'backups' / 'database'
    backup_dir.mkdir(parents=True, exist_ok=True)

    try:
        filename = f'rocky_road_{period_label.replace(" ", "_")}_{timestamp}.db'
        db_backup_path = backup_dir / filename
        shutil.copy2(DB_PATH, db_backup_path)
        logger.info(f"Database backup saved: {db_backup_path.name}")
        size_kb = db_backup_path.stat().st_size / 1024
        asyncio.ensure_future(WebhookLogger.backup_saved(filename, size_kb))
        asyncio.ensure_future(WebhookLogger.backup_file(str(db_backup_path)))

        # Keep only the last 28 DB backups (7 days × 4/day)
        all_db_backups = sorted(backup_dir.glob('*.db'), key=lambda p: p.stat().st_mtime)
        for old_file in all_db_backups[:-28]:
            old_file.unlink()
            logger.info(f"Old DB backup removed: {old_file.name}")

    except Exception as e:
        logger.error(f"Database backup failed: {e}")
        asyncio.ensure_future(WebhookLogger.error("db_backup_task", e))


# Keep old name as alias so existing references don't break
@tasks.loop(hours=24)
async def backup_task():
    """Legacy daily backup — kept for compatibility. Real backups handled by sheets_backup_task and db_backup_task."""
    pass


# ==================== MONTHLY LEADERBOARD RESET ====================

@tasks.loop(time=AUTO_INVOICE_TIME)  # Runs daily at 14:00 EST, fires on 1st of each month
async def monthly_leaderboard_reset_task():
    """On the 1st of each month at 14:00 EST: post final leaderboard from last month
    to payroll channel and announce the reset.
    """
    await bot.wait_until_ready()

    now_est = datetime.now(EST)
    if now_est.day != 1:
        return

    payroll_channel = bot.get_channel(PAYROLL_CHANNEL_ID)
    if not payroll_channel:
        logger.error("Monthly leaderboard reset: payroll channel not found")
        return

    try:
        # Get last month's data
        if now_est.month == 1:
            prev_month, prev_year = 12, now_est.year - 1
        else:
            prev_month, prev_year = now_est.month - 1, now_est.year

        prev_month_name = datetime(prev_year, prev_month, 1).strftime('%B')

        db_periods = await db.get_all_period_revenues_this_month(prev_year, prev_month)

        # Merge all periods into totals for last month
        totals = {}
        for period, people in db_periods.items():
            for key, entry in people.items():
                if key in totals:
                    totals[key]['revenue'] += entry['revenue']
                    if entry['name'].isascii() and not totals[key]['name'].isascii():
                        totals[key]['name'] = entry['name']
                else:
                    totals[key] = {'name': entry['name'], 'revenue': entry['revenue']}

        sorted_totals = sorted(totals.values(), key=lambda x: x['revenue'], reverse=True)

        # Build final leaderboard embed
        embed = discord.Embed(
            title=f"🏆 Final Leaderboard — {prev_month_name} {prev_year}",
            description=f"Monthly leaderboard has been reset. New month: **{now_est.strftime('%B %Y')}**",
            color=Colors.SUCCESS,
            timestamp=datetime.now(timezone.utc)
        )
        apply_thumbnail(embed)

        if sorted_totals:
            medals = ['🥇', '🥈', '🥉']
            lines = []
            for i, entry in enumerate(sorted_totals[:10]):
                medal = medals[i] if i < 3 else f"{i+1}."
                lines.append(f"{medal} **{entry['name']}** — ${entry['revenue']:,.0f}")
            embed.add_field(name="Final Rankings", value="\n".join(lines), inline=False)
        else:
            embed.add_field(name="Final Rankings", value="No data for last month.", inline=False)

        await payroll_channel.send(embed=embed)
        logger.info(f"Monthly leaderboard reset posted for {prev_month_name} {prev_year}")

    except Exception as e:
        logger.error(f"Monthly leaderboard reset error: {e}")


# ==================== DEBTLIST COMMAND ====================
@bot.tree.command(name="debtlist", description="Show current debt preview for all staff")
@admin_only()
async def debtlist_command(interaction: discord.Interaction):
    """Show debt preview for all staff."""
    
    await interaction.response.defer(thinking=True)
    
    try:
        # Fetch fresh data from sheets
        drivers = sheets.get_driver_data()
        trainers = sheets.get_trainer_data()
        management = sheets.get_management_data()
        all_staff = drivers + trainers + management
        
        if not all_staff:
            error_msg = sheets.last_error or "No data found"
            embed = discord.Embed(
                title="❌ Error",
                description=f"Could not fetch data: {error_msg}\n\nMake sure sheets are shared with:\n`rocky-road-financial-bot@rocky-road-financial-bot.iam.gserviceaccount.com`",
                color=Colors.ERROR,
                timestamp=datetime.now(timezone.utc)
            )
            apply_thumbnail(embed)
            await interaction.followup.send(embed=embed)
            return
        
        raw_entries = []
        sheet_error = sheets.last_error
        
        # Compute current weekly period id to match invoices (ISO week, same as get_period_id)
        now = datetime.now(timezone.utc)
        period_id = get_period_id(now)
        year = now.year
        
        # 1. PROCESS SHEET DATA (DRIVERS/TRAINERS)
        for staff in all_staff:
            name = staff.get('name', 'Unknown')
            new_work = staff.get('total_to_business', 0)
            commission_rate = staff.get('commission_rate', DRIVER_COMMISSION)
            staff_type = staff.get('type', 'driver')
            
            # Find Discord Member (with unicode normalization for fancy names)
            member = None
            for m in interaction.guild.members:
                if names_match(m.display_name, name) or names_match(m.name, name):
                    member = m
                    break
            
            # Skip if LOA (they will be listed in LOA section only)
            if member and LOA_ROLE_ID in [r.id for r in member.roles]:
                continue
            
            # Calculate Debt
            member_id = None
            if member:
                member_id = member.id
            
            # Calculate charges for the current week based on revenue
            # Use calculate_amount_owed with 0 previous debt to show only CURRENT week's projection
            # This matches user request to not add up previous debt in this view
            base_amount, _ = calculate_amount_owed(new_work, 0, staff_type)
            
            # Total display debt is just the base amount for this week
            display_debt = base_amount
            
            emoji = "🚛" if staff_type == 'driver' else "🎓"
            line_text = f"{emoji} **{name}**\n└ Debt: `${display_debt:,.0f}` │ Made: `${new_work:,.0f}`"
            
            raw_entries.append({
                'name': name,
                'amount': display_debt,
                'line': line_text,
                'is_loa': False,
                'member_id': member_id,
                'clean_name': name.lower().strip(),
                'staff_type': staff_type
            })

        # 2. PROCESS LOA DATA
        loa_role = interaction.guild.get_role(LOA_ROLE_ID)
        loa_entries = []
        
        if loa_role:
            for member in loa_role.members:
                name = member.display_name
                line_text = f"🏖️ **{name}** ─ *(LOA - No Charge)*"
                
                loa_entries.append({
                    'name': name,
                    'amount': 0,
                    'line': line_text,
                    'is_loa': True,
                    'member_id': member.id,
                    'clean_name': name.lower().strip()
                })

        loa_ids = set(e['member_id'] for e in loa_entries if e['member_id'])
        loa_names = set(e['clean_name'] for e in loa_entries)
        drivers_list = []
        trainers_list = []
        management_list = []
        
        for entry in raw_entries:
            if (entry['member_id'] in loa_ids) or (entry['clean_name'] in loa_names):
                continue
            if entry['staff_type'] == 'driver':
                drivers_list.append(entry)
            elif entry['staff_type'] == 'trainer':
                trainers_list.append(entry)
            elif entry['staff_type'] == 'management':
                management_list.append(entry)

        total_projected = sum(item['amount'] for item in drivers_list) + \
                          sum(item['amount'] for item in trainers_list) + \
                          sum(item['amount'] for item in management_list)
        staff_on_loa = len(loa_entries)
        
        drivers_list.sort(key=lambda x: -x['amount'])
        trainers_list.sort(key=lambda x: -x['amount'])
        management_list.sort(key=lambda x: -x['amount'])
        loa_entries.sort(key=lambda x: x['name'])
        
        embed = discord.Embed(
            title="💰 Current Debt Preview",
            color=Colors.PRIMARY,
            timestamp=datetime.now(timezone.utc)
        )
        apply_thumbnail(embed)
        staff_count = len(drivers_list) + len(trainers_list) + len(management_list) + staff_on_loa
        embed.description = f"```\n📊 {staff_count} Staff  │  💵 ${total_projected:,.0f} Total  │  🏖️ {staff_on_loa} LOA\n```"
        
        if sheet_error:
            embed.add_field(
                name="⚠ Data Warning",
                value=f"Some sheet data could not be loaded:\n`{sheet_error}`",
                inline=False
            )
        
        def add_group_fields(title, items):
            if not items:
                return
            group_text = "\n".join([item['line'] for item in items])
            if len(group_text) > 1000:
                chunks = []
                current_chunk = []
                current_length = 0
                for item in items:
                    line_len = len(item['line'])
                    if current_length + line_len + 1 > 1000:
                        chunks.append("\n".join(current_chunk))
                        current_chunk = [item['line']]
                        current_length = line_len
                    else:
                        current_chunk.append(item['line'])
                        current_length += line_len + 1
                if current_chunk:
                    chunks.append("\n".join(current_chunk))
                for i, chunk in enumerate(chunks):
                    embed.add_field(name=f"{title} ({i+1}/{len(chunks)})", value=chunk, inline=False)
            else:
                embed.add_field(name=title, value=group_text, inline=False)
        
        add_group_fields("Drivers", drivers_list)
        add_group_fields("Trainers", trainers_list)
        add_group_fields("Management", management_list)
        if loa_entries:
            loa_text = "\n".join([e['line'] for e in loa_entries])
            if len(loa_text) > 1000:
                chunks = []
                current_chunk = []
                current_length = 0
                for e in loa_entries:
                    line_len = len(e['line'])
                    if current_length + line_len + 1 > 1000:
                        chunks.append("\n".join(current_chunk))
                        current_chunk = [e['line']]
                        current_length = line_len
                    else:
                        current_chunk.append(e['line'])
                        current_length += line_len + 1
                if current_chunk:
                    chunks.append("\n".join(current_chunk))
                for i, chunk in enumerate(chunks):
                    embed.add_field(name=f"LOA ({i+1}/{len(chunks)})", value=chunk, inline=False)
            else:
                embed.add_field(name="LOA", value=loa_text, inline=False)
        
        embed.add_field(name="", value=f"```fix\n💎 Total: ${total_projected:,.0f}\n```", inline=False)
        embed.set_footer(text=f"Requested by {interaction.user.display_name}")
        
        await interaction.followup.send(embed=embed)
        
        # Log to payroll channel (only if not already in payroll channel)
        payroll_channel = bot.get_channel(PAYROLL_CHANNEL_ID)
        if payroll_channel and interaction.channel_id != PAYROLL_CHANNEL_ID:
            await payroll_channel.send(embed=embed)
        
    except Exception as e:
        logger.error(f"Debtlist error: {e}")
        embed = discord.Embed(title="❌ Error", description=str(e), color=Colors.ERROR)
        await interaction.followup.send(embed=embed)

# ==================== STATS COMMAND ====================
@bot.tree.command(name="stats", description="View weekly leaderboard and payment status")
async def stats_command(interaction: discord.Interaction):
    """Display weekly stats."""
    await interaction.response.defer()
    
    try:
        now = datetime.now(timezone.utc)
        # Match invoice generation logic
        period_id = get_period_id(now)
        year = now.year
        
        top_earners = await db.get_top_earners(period_id, year, 10)
        unpaid = await db.get_unpaid_invoices()
        
        embed = discord.Embed(
            title="📊 Rocky Road Financial - Stats",
            description=f"**{period_id} {year}**",
            color=Colors.INFO,
            timestamp=now
        )
        apply_thumbnail(embed)
        
        if top_earners:
            medals = ['🥇', '🥈', '🥉', '4️⃣', '5️⃣', '6️⃣', '7️⃣', '8️⃣', '9️⃣', '🔟']
            leaderboard = []
            for i, earner in enumerate(top_earners):
                medal = medals[i] if i < len(medals) else f"{i+1}."
                leaderboard.append(f"{medal} **{earner['discord_username']}** - ${earner['revenue']:,.2f}")
            embed.add_field(name="🏆 Top Earners", value="\n".join(leaderboard) or "No data", inline=False)
        else:
            embed.add_field(name="🏆 Top Earners", value="No invoices yet", inline=False)
        
        if unpaid:
            unpaid_list = []
            total_unpaid = 0
            for inv in unpaid[:10]:
                unpaid_list.append(f"• **{inv['discord_username']}** - ${inv['total_due']:,.2f}")
                total_unpaid += inv['total_due']
            if len(unpaid) > 10:
                unpaid_list.append(f"... +{len(unpaid) - 10} more")
            embed.add_field(name=f"⚠️ Unpaid ({len(unpaid)})", value="\n".join(unpaid_list), inline=False)
            embed.add_field(name="💰 Outstanding", value=f"**${total_unpaid:,.2f}**", inline=False)
        else:
            embed.add_field(name="✅ Status", value="All paid!", inline=False)
        
        await interaction.followup.send(embed=embed)
        
        # Log to payroll channel (only if not already in payroll channel)
        payroll_channel = bot.get_channel(PAYROLL_CHANNEL_ID)
        if payroll_channel and interaction.channel_id != PAYROLL_CHANNEL_ID:
            embed.set_footer(text=f"Viewed by {interaction.user.display_name}")
            await payroll_channel.send(embed=embed)
        
    except Exception as e:
        logger.error(f"Stats error: {e}")
        embed = discord.Embed(title="❌ Error", description=str(e), color=Colors.ERROR)
        await interaction.followup.send(embed=embed)


def parse_period_string(period: str, current_year: int) -> tuple[str, int]:
    """Parse various period formats like 'Week 6', '6 week', 'Week 06 - February 2026'."""
    if not period:
        return None, current_year
        
    # Standard format: "Week XX - Month Year"
    # Try to extract year first
    target_year = current_year
    target_period = period
    
    # Check if ends with a 4-digit year
    parts = period.rsplit(' ', 1)
    if len(parts) == 2 and parts[1].isdigit() and len(parts[1]) == 4:
        target_period = parts[0]
        target_year = int(parts[1])
        
    # Normalize "6 week" or "Week 6" to "Week 06"
    match = re.search(r'(\d+)', target_period)
    if match:
        week_num = int(match.group(1))
        # If it's just a number or "X week" or "Week X"
        if 'week' in target_period.lower():
            # Try to find if there's a month name already
            month_match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)', target_period, re.I)
            if month_match:
                month_name = month_match.group(1).capitalize()
                target_period = f"Week {week_num:02d} - {month_name}"
            else:
                # Default to current month if not specified but it's a week number
                # This is a bit risky but better than failing
                now = datetime.now(timezone.utc)
                month_name = now.strftime('%B')
                target_period = f"Week {week_num:02d} - {month_name}"
                
    return target_period, target_year


# ==================== PAID LIST COMMAND ====================
@bot.tree.command(name="paidlist", description="View payment status for all staff for a specific week")
@app_commands.describe(period="Select a week to view (default: current week)")
async def paidlist_command(interaction: discord.Interaction, period: str = None):
    """Display payment status for all staff."""
    await interaction.response.defer()
    
    # Determine period
    now = datetime.now(timezone.utc)
    current_year = now.year
    
    if not period:
        target_period = get_period_id(now)
        target_year = current_year
    else:
        target_period, target_year = parse_period_string(period, current_year)
    
    try:
        current_period = get_period_id(now)
        invoices = []

        if target_period == current_period:
            # Current week — DB is source of truth (has correct total_due with previous_debt)
            # Fall back to sheets only for staff not yet in DB
            db_invoices = await db.get_all_invoices_for_period(target_period, target_year)

            if db_invoices:
                # DB has data — use it directly (correct totals with debt)
                invoices = db_invoices
            else:
                # DB empty — sheets fallback (pre-sync, no previous_debt yet)
                try:
                    all_staff = sheets.get_driver_data() + sheets.get_trainer_data() + sheets.get_management_data()
                except Exception as e:
                    logger.error(f"paidlist sheets error: {e}")
                    all_staff = []

                for staff in all_staff:
                    name = (staff.get('name') or '').strip()
                    if not name:
                        continue
                    staff_type        = staff.get('type', 'driver')
                    commission_amount = float(staff.get('total_to_business') or 0)
                    _, total_due      = calculate_amount_owed(commission_amount, 0, staff_type)
                    invoices.append({
                        'discord_username': name,
                        'total_due': total_due,
                        'status': 'Pending',
                        'paid_at': None,
                    })
        else:
            # Past week — DB only
            invoices = await db.get_all_invoices_for_period(target_period, target_year)

        if not invoices:
            embed = discord.Embed(
                title="📊 Payment Status",
                description=f"No data found for **{target_period} {target_year}**",
                color=Colors.WARNING
            )
            await interaction.followup.send(embed=embed)
            return
            
        # Organize data
        paid_list = []
        pending_list = []
        total_expected = 0
        total_collected = 0
        
        for inv in invoices:
            name = inv['discord_username']
            amount = inv['total_due']
            status = inv['status']
            
            if status == 'Paid':
                paid_at = ""
                if inv['paid_at']:
                    try:
                        dt = datetime.fromisoformat(inv['paid_at'])
                        paid_at = f" ({dt.strftime('%m/%d')})"
                    except:
                        pass
                paid_list.append(f"✅ **{name}** - ${amount:,.0f}{paid_at}")
                total_collected += amount
            else:
                pending_list.append(f"❌ **{name}** - ${amount:,.0f}")
            
            total_expected += amount
            
        # Create Embed
        embed = discord.Embed(
            title=f"📊 Payment Status: {target_period} {target_year}",
            description=f"**Collected:** ${total_collected:,.0f} / ${total_expected:,.0f}\n**Paid:** {len(paid_list)}/{len(invoices)} staff",
            color=Colors.INFO,
            timestamp=datetime.now(timezone.utc)
        )
        apply_thumbnail(embed)
        
        # Split lists if they are too long (Discord field limit is 1024 chars)
        def create_chunks(lines, max_chars=1000):
            chunks = []
            current_chunk = []
            current_len = 0
            for line in lines:
                if current_len + len(line) + 1 > max_chars:
                    chunks.append("\n".join(current_chunk))
                    current_chunk = [line]
                    current_len = len(line)
                else:
                    current_chunk.append(line)
                    current_len += len(line) + 1
            if current_chunk:
                chunks.append("\n".join(current_chunk))
            return chunks

        if paid_list:
            chunks = create_chunks(paid_list)
            for i, chunk in enumerate(chunks):
                name = f"✅ Paid ({len(paid_list)})" if i == 0 else "✅ Paid (cont.)"
                embed.add_field(name=name, value=chunk, inline=False)
        else:
            embed.add_field(name="✅ Paid", value="None yet", inline=False)
            
        if pending_list:
            chunks = create_chunks(pending_list)
            for i, chunk in enumerate(chunks):
                name = f"❌ Pending ({len(pending_list)})" if i == 0 else "❌ Pending (cont.)"
                embed.add_field(name=name, value=chunk, inline=False)
        else:
            embed.add_field(name="❌ Pending", value="All paid! 🎉", inline=False)
            
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        logger.error(f"Paidlist error: {e}")
        await interaction.followup.send(embed=discord.Embed(title="❌ Error", description=str(e), color=Colors.ERROR))

@paidlist_command.autocomplete('period')
async def paidlist_period_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    recent = await db.get_recent_periods(10)
    choices = []
    
    # Add current period (Week 7) if not in DB
    now = datetime.now(timezone.utc)
    current_period = get_period_id(now)
    current_year = now.year
    full_current = f"{current_period} {current_year}"
    
    # Check if current is already in recent
    is_in_recent = any(f"{r['month']} {r['year']}" == full_current for r in recent)
    
    if not is_in_recent:
        # Prepend current period (it's the latest)
        if current.lower() in full_current.lower():
            choices.append(app_commands.Choice(name=full_current + " (Current)", value=full_current))
            
    for r in recent:
        name = f"{r['month']} {r['year']}"
        if current.lower() in name.lower():
            choices.append(app_commands.Choice(name=name, value=name))
    return choices[:25]


# ==================== INVOICE COMMAND ====================
@bot.tree.command(name="invoice", description="Generate and send weekly invoices")
@admin_only()
@app_commands.describe(
    user="Specific user to invoice (optional)", 
    amount="Manual invoice amount (overrides sheets)",
    period="Specific period/week to invoice for (e.g. 'Week 06 - February 2026')"
)
async def invoice_command(interaction: discord.Interaction, user: discord.Member = None, amount: int = None, period: str = None):
    """Send invoices to all staff or a specific user."""

    await interaction.response.defer(ephemeral=True)
    asyncio.ensure_future(WebhookLogger.admin_action(
        interaction.user.display_name, "invoice",
        f"Target: {user.display_name if user else 'ALL'}" + (f" | Amount: ${amount:,}" if amount else "") + (f" | Period: {period}" if period else "")
    ))
    
    # Ensure members are cached
    if not interaction.guild.chunked:
        try:
            await interaction.guild.chunk()
        except:
            pass
    
    try:
        now = datetime.now(timezone.utc)
        
        # Determine period and year
        if period:
            period_id, year = parse_period_string(period, now.year)
        else:
            period_id = get_period_id(now)
            year = now.year
        
        # --- FETCH STAFF DATA ---
        success = 0
        failed = 0
        results = []
        invoices_to_send = []
        already_paid = []
        not_found = []
        on_loa = []

        # Check if we already have data in DB for this period
        # If we are invoicing for a PAST period (like Week 6), we should prefer the DB state
        db_invoices = await db.get_all_invoices_for_period(period_id, year)
        
        if db_invoices and not user and amount is None:
            # We have historical data for this period!
            # Use the DB data instead of calculating from current sheets
            for inv in db_invoices:
                if inv['status'] == 'Paid':
                    already_paid.append(inv['discord_username'])
                    continue
                
                # Find member
                member = None
                uid = inv['discord_user_id']
                if uid.startswith('MIGRATED_'):
                    # Search by name for migrated users
                    logger.info(f"Searching for migrated user: {inv['discord_username']}")
                    for m in interaction.guild.members:
                        if names_match(m.display_name, inv['discord_username']) or names_match(m.name, inv['discord_username']):
                            member = m
                            logger.info(f"Found migrated user: {m.display_name}")
                            break
                else:
                    try:
                        member = interaction.guild.get_member(int(uid))
                    except ValueError:
                        member = None
                    
                    # Fallback to name search if ID not found or invalid
                    if not member:
                        for m in interaction.guild.members:
                            if names_match(m.display_name, inv['discord_username']) or names_match(m.name, inv['discord_username']):
                                member = m
                                break

                if not member:
                    logger.warning(f"Member NOT found: {inv['discord_username']} (ID: {uid})")
                    not_found.append(inv['discord_username'])
                    continue

                invoices_to_send.append({
                    'member': member,
                    'name': inv['discord_username'],
                    'staff_type': inv['staff_type'],
                    'revenue': inv['revenue'],
                    'base_amount': inv['total_due'],
                    'total_due': inv['total_due'],
                    'profit_from_sheet': inv.get('profit_from_sheet', 0),
                })
        else:
            # Case 1: Manual amount for a specific user (doesn't need sheet data)
            if user and amount is not None:
                # Check LOA
                if LOA_ROLE_ID in [r.id for r in user.roles]:
                    on_loa.append(user.display_name)
                else:
                    # Check if already paid
                    status = await db.get_invoice_status_for_period(str(user.id), period_id, year)
                    if status == 'Paid':
                        already_paid.append(user.display_name)
                    else:
                        invoices_to_send.append({
                            'member': user,
                            'name': user.display_name,
                            'staff_type': 'driver',
                            'revenue': 0,
                            'base_amount': amount,
                            'total_due': amount,
                            'profit_from_sheet': 0,
                        })
            
            # Case 2: General invoicing or specific user from Sheets
            else:
                drivers = sheets.get_driver_data()
                trainers = sheets.get_trainer_data()
                management = sheets.get_management_data()
                all_staff = drivers + trainers + management
                
                if not all_staff:
                    embed = discord.Embed(title="❌ Error", description="No data from sheets and no manual override provided.", color=Colors.ERROR)
                    await interaction.followup.send(embed=embed)
                    return

                # Determine who to invoice
                for staff in all_staff:
                    name = staff.get('name')
                    staff_type = staff.get('type', 'driver')
                    
                    # Find member
                    member = None
                    for m in interaction.guild.members:
                        if names_match(m.display_name, name) or names_match(m.name, name):
                            member = m
                            break
                    
                    if not member:
                        if not user:
                            not_found.append(name)
                        continue
                    
                    # Filter for specific user if requested
                    if user and member.id != user.id:
                        continue
                    
                    # All staff types are invoiced

                    # Check LOA
                    if LOA_ROLE_ID in [r.id for r in member.roles]:
                        on_loa.append(name)
                        continue
                    
                    # Calculate values
                    if amount is not None:
                        revenue     = 0
                        base_amount = float(amount)
                        total_due   = float(amount)
                    else:
                        revenue = staff.get('total_to_business', 0)
                        previous_debt = await db.get_older_debt(str(member.id), period_id, year)
                        base_amount, total_due = calculate_amount_owed(revenue, previous_debt, staff_type)

                    # Check if already paid
                    status = await db.get_invoice_status_for_period(str(member.id), period_id, year)
                    if status == 'Paid':
                        already_paid.append(name)
                        continue

                    # Add to list for confirmation
                    invoices_to_send.append({
                        'member': member,
                        'name': name,
                        'staff_type': staff_type,
                        'revenue': revenue,
                        'base_amount': base_amount,
                        'previous_debt': previous_debt if amount is None else 0,
                        'total_due': total_due,
                        'profit_from_sheet': staff.get('profit_from_sheet', 0),
                    })

        if not invoices_to_send:
            desc = f"No pending invoices to send for **{period_id} {year}**.\n\n"
            if already_paid: desc += f"✅ **Already Paid:** {', '.join(already_paid[:10])}{'...' if len(already_paid) > 10 else ''}\n"
            if on_loa: desc += f"🏖️ **On LOA:** {', '.join(on_loa[:10])}\n"
            if not_found: desc += f"⚠️ **Not Found:** {', '.join(not_found[:10])}\n"
            
            embed = discord.Embed(title="ℹ️ No Invoices to Send", description=desc, color=Colors.INFO)
            await interaction.followup.send(embed=embed)
            return

        # Create Preview Embed
        preview = discord.Embed(
            title="📋 Invoice Preview",
            description=f"Review the list below before sending invoices for **{period_id} {year}**.",
            color=Colors.INFO
        )
        
        preview_text = ""
        # Increased preview limit to 25 to show all users
        for inv in invoices_to_send[:25]:
            preview_text += f"• **{inv['name']}**: ${inv['total_due']:,.0f}\n"
        
        if len(invoices_to_send) > 25:
            preview_text += f"*...and {len(invoices_to_send) - 25} more*"
            
        preview.add_field(name=f"To be Invoiced ({len(invoices_to_send)})", value=preview_text or "None", inline=False)
        
        if not_found:
            nf_text = ", ".join(not_found[:10])
            if len(not_found) > 10: nf_text += f" (+{len(not_found)-10} more)"
            preview.add_field(name="⚠️ Users Not Found", value=nf_text, inline=False)

        if already_paid:
            preview.add_field(name="Already Paid", value=f"{len(already_paid)} users skipped", inline=True)
        if on_loa:
            preview.add_field(name="On LOA", value=f"{len(on_loa)} users skipped", inline=True)

        view = InvoiceConfirmationView(interaction, invoices_to_send, period_id, year, amount is not None)
        await interaction.followup.send(embed=preview, view=view)
        
    except Exception as e:
        logger.error(f"Invoice error: {e}")
        await interaction.followup.send(embed=discord.Embed(title="❌ Error", description=str(e), color=Colors.ERROR))


@invoice_command.autocomplete('period')
async def invoice_period_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    return await paidlist_period_autocomplete(interaction, current)


# ==================== PAY COMMAND ====================
@bot.tree.command(name="pay", description="Submit payment proof")
@app_commands.describe(screenshot="Your bank transfer screenshot")
async def pay_command(interaction: discord.Interaction, screenshot: discord.Attachment):
    """Submit payment proof. Debt from older unpaid weeks is already included in the invoice total."""
    await interaction.response.defer(ephemeral=True)

    try:
        user_id     = str(interaction.user.id)
        all_pending = await db.get_all_pending_for_user(user_id)

        if not all_pending:
            await interaction.followup.send(embed=discord.Embed(
                title="❌ No Invoice",
                description="You have no pending invoices. If you think this is wrong, contact an admin.",
                color=Colors.ERROR
            ), ephemeral=True)
            return

        # Always use the newest invoice — older debt is already rolled into total_due
        invoice  = all_pending[0]
        existing = await db.get_payment_log_by_invoice(invoice['id'])
        if existing and not existing['verified']:
            await interaction.followup.send(embed=discord.Embed(
                title="⚠️ Already Submitted",
                description=(
                    f"You already submitted payment for **{invoice['month']} {invoice['year']}** "
                    f"(${invoice['total_due']:,.0f}) — waiting for admin verification."
                ),
                color=Colors.WARNING
            ), ephemeral=True)
            return

        await _submit_payment(interaction, invoice, screenshot.url)

    except Exception as e:
        logger.error(f"Pay error: {e}")
        await interaction.followup.send(embed=discord.Embed(title="❌ Error", description=str(e), color=Colors.ERROR), ephemeral=True)


async def _submit_payment(interaction: discord.Interaction, invoice: dict, screenshot_url: str):
    """Shared helper — logs the payment to payroll channel and DB."""
    payroll_channel = bot.get_channel(PAYROLL_CHANNEL_ID)
    if not payroll_channel:
        await interaction.followup.send(embed=discord.Embed(
            title="❌ Error", description="Payroll channel not found.", color=Colors.ERROR
        ), ephemeral=True)
        return False

    now       = datetime.now(timezone.utc)
    user_id   = str(interaction.user.id)
    total_due = invoice['total_due']
    period    = invoice['month']
    year      = invoice['year']

    log_embed = discord.Embed(
        title="💳 Payment Submission",
        description=f"**{interaction.user.display_name}** submitted payment for **{period} {year}**",
        color=Colors.INFO,
        timestamp=now
    )
    log_embed.add_field(name="Amount Due", value=f"${total_due:,.0f}", inline=True)
    log_embed.add_field(name="Period",     value=f"{period} {year}",   inline=True)
    log_embed.add_field(name="User",       value=interaction.user.mention, inline=True)
    log_embed.set_image(url=screenshot_url)
    log_embed.set_footer(text="React ✅ to verify payment")

    log_msg = await payroll_channel.send(embed=log_embed)
    await log_msg.add_reaction('✅')
    await db.create_payment_log(invoice['id'], user_id, screenshot_url, str(log_msg.id))
    asyncio.ensure_future(WebhookLogger.payment_submitted(
        interaction.user.display_name, period, total_due, screenshot_url
    ))

    confirm = discord.Embed(
        title="✅ Payment Submitted",
        description=(
            f"Your payment for **{period} {year}** (${total_due:,.0f}) has been submitted for review.\n\n"
            f"An admin will verify it shortly. You'll get a DM confirmation."
        ),
        color=Colors.SUCCESS
    )
    await interaction.followup.send(embed=confirm, ephemeral=True)
    return True


@bot.tree.command(name="invoice_edit", description="Manually edit an invoice amount and optionally resend it (Admin only)")
@admin_only()
@app_commands.describe(
    user="The user whose invoice to edit",
    period="The period/week of the invoice (e.g. 'Week 09')",
    amount="The new amount to be charged",
    resend="Whether to send the updated invoice to the user's DM"
)
async def invoice_edit_command(interaction: discord.Interaction, user: discord.Member, period: str, amount: int, resend: bool = False):
    """Manually edit an invoice amount and optionally resend it."""
    await interaction.response.defer(ephemeral=True)
    
    try:
        now = datetime.now(timezone.utc)
        period_id, year = parse_period_string(period, now.year)
        
        # Update database
        success = await db.update_invoice_amount(str(user.id), period_id, year, float(amount))
        
        if not success:
            await interaction.followup.send(embed=discord.Embed(
                title="❌ Error", 
                description=f"No invoice found for **{user.display_name}** in **{period_id} {year}**.", 
                color=Colors.ERROR
            ))
            return
            
        embed = discord.Embed(
            title="✅ Invoice Updated",
            description=f"Invoice for **{user.display_name}** (**{period_id} {year}**) has been updated to **${amount:,.0f}**.",
            color=Colors.SUCCESS,
            timestamp=now
        )
        
        if resend:
            # Fetch invoice data for profit calculation
            invoice = await db.get_invoice_for_period(str(user.id), period_id, year)
            profit_val = invoice.get('profit_from_sheet', 0) if invoice else 0
            if profit_val <= 0:
                profit_val = max(0.0, float(amount)) # Fallback if no profit info
                
            invoice_embed = create_invoice_embed(user.display_name, float(amount), profit_val, period_id)
            try:
                await user.send(embed=invoice_embed)
                await db.mark_dm_sent(invoice['id']) # Track successful DM
                embed.add_field(name="DM Status", value="✅ Updated invoice sent to user.", inline=False)
            except:
                embed.add_field(name="DM Status", value="❌ Failed to send DM to user.", inline=False)
                
        await interaction.followup.send(embed=embed)
        
        # Log to payroll
        payroll_channel = bot.get_channel(PAYROLL_CHANNEL_ID)
        if payroll_channel:
            log_embed = discord.Embed(
                title="📝 Invoice Edited Manually",
                description=f"**{interaction.user.display_name}** edited **{user.display_name}**'s invoice for **{period_id} {year}**.",
                color=Colors.INFO,
                timestamp=now
            )
            log_embed.add_field(name="New Amount", value=f"`${amount:,.0f}`", inline=True)
            await payroll_channel.send(embed=log_embed)

    except Exception as e:
        logger.error(f"Invoice edit error: {e}")
        await interaction.followup.send(embed=discord.Embed(title="❌ Error", description=str(e), color=Colors.ERROR))

@invoice_edit_command.autocomplete('period')
async def invoice_edit_period_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    return await paidlist_period_autocomplete(interaction, current)


# ==================== PAID COMMAND (MANUAL) ====================
@bot.tree.command(name="paid", description="Manually mark a user's pending invoices as paid (Admin only)")
@admin_only()
@app_commands.describe(
    user="Type name or select user",
    period="Optional: Specific week (e.g. 'Week 06')"
)
async def paid_manual_command(interaction: discord.Interaction, user: str, period: str = None):
    """Manually mark a user as paid."""

    await interaction.response.defer(ephemeral=True)
    asyncio.ensure_future(WebhookLogger.admin_action(
        interaction.user.display_name, "paid",
        f"Target: {user}" + (f" | Period: {period}" if period else " | All pending")
    ))

    try:
        # 1. Try to resolve to a member first
        target_member = None
        target_id = None
        target_name = user
        
        # Check if it's a mention or ID
        clean_user = user.strip('<@!>')
        if clean_user.isdigit():
            target_member = interaction.guild.get_member(int(clean_user))
        
        # If not found yet, try name search
        if not target_member:
            # Ensure members are cached
            if not interaction.guild.chunked:
                try:
                    await interaction.guild.chunk()
                except:
                    pass
            
            for m in interaction.guild.members:
                if names_match(m.display_name, user) or names_match(m.name, user):
                    target_member = m
                    break
        
        if target_member:
            target_id = str(target_member.id)
            target_name = target_member.display_name
        else:
            # If still not found, search in DB for a user with this name to get their ID
            # (Handles migrated users who aren't in the server)
            async with aiosqlite.connect(DB_PATH) as conn:
                conn.row_factory = aiosqlite.Row
                # Try exact match first
                cursor = await conn.execute("SELECT discord_user_id, discord_username FROM invoices WHERE discord_username = ? LIMIT 1", (user,))
                row = await cursor.fetchone()
                
                # If no exact match, try fuzzy match
                if not row:
                    cursor = await conn.execute("SELECT discord_user_id, discord_username FROM invoices WHERE discord_username LIKE ? LIMIT 1", (f"%{user}%",))
                    row = await cursor.fetchone()
                    
                if row:
                    target_id = row['discord_user_id']
                    target_name = row['discord_username']
        
        if not target_id:
            await interaction.followup.send(embed=discord.Embed(title="❌ Error", description=f"Could not find user '{user}' in server or database.", color=Colors.ERROR))
            return

        # 2. If period is NOT provided, show pending invoices as buttons
        if not period:
            pending = await db.get_all_pending_for_user(target_id)
            if not pending:
                await interaction.followup.send(embed=discord.Embed(
                    title="✅ Nothing to Pay",
                    description=f"**{target_name}** has no pending invoices.",
                    color=Colors.INFO
                ), ephemeral=True)
                return

            lines = "\n".join(f"❌ **{inv['month']} {inv['year']}** — ${inv['total_due']:,.0f}" for inv in pending)
            embed = discord.Embed(
                title="💳 Mark as Paid",
                description=f"Pending invoices for **{target_name}**:\n\n{lines}\n\nSelect which week to mark as paid:",
                color=Colors.INFO
            )
            view = PaidPeriodSelectionView(target_id, target_name, interaction.user.display_name, pending)
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)
            return

        # 3. If period IS provided, execute immediately
        now = datetime.now(timezone.utc)
        target_period, target_year = parse_period_string(period, now.year)

        await db.mark_all_pending_invoices_paid(target_id, interaction.user.display_name, target_period, target_year)
        
        # Success message
        msg = f"Pending invoice for **{target_name}** (**{target_period} {target_year}**)"
        
        embed = discord.Embed(
            title="✅ Status Updated",
            description=f"{msg} has been marked as **Paid**.",
            color=Colors.SUCCESS,
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_footer(text=f"Verified by {interaction.user.display_name}")
        
        await interaction.followup.send(embed=embed)
        
        # Log to payroll channel
        payroll_channel = bot.get_channel(PAYROLL_CHANNEL_ID)
        if payroll_channel:
            log_embed = discord.Embed(
                title="💳 Manual Payment Verification",
                description=f"**{interaction.user.display_name}** manually marked **{target_name}** as paid for **{target_period} {target_year}**.",
                color=Colors.SUCCESS,
                timestamp=datetime.now(timezone.utc)
            )
            await payroll_channel.send(log_embed)

    except Exception as e:
        logger.error(f"Manual paid error: {e}")
        await interaction.followup.send(embed=discord.Embed(title="❌ Error", description=str(e), color=Colors.ERROR))

@paid_manual_command.autocomplete('user')
async def paid_user_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    choices = []
    
    # 1. Search in DB names
    db_names = await db.get_all_staff_names()
    for name in db_names:
        if current.lower() in name.lower():
            if name not in choices:
                choices.append(name)
        if len(choices) >= 15:
            break
            
    # 2. Search in current members if we have space
    if len(choices) < 25:
        # Ensure members are cached
        if not interaction.guild.chunked:
            try: await interaction.guild.chunk()
            except: pass
            
        for member in interaction.guild.members:
            if current.lower() in member.display_name.lower() or current.lower() in member.name.lower():
                if member.display_name not in choices:
                    choices.append(member.display_name)
            if len(choices) >= 25:
                break
                
    return [app_commands.Choice(name=name, value=name) for name in choices[:25]]

@paid_manual_command.autocomplete('period')
async def paid_period_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    return await paidlist_period_autocomplete(interaction, current)


@bot.tree.command(name="unpaid", description="Mark a previously paid invoice as unpaid (Admin only)")
@admin_only()
@app_commands.describe(
    user="Type name or select user",
    period="Specific week (e.g. 'Week 06')"
)
async def unpaid_manual_command(interaction: discord.Interaction, user: str, period: str):
    if not interaction.guild:
        return
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        target_member = None
        target_id = None
        target_name = user

        async with aiosqlite.connect(DB_PATH) as conn:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute(
                "SELECT discord_user_id, discord_username FROM invoices WHERE LOWER(discord_username) = LOWER(?) LIMIT 1",
                (user,)
            )
            row = await cursor.fetchone()
            
            if not row:
                cursor = await conn.execute(
                    "SELECT discord_user_id, discord_username FROM invoices WHERE discord_username LIKE ? COLLATE NOCASE LIMIT 1",
                    (f"%{user}%",)
                )
                row = await cursor.fetchone()
            
            if row:
                target_id = row['discord_user_id']
                target_name = row['discord_username']

        if not target_id:
            clean_user = user.strip('<@!>')
            if clean_user.isdigit():
                target_member = interaction.guild.get_member(int(clean_user))
            
            if not target_member:
                if not interaction.guild.chunked:
                    try:
                        await interaction.guild.chunk()
                    except:
                        pass
                
                for m in interaction.guild.members:
                    if names_match(m.display_name, user) or names_match(m.name, user):
                        target_member = m
                        break
            
            if target_member:
                target_id = str(target_member.id)
                target_name = target_member.display_name
        
        if not target_id:
            await interaction.followup.send(embed=discord.Embed(title="❌ Error", description=f"Could not find user '{user}' in server or database.", color=Colors.ERROR))
            return
        
        now = datetime.now(timezone.utc)
        target_period, target_year = parse_period_string(period, now.year)
        
        changed = await db.mark_invoice_unpaid(target_id, target_period, target_year)
        
        if not changed:
            async with aiosqlite.connect(DB_PATH) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute(
                    """
                    SELECT id, discord_username FROM invoices
                    WHERE month = ? AND year = ? AND staff_type != 'Manual'
                    """,
                    (target_period, target_year),
                )
                rows = await cursor.fetchall()
                
                match_row = None
                for row in rows:
                    if names_match(row["discord_username"], target_name):
                        match_row = row
                        break
                
                if match_row:
                    invoice_id = match_row["id"]
                    await conn.execute(
                        """
                        UPDATE invoices 
                        SET status = 'Pending', paid_at = NULL, verified_by = NULL
                        WHERE id = ?
                        """,
                        (invoice_id,),
                    )
                    await conn.execute(
                        """
                        UPDATE payment_logs
                        SET verified = 0, verified_by = NULL, verified_at = NULL
                        WHERE invoice_id = ?
                        """,
                        (invoice_id,),
                    )
                    await conn.commit()
                    changed = True
        
        if not changed:
            await interaction.followup.send(embed=discord.Embed(title="❌ Error", description=f"No invoice found for {target_name} in {target_period} {target_year}.", color=Colors.ERROR))
            return
        
        msg = f"Invoice for **{target_name}** (**{target_period} {target_year}**)"
        embed = discord.Embed(
            title="✅ Status Updated",
            description=f"{msg} has been marked as **Pending**.",
            color=Colors.WARNING,
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_footer(text=f"Reverted by {interaction.user.display_name}")
        
        await interaction.followup.send(embed=embed)
        
        payroll_channel = bot.get_channel(PAYROLL_CHANNEL_ID)
        if payroll_channel:
            log_embed = discord.Embed(
                title="↩️ Manual Payment Reverted",
                description=f"**{interaction.user.display_name}** marked **{target_name}** as unpaid for **{target_period} {target_year}**.",
                color=Colors.WARNING,
                timestamp=datetime.now(timezone.utc)
            )
            await payroll_channel.send(log_embed)
    
    except Exception as e:
        logger.error(f"Manual unpaid error: {e}")
        await interaction.followup.send(embed=discord.Embed(title="❌ Error", description=str(e), color=Colors.ERROR))


@unpaid_manual_command.autocomplete('user')
async def unpaid_user_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    return await paid_user_autocomplete(interaction, current)


@unpaid_manual_command.autocomplete('period')
async def unpaid_period_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    return await paidlist_period_autocomplete(interaction, current)


# ==================== UI COMPONENTS ====================

class RemindSelectMenu(discord.ui.Select):
    """Multi-select menu to choose who gets a reminder."""
    def __init__(self, pending_invoices: list, target_period: str, target_year: int, now: datetime):
        self.pending_invoices = pending_invoices
        self.target_period    = target_period
        self.target_year      = target_year
        self.now              = now

        options = []
        for inv in pending_invoices[:25]:
            username  = inv['discord_username']
            total_due = inv['total_due']
            days = 0
            if inv.get('created_at'):
                try:
                    created = datetime.fromisoformat(inv['created_at'])
                    if created.tzinfo is None:
                        created = created.replace(tzinfo=timezone.utc)
                    days = (now - created).days
                except Exception:
                    pass
            desc = f"${total_due:,.0f}" + (f" — {days}d overdue" if days > 0 else "")
            options.append(discord.SelectOption(
                label=username[:100], description=desc[:100], value=str(inv['id'])
            ))

        super().__init__(
            placeholder="Select staff to remind...",
            options=options, min_values=1, max_values=min(len(options), 25)
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        selected_ids      = {int(v) for v in self.values}
        selected_invoices = [inv for inv in self.pending_invoices if inv['id'] in selected_ids]
        await _send_reminders(interaction, selected_invoices, self.target_period, self.target_year, self.now)
        self.disabled = True
        await interaction.edit_original_response(view=self.view)


class RemindSelectView(discord.ui.View):
    def __init__(self, pending_invoices: list, target_period: str, target_year: int, now: datetime):
        super().__init__(timeout=300)
        self.pending_invoices = pending_invoices
        self.target_period    = target_period
        self.target_year      = target_year
        self.now              = now
        self.add_item(RemindSelectMenu(pending_invoices, target_period, target_year, now))

    @discord.ui.button(label="Send to ALL", style=discord.ButtonStyle.danger, row=1)
    async def send_all_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        await _send_reminders(interaction, self.pending_invoices, self.target_period, self.target_year, self.now)
        button.disabled = True
        await interaction.edit_original_response(view=self)


async def _send_reminders(interaction: discord.Interaction, invoices: list, target_period: str, target_year: int, now: datetime):
    """Shared helper — sends reminder DMs to selected invoices."""
    if not interaction.guild.chunked:
        try: await interaction.guild.chunk()
        except: pass

    sent, failed, not_found = 0, 0, 0
    sent_list, failed_list, nf_list = [], [], []

    for inv in invoices:
        user_id   = inv['discord_user_id']
        username  = inv['discord_username']
        total_due = inv['total_due']

        member = None
        try:
            member = interaction.guild.get_member(int(user_id))
            if not member:
                member = await interaction.guild.fetch_member(int(user_id))
        except (ValueError, discord.NotFound, discord.HTTPException):
            pass
        if not member:
            for m in interaction.guild.members:
                if names_match(m.display_name, username) or names_match(m.name, username):
                    member = m
                    break

        if not member:
            nf_list.append(f"⚠️ **{username}** — Not in server")
            not_found += 1
            continue

        days_overdue = 0
        if inv.get('created_at'):
            try:
                created = datetime.fromisoformat(inv['created_at'])
                if created.tzinfo is None:
                    created = created.replace(tzinfo=timezone.utc)
                days_overdue = (now - created).days
            except: pass

        older_pending = await db.get_all_pending_for_user(user_id)
        older_pending = [i for i in older_pending if i['id'] != inv['id']]

        revenue   = inv.get('revenue') or 0
        base      = inv.get('commission_amount') or 0
        profit_val = max(0.0, revenue - base)
        overdue_str = f" — **{days_overdue} day{'s' if days_overdue != 1 else ''} overdue**" if days_overdue > 0 else ""

        reminder_embed = discord.Embed(
            title="🔔 Payment Reminder — Invoice Outstanding",
            color=Colors.WARNING,
            timestamp=now
        )
        overdue_note = f" ({days_overdue}d overdue)" if days_overdue > 0 else ""
        reminder_embed.description = (
            f"⚠️ Your payment for **{target_period} {target_year}**"
            f" (${total_due:,.0f}) is still outstanding{overdue_note}.\n\n"
            "**To pay:**\n"
            f"1. Transfer **${total_due:,.0f}** to account `#4966714`\n"
            f"2. Reference: `Weekly payment {target_period}`\n"
            "3. Run `/pay` in the server and upload your screenshot\n"
            f"4. In the dropdown, select **{target_period}**"
        )
        reminder_embed.add_field(name="💰 Amount Due", value=f"`${total_due:,.0f}`", inline=True)
        reminder_embed.add_field(name="📅 Period", value=f"`{target_period} {target_year}`", inline=True)
        reminder_embed.add_field(name="🏦 Account", value="`#4966714`", inline=True)
        if older_pending:
            older_lines = '\n'.join(
                f'• **{i["month"]} {i["year"]}** — ${i["total_due"]:,.0f}'
                for i in older_pending[:5]
            )
            reminder_embed.add_field(
                name=f"⚠️ You also have {len(older_pending)} other unpaid invoice(s)",
                value=older_lines + "\n\nRun `/pay` — dropdown shows all unpaid weeks.",
                inline=False
            )

        try:
            await member.send(embed=reminder_embed)
            sent_list.append(f"✅ **{username}** — ${total_due:,.0f}" + (f" ({days_overdue}d)" if days_overdue else ""))
            sent += 1
        except discord.Forbidden:
            failed_list.append(f"❌ **{username}** — DMs disabled")
            failed += 1
        except Exception as e:
            failed_list.append(f"❌ **{username}** — {str(e)[:50]}")
            failed += 1

    def _chunks(lines, max_chars=1000):
        chunks, cur, cur_len = [], [], 0
        for line in lines:
            if cur_len + len(line) + 1 > max_chars:
                chunks.append("\n".join(cur))
                cur, cur_len = [line], len(line)
            else:
                cur.append(line)
                cur_len += len(line) + 1
        if cur: chunks.append("\n".join(cur))
        return chunks

    result_embed = discord.Embed(
        title="🔔 Reminders Sent",
        description=f"**Period:** {target_period} {target_year}\n✅ **{sent}** sent  |  ❌ **{failed}** failed  |  ⚠️ **{not_found}** not found",
        color=Colors.SUCCESS if sent > 0 else Colors.WARNING, timestamp=now
    )
    for i, chunk in enumerate(_chunks(sent_list)):
        result_embed.add_field(name=f"✅ Sent ({sent})" if i == 0 else "✅ (cont.)", value=chunk, inline=False)
    for i, chunk in enumerate(_chunks(failed_list)):
        result_embed.add_field(name=f"❌ Failed ({failed})" if i == 0 else "❌ (cont.)", value=chunk, inline=False)
    for i, chunk in enumerate(_chunks(nf_list)):
        result_embed.add_field(name=f"⚠️ Not in Server ({not_found})" if i == 0 else "⚠️ (cont.)", value=chunk, inline=False)
    result_embed.set_footer(text=f"Triggered by {interaction.user.display_name}")

    await interaction.followup.send(embed=result_embed, ephemeral=True)
    payroll_channel = bot.get_channel(PAYROLL_CHANNEL_ID)
    if payroll_channel:
        await payroll_channel.send(embed=result_embed)


class PaidPeriodButton(discord.ui.Button):
    """Button for a specific pending invoice period."""
    def __init__(self, month: str, year: int, total_due: float):
        super().__init__(
            label=f"{month} — ${total_due:,.0f}",
            style=discord.ButtonStyle.primary
        )
        self.month     = month
        self.year      = year
        self.total_due = total_due

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        view: PaidPeriodSelectionView = self.view

        await db.mark_all_pending_invoices_paid(view.target_id, view.admin_name, self.month, self.year)

        embed = discord.Embed(
            title="✅ Marked as Paid",
            description=f"**{view.target_name}** — **{self.month} {self.year}** (${self.total_due:,.0f}) marked as Paid.",
            color=Colors.SUCCESS,
            timestamp=datetime.now(timezone.utc)
        )
        await interaction.edit_original_response(embed=embed, view=None)

        payroll_channel = bot.get_channel(PAYROLL_CHANNEL_ID)
        if payroll_channel:
            await payroll_channel.send(embed=discord.Embed(
                title="💳 Manual Payment Verification",
                description=f"**{view.admin_name}** manually marked **{view.target_name}** as paid for **{self.month} {self.year}**.",
                color=Colors.SUCCESS,
                timestamp=datetime.now(timezone.utc)
            ))


class PaidPeriodSelectionView(discord.ui.View):
    """Shows pending invoices as buttons — admin picks which period to mark paid."""
    def __init__(self, target_id: str, target_name: str, admin_name: str, pending_invoices: list):
        super().__init__(timeout=180)
        self.target_id   = target_id
        self.target_name = target_name
        self.admin_name  = admin_name

        for inv in pending_invoices[:4]:
            self.add_item(PaidPeriodButton(inv['month'], inv['year'], inv['total_due']))

        # Mark ALL button
        all_btn = discord.ui.Button(label="Mark ALL as Paid", style=discord.ButtonStyle.danger)
        all_btn.callback = self.mark_all_callback
        self.add_item(all_btn)

        cancel_btn = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary)
        cancel_btn.callback = self.cancel_callback
        self.add_item(cancel_btn)

    async def mark_all_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        await db.mark_all_pending_invoices_paid(self.target_id, self.admin_name)

        embed = discord.Embed(
            title="✅ Marked as Paid",
            description=f"All pending invoices for **{self.target_name}** marked as Paid.",
            color=Colors.SUCCESS,
            timestamp=datetime.now(timezone.utc)
        )
        await interaction.edit_original_response(embed=embed, view=None)

        payroll_channel = bot.get_channel(PAYROLL_CHANNEL_ID)
        if payroll_channel:
            await payroll_channel.send(embed=discord.Embed(
                title="💳 Manual Payment Verification",
                description=f"**{self.admin_name}** manually marked **{self.target_name}** as paid (all pending).",
                color=Colors.SUCCESS,
                timestamp=datetime.now(timezone.utc)
            ))

    async def cancel_callback(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            embed=discord.Embed(title="❌ Cancelled", color=Colors.WARNING),
            view=None
        )

class InvoiceConfirmationView(discord.ui.View):
    def __init__(self, interaction: discord.Interaction, invoices_to_send: list, period_id: str, year: int, amount_is_manual: bool):
        super().__init__(timeout=300) # 5 minute timeout
        self.interaction = interaction
        self.invoices_to_send = invoices_to_send
        self.period_id = period_id
        self.year = year
        self.amount_is_manual = amount_is_manual

    @discord.ui.button(label="Confirm & Send", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.interaction.user.id:
            await interaction.response.send_message("This confirmation is not for you.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        self.stop()
        
        success = 0
        failed = 0
        results = []
        
        for inv in self.invoices_to_send:
            member = inv['member']
            name = inv['name']
            staff_type = inv['staff_type']
            revenue = inv['revenue']
            base_amount = inv['base_amount']
            total_due         = inv['total_due']
            profit_from_sheet = inv.get('profit_from_sheet', 0) or 0
            if profit_from_sheet <= 0:
                profit_from_sheet = max(0.0, revenue - base_amount)

            # Use pre-calculated previous_debt from preview if available,
            # otherwise re-query (for DB-path invoices)
            previous_debt = inv.get('previous_debt') if 'previous_debt' in inv else await db.get_older_debt(str(member.id), self.period_id, self.year)
            _, total_due_with_debt = calculate_amount_owed(base_amount, previous_debt, staff_type)
            result = await db.create_or_update_invoice_preserve_status(
                str(member.id), member.display_name, self.period_id, self.year,
                staff_type, revenue, base_amount, previous_debt, total_due_with_debt, profit_from_sheet
            )
            total_due = total_due_with_debt

            invoice_embed = create_invoice_embed(member.display_name, total_due_with_debt, profit_from_sheet, self.period_id, previous_debt)
            try:
                await member.send(embed=invoice_embed)
                await db.mark_dm_sent(result['id'])
                results.append(f"✅ {name} - ${total_due:,.0f}")
                asyncio.ensure_future(WebhookLogger.invoice_sent(name, self.period_id, total_due, previous_debt, dm_ok=True))
                success += 1
            except:
                results.append(f"❌ {name} - DM failed")
                asyncio.ensure_future(WebhookLogger.invoice_sent(name, self.period_id, total_due, previous_debt, dm_ok=False))
                failed += 1
        
        total_owed = sum(inv['total_due'] for inv in self.invoices_to_send)
        asyncio.ensure_future(WebhookLogger.invoice_batch(
            self.period_id, success, failed, total_owed
        ))

        summary = discord.Embed(
            title="📨 Weekly Invoices Sent",
            description=f"✅ {success} sent | ❌ {failed} failed\n**Period:** {self.period_id}",
            color=Colors.SUCCESS if failed == 0 else Colors.WARNING,
            timestamp=datetime.now(timezone.utc)
        )
        apply_thumbnail(summary)
        summary.add_field(name="Details", value="\n".join(results[:20]) or "None", inline=False)

        await interaction.followup.send(embed=summary)

        # Log to payroll
        payroll_channel = bot.get_channel(PAYROLL_CHANNEL_ID)
        if payroll_channel:
            await payroll_channel.send(embed=summary)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.red)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.interaction.user.id:
            await interaction.response.send_message("This confirmation is not for you.", ephemeral=True)
            return
            
        self.stop()
        await interaction.response.send_message("❌ Invoicing cancelled.", ephemeral=True)


# ==================== CLEAR SHEETS COMMAND ====================
@bot.tree.command(name="clearsheets", description="Manually clear all weekly sheet data (Admin only)")
@admin_only()
async def clearsheets_command(interaction: discord.Interaction):
    """Manually clear all driver, trainer and management sheets."""
    await interaction.response.defer(ephemeral=True)

    try:
        driver_result = sheets.clear_weekly_data(DRIVER_SHEET_ID)
        trainer_result = sheets.clear_weekly_data(TRAINER_SHEET_ID)
        mgmt_result = sheets.clear_weekly_data(MANAGEMENT_SHEET_ID)

        cleared = []
        failed = []

        for result, label in [(driver_result, "Drivers"), (trainer_result, "Trainers"), (mgmt_result, "Management")]:
            if result.get('success'):
                cleared.extend(result.get('cleared', []))
            else:
                failed.append(label)

        embed = discord.Embed(
            title="🧹 Sheets Cleared",
            description=f"Cleared **{len(cleared)}** worksheets across all trackers.",
            color=Colors.SUCCESS if not failed else Colors.WARNING,
            timestamp=datetime.now(timezone.utc)
        )
        apply_thumbnail(embed)

        if cleared:
            embed.add_field(name="✅ Cleared", value=", ".join(cleared[:20]) or "None", inline=False)
        if failed:
            embed.add_field(name="❌ Failed", value=", ".join(failed), inline=False)

        await interaction.followup.send(embed=embed)
        logger.info(f"Sheets manually cleared by {interaction.user.display_name}: {len(cleared)} worksheets")

    except Exception as e:
        logger.error(f"clearsheets error: {e}")
        await interaction.followup.send(embed=discord.Embed(title="❌ Error", description=str(e), color=Colors.ERROR))


@bot.tree.command(name="invoice_audit", description="Audit DMs to verify invoices were received (Admin only)")
@admin_only()
@app_commands.describe(
    period="The period/week to audit (e.g. 'Week 09')",
    check_history="Whether to actually scan DM history (slower but more accurate)"
)
async def invoice_audit_command(interaction: discord.Interaction, period: str, check_history: bool = False):
    """Audit DMs to verify invoices were received."""
    await interaction.response.defer(ephemeral=True)
    
    try:
        now = datetime.now(timezone.utc)
        period_id, year = parse_period_string(period, now.year)
        
        invoices = await db.get_all_invoices_for_period(period_id, year)
        
        if not invoices:
            await interaction.followup.send(f"No invoices found for **{period_id} {year}**.")
            return
            
        embed = discord.Embed(
            title=f"📋 Invoice Audit: {period_id} {year}",
            color=Colors.INFO,
            timestamp=now
        )
        
        audit_results = []
        for inv in invoices:
            name = inv['discord_username']
            user_id = inv['discord_user_id']
            db_sent = inv.get('dm_sent', 0)
            
            status_icon = "✅" if db_sent else "❌"
            history_icon = ""
            
            if check_history:
                member = interaction.guild.get_member(int(user_id))
                if member:
                    try:
                        # Try to find the invoice in last 10 messages
                        found_in_history = False
                        async for msg in member.history(limit=10):
                            if msg.author.id == bot.user.id and msg.embeds:
                                for e in msg.embeds:
                                    if "Invoice" in (e.title or "") and period_id in (e.description or ""):
                                        found_in_history = True
                                        break
                            if found_in_history: break
                        
                        history_icon = " (Verified in DM history)" if found_in_history else " (NOT found in DMs)"
                        if found_in_history and not db_sent:
                            await db.mark_dm_sent(inv['id']) # Correct DB if found
                    except:
                        history_icon = " (Could not access DMs)"
                else:
                    history_icon = " (User not in server)"
            
            audit_results.append(f"{status_icon} **{name}**{history_icon}")
            
        # Group results for display
        res_text = "\n".join(audit_results[:20])
        if len(audit_results) > 20:
            res_text += f"\n*...and {len(audit_results) - 20} more*"
            
        embed.add_field(name="Delivery Status (Database)", value=res_text or "None", inline=False)
        embed.set_footer(text="✅ = Sent successfully | ❌ = Delivery failed or not attempted")
        
        await interaction.followup.send(embed=embed)

    except Exception as e:
        logger.error(f"Audit error: {e}")
        await interaction.followup.send(embed=discord.Embed(title="❌ Error", description=str(e), color=Colors.ERROR))

@invoice_audit_command.autocomplete('period')
async def invoice_audit_period_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    return await paidlist_period_autocomplete(interaction, current)


# ==================== HELP COMMAND ====================
@bot.tree.command(name="help", description="Show all commands")
async def help_command(interaction: discord.Interaction):
    """Show help."""
    embed = discord.Embed(
        title="📚 Rocky Road Bot - Help",
        color=Colors.INFO
    )
    embed.description = BOT_DESCRIPTION
    embed.add_field(
        name="👤 Staff Commands",
        value=(
            "`/pay` - Submit payment proof\n"
            "`/mystatus` - Check your own invoice status & history\n"
            "`/stats` - View leaderboard & payment status\n"
            "`/leaderboard` - Monthly revenue rankings\n"
            "`/help` - Show this"
        ),
        inline=False
    )
    embed.add_field(
        name="🔐 Admin Commands",
        value=(
            "`/invoice` - Send invoices manually\n"
            "`/invoice_edit` - Edit existing invoice amount\n"
            "`/invoice_audit` - Verify DM delivery\n"
            "`/remind [period]` - Remind unpaid staff for a week\n"
            "`/debtlist` - Show debt preview from sheets\n"
            "`/debtadd` - Add debt to a user\n"
            "`/debtdel` - Remove debt from a user\n"
            "`/debtcheck` - Check user's debt records\n"
            "`/paid` - Mark a user as paid\n"
            "`/unpaid` - Revert a payment to pending\n"
            "`/paidlist [period]` - Payment status for a week\n"
            "`/invoicehistory` - Full history for a staff member\n"
            "`/total_revenue` - View revenue report"
        ),
        inline=False
    )
    embed.add_field(name="⏰ Auto-Invoice", value="Runs automatically every **Sunday at 2:00 PM EST**", inline=False)
    embed.add_field(name="✅ Verify", value="React ✅ in payroll channel to verify payments", inline=False)
    
    await interaction.response.send_message(embed=embed)


# ==================== REACTION HANDLER ====================
@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    """Handle payment verification."""
    if str(payload.emoji) != '✅' or payload.user_id == bot.user.id:
        return
    
    guild = bot.get_guild(payload.guild_id)
    if not guild:
        return
    
    member = guild.get_member(payload.user_id)
    if not member:
        try:
            member = await guild.fetch_member(payload.user_id)
        except Exception:
            logger.warning(f"on_raw_reaction_add: could not fetch member {payload.user_id}")
            return
    
    # Check role
    user_roles = [role.name.lower() for role in member.roles]
    if not any(role in user_roles for role in AUTHORIZED_ROLES):
        return
    
    payment_log = await db.get_payment_log_by_message(str(payload.message_id))
    if not payment_log or payment_log['verified']:
        return

    payment_log = dict(payment_log)  # ensure plain dict (not aiosqlite.Row)
    
    # Verify — mark ALL pending invoices for this user as paid
    # (the paid invoice already included older debt in total_due, so all are cleared)
    await db.verify_payment(str(payload.message_id), member.display_name)
    await db.mark_all_pending_invoices_paid(
        payment_log['discord_user_id'], member.display_name
    )
    
    # Update message
    channel = guild.get_channel(payload.channel_id)
    if channel:
        try:
            message = await channel.fetch_message(payload.message_id)
            embed = discord.Embed(
                title="✅ Payment Verified",
                description=f"**{payment_log['discord_username']}** - ${payment_log['total_due']:,.2f}",
                color=Colors.SUCCESS
            )
            embed.add_field(name="Verified by", value=member.display_name, inline=True)
            await message.edit(embed=embed)
        except:
            pass
    
    asyncio.ensure_future(WebhookLogger.payment_verified(
        payment_log['discord_username'],
        payment_log['month'],
        payment_log['total_due'],
        member.display_name
    ))

    # DM user
    try:
        staff = await guild.fetch_member(int(payment_log['discord_user_id']))
        dm_embed = discord.Embed(
            title="✅ Payment Confirmed",
            description=f"Your payment of **${payment_log['total_due']:,.2f}** has been verified!",
            color=Colors.SUCCESS
        )
        await staff.send(embed=dm_embed)
    except:
        pass

# ==================== TOTAL REVENUE COMMAND ====================
@bot.tree.command(name="total_revenue", description="View total business revenue and projected profit")
@admin_only()
async def total_revenue_command(interaction: discord.Interaction):
    """Show total revenue stats."""
    
    await interaction.response.defer()
    
    try:
        # 1. Fetch Data
        drivers = sheets.get_driver_data()
        trainers = sheets.get_trainer_data()
        management = sheets.get_management_data()
        all_staff = drivers + trainers + management
        
        if not all_staff:
            await interaction.followup.send(embed=discord.Embed(title="❌ Error", description="No data found in sheets.", color=Colors.ERROR))
            return

        # 2. Calculate Totals
        total_gross_revenue = 0.0
        total_projected_fees = 0.0
        driver_revenue = 0.0
        trainer_revenue = 0.0
        management_revenue = 0.0
        
        for staff in all_staff:
            revenue = staff.get('total_to_business', 0)
            commission_rate = staff.get('commission_rate', DRIVER_COMMISSION)
            staff_type = staff.get('type', 'driver')
            
            total_gross_revenue += revenue
            
            # Calculate fee
            fee = 0
            if staff_type == 'management':
                fee = revenue * commission_rate
            else:
                if revenue > 0:
                    calc_fee = revenue * commission_rate
                    fee = max(calc_fee, MINIMUM_FEE)
                else:
                    fee = MINIMUM_FEE 
            
            total_projected_fees += fee
            
            if staff_type == 'driver':
                driver_revenue += revenue
            elif staff_type == 'trainer':
                trainer_revenue += revenue
            elif staff_type == 'management':
                management_revenue += revenue

        # 3. Create Embed
        embed = discord.Embed(
            title="📈 Business Revenue Report",
            description=f"Summary for **{datetime.now().strftime('%B %Y')}**",
            color=Colors.GOLD,
            timestamp=datetime.now(timezone.utc)
        )
        
        embed.add_field(name="💰 Gross Revenue", value=f"```yaml\n${total_gross_revenue:,.2f}\n```", inline=False)
        embed.add_field(name="🚛 Driver Revenue", value=f"${driver_revenue:,.2f}", inline=True)
        embed.add_field(name="🎓 Trainer Revenue", value=f"${trainer_revenue:,.2f}", inline=True)
        embed.add_field(name="👔 Management Revenue", value=f"${management_revenue:,.2f}", inline=True)
        embed.add_field(name="", value="───────────────────", inline=False)
        embed.add_field(name="💎 Projected Net Profit (Fees)", value=f"**${total_projected_fees:,.2f}**", inline=False)
        embed.set_footer(text=f"Calculated from {len(all_staff)} active staff sheets")
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        logger.error(f"Revenue command error: {e}")
        await interaction.followup.send(embed=discord.Embed(title="❌ Error", description=str(e), color=Colors.ERROR))
        
        # ==================== LEADERBOARD COMMAND ====================
@bot.tree.command(name="leaderboard", description="Show monthly leaderboard — staff who have paid")
async def leaderboard_command(interaction: discord.Interaction):
    """Show leaderboard of staff who have paid this month."""
    await interaction.response.defer()

    try:
        now           = datetime.now(timezone.utc)
        display_year  = now.year
        display_month = now.month

        paid = await db.get_paid_this_month(display_year, display_month)

        # If current month has no paid invoices yet, fall back to previous month
        if not paid:
            prev_month = now.month - 1
            prev_year  = now.year
            if prev_month == 0:
                prev_month = 12
                prev_year -= 1
            paid = await db.get_paid_this_month(prev_year, prev_month)
            if paid:
                display_year  = prev_year
                display_month = prev_month

        if not paid:
            await interaction.followup.send(embed=discord.Embed(
                title="🏆 Leaderboard",
                description="No paid invoices found yet.",
                color=Colors.WARNING
            ))
            return

        display_month_name = datetime(display_year, display_month, 1).strftime('%B %Y')
        medals = {0: "🥇", 1: "🥈", 2: "🥉"}
        lines  = []
        for i, entry in enumerate(paid):
            name   = entry['discord_username']
            amount = entry['total_due']
            prefix = medals.get(i, f"**{i+1}.**")
            lines.append(f"{prefix} **{name}** — `${amount:,.0f}`")

        embed = discord.Embed(
            title="🏆 Monthly Leaderboard",
            description=f"**{display_month_name}** — Staff who have paid",
            color=Colors.GOLD,
            timestamp=now
        )
        apply_thumbnail(embed)
        embed.set_footer(text="Only confirmed paid invoices")

        chunks, cur, cur_len = [], [], 0
        for line in lines:
            if cur_len + len(line) + 1 > 1000:
                chunks.append("\n".join(cur))
                cur, cur_len = [line], len(line)
            else:
                cur.append(line)
                cur_len += len(line) + 1
        if cur:
            chunks.append("\n".join(cur))

        for i, chunk in enumerate(chunks):
            embed.add_field(
                name=f"Top Earners ({len(paid)} paid)" if i == 0 else "\u200b",
                value=chunk,
                inline=False
            )

        await interaction.followup.send(embed=embed)

    except Exception as e:
        logger.error(f"Leaderboard error: {e}")
        await interaction.followup.send(embed=discord.Embed(title="❌ Error", description=str(e), color=Colors.ERROR))


# ==================== DEBT ADD COMMAND ====================
@bot.tree.command(name="debtadd", description="Add debt to a user")
@admin_only()
@app_commands.describe(user="User to add debt to", amount="Amount to add", reason="Reason for addition")
async def debtadd_command(interaction: discord.Interaction, user: discord.Member, amount: float, reason: str = "Manual Adjustment"):
    """Add debt to a user."""

    if amount <= 0:
        await interaction.response.send_message(
            embed=discord.Embed(title="❌ Error", description="Amount must be positive.", color=Colors.ERROR),
            ephemeral=True
        )
        return
        
    await interaction.response.defer()
    
    try:
        await db.adjust_debt(str(user.id), user.display_name, amount, reason, interaction.user.display_name)
        
        embed = discord.Embed(
            title="✅ Debt Added",
            description=f"Added **${amount:,.2f}** to {user.mention}",
            color=Colors.SUCCESS
        )
        embed.add_field(name="Reason", value=reason, inline=False)
        embed.set_footer(text=f"Action by {interaction.user.display_name}")
        
        await interaction.followup.send(embed=embed)
        
        # Log to payroll channel (only if not already in payroll channel)
        payroll_channel = bot.get_channel(PAYROLL_CHANNEL_ID)
        if payroll_channel and interaction.channel_id != PAYROLL_CHANNEL_ID:
            await payroll_channel.send(embed=embed)
            
    except Exception as e:
        logger.error(f"Debtadd error: {e}")
        await interaction.followup.send(embed=discord.Embed(title="❌ Error", description=str(e), color=Colors.ERROR))


# ==================== DEBT DEL COMMAND ====================
@bot.tree.command(name="debtdel", description="Remove debt from a user")
@admin_only()
@app_commands.describe(user="User to remove debt from", amount="Amount to remove", reason="Reason for removal")
async def debtdel_command(interaction: discord.Interaction, user: discord.Member, amount: float, reason: str = "Manual Adjustment"):
    """Remove debt from a user."""

    if amount <= 0:
        await interaction.response.send_message(
            embed=discord.Embed(title="❌ Error", description="Amount must be positive.", color=Colors.ERROR),
            ephemeral=True
        )
        return
        
    await interaction.response.defer()
    
    try:
        # Removing debt is equivalent to adding negative debt
        await db.adjust_debt(str(user.id), user.display_name, -amount, reason, interaction.user.display_name)
        
        embed = discord.Embed(
            title="✅ Debt Removed",
            description=f"Removed **${amount:,.2f}** from {user.mention}",
            color=Colors.SUCCESS
        )
        embed.add_field(name="Reason", value=reason, inline=False)
        embed.set_footer(text=f"Action by {interaction.user.display_name}")
        
        await interaction.followup.send(embed=embed)
        
        # Log to payroll channel (only if not already in payroll channel)
        payroll_channel = bot.get_channel(PAYROLL_CHANNEL_ID)
        if payroll_channel and interaction.channel_id != PAYROLL_CHANNEL_ID:
            await payroll_channel.send(embed=embed)
            
    except Exception as e:
        logger.error(f"Debtdel error: {e}")
        await interaction.followup.send(embed=discord.Embed(title="❌ Error", description=str(e), color=Colors.ERROR))



# ==================== MY STATUS COMMAND ====================
@bot.tree.command(name="mystatus", description="Check your own invoice status and payment history")
async def mystatus_command(interaction: discord.Interaction):
    """Let staff check their own invoice status."""
    await interaction.response.defer(ephemeral=True)

    try:
        user_id = str(interaction.user.id)
        now = datetime.now(timezone.utc)

        async with aiosqlite.connect(DB_PATH) as conn:
            conn.row_factory = aiosqlite.Row
            # Get all invoices for this user, most recent first
            cursor = await conn.execute('''
                SELECT month, year, staff_type, total_due, status, paid_at, created_at
                FROM invoices
                WHERE discord_user_id = ?
                ORDER BY created_at DESC
                LIMIT 52
            ''', (user_id,))
            invoices = await cursor.fetchall()

            # Get total pending debt
            cursor2 = await conn.execute('''
                SELECT COALESCE(SUM(total_due), 0)
                FROM invoices
                WHERE discord_user_id = ? AND status = 'Pending'
            ''', (user_id,))
            total_pending = (await cursor2.fetchone())[0]

        if not invoices:
            embed = discord.Embed(
                title="📋 Your Invoice Status",
                description="No invoices found for your account. You either haven't been invoiced yet or your account isn't linked.",
                color=Colors.INFO,
                timestamp=now
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        streak = 0
        for inv in invoices:
            if inv['status'] != InvoiceStatus.PAID or not inv['paid_at']:
                break
            try:
                paid_dt = datetime.fromisoformat(inv['paid_at'])
                created_raw = inv['created_at']
                created_dt = datetime.fromisoformat(created_raw) if created_raw else None
                if paid_dt.tzinfo is None:
                    paid_dt = paid_dt.replace(tzinfo=timezone.utc)
                if created_dt and created_dt.tzinfo is None:
                    created_dt = created_dt.replace(tzinfo=timezone.utc)
                on_time = True
                if created_dt:
                    on_time = paid_dt <= created_dt + timedelta(days=7)
                if not on_time:
                    break
            except:
                break
            streak += 1

        embed = discord.Embed(
            title="📋 Your Invoice Status",
            description=f"**{interaction.user.display_name}**",
            color=Colors.SUCCESS if total_pending == 0 else Colors.WARNING,
            timestamp=now
        )
        apply_thumbnail(embed)

        # Outstanding debt callout
        if total_pending > 0:
            embed.add_field(
                name="⚠️ Outstanding Balance",
                value=f"```fix\n${total_pending:,.0f} due\n```",
                inline=False
            )
        else:
            embed.add_field(name="✅ Balance", value="All paid up!", inline=False)

        if streak > 0:
            embed.add_field(
                name="🔥 Payment Streak",
                value=f"{streak}-week on-time streak",
                inline=False
            )

        # Invoice history
        history_lines = []
        for inv in invoices:
            status_icon = "✅" if inv['status'] == InvoiceStatus.PAID else "❌"
            paid_str = ""
            if inv['paid_at']:
                try:
                    dt = datetime.fromisoformat(inv['paid_at'])
                    paid_str = f" — paid {dt.strftime('%b %d')}"
                except:
                    pass
            history_lines.append(
                f"{status_icon} **{inv['month']}** — `${inv['total_due']:,.0f}`{paid_str}"
            )

        embed.add_field(
            name="📅 Recent Invoices",
            value="\n".join(history_lines) or "None",
            inline=False
        )

        embed.set_footer(text="Use /pay to submit your payment proof")
        await interaction.followup.send(embed=embed, ephemeral=True)

    except Exception as e:
        logger.error(f"Mystatus error: {e}")
        await interaction.followup.send(
            embed=discord.Embed(
                title="❌ Error",
                description=str(e),
                color=Colors.ERROR,
                timestamp=datetime.now(timezone.utc)
            ),
            ephemeral=True
        )


# ==================== INVOICE HISTORY COMMAND ====================
@bot.tree.command(name="invoicehistory", description="View full invoice history for a staff member (Admin only)")
@admin_only()
@app_commands.describe(user="Staff member to look up (name or @mention)")
async def invoicehistory_command(interaction: discord.Interaction, user: str):
    """Show complete invoice history for any staff member across all weeks."""
    await interaction.response.defer(ephemeral=True)

    try:
        # Resolve the user - try mention/ID first, then name search, then DB search
        target_member = None
        target_id = None
        target_name = user

        clean_user = user.strip('<@!>')
        if clean_user.isdigit():
            target_member = interaction.guild.get_member(int(clean_user))

        if not target_member:
            if not interaction.guild.chunked:
                try:
                    await interaction.guild.chunk()
                except:
                    pass
            for m in interaction.guild.members:
                if names_match(m.display_name, user) or names_match(m.name, user):
                    target_member = m
                    break

        if target_member:
            target_id = str(target_member.id)
            target_name = target_member.display_name

        # Also search the DB by name for migrated/historical users
        async with aiosqlite.connect(DB_PATH) as conn:
            conn.row_factory = aiosqlite.Row

            if not target_id:
                cursor = await conn.execute(
                    "SELECT DISTINCT discord_user_id, discord_username FROM invoices WHERE LOWER(discord_username) = LOWER(?) LIMIT 1",
                    (user,)
                )
                row = await cursor.fetchone()
                if not row:
                    cursor = await conn.execute(
                        "SELECT DISTINCT discord_user_id, discord_username FROM invoices WHERE discord_username LIKE ? LIMIT 1",
                        (f"%{user}%",)
                    )
                    row = await cursor.fetchone()
                if row:
                    target_id = row['discord_user_id']
                    target_name = row['discord_username']

            if not target_id:
                await interaction.followup.send(
                    embed=discord.Embed(title="❌ Not Found", description=f"Could not find '{user}' in server or database.", color=Colors.ERROR),
                    ephemeral=True
                )
                return

            # Fetch all invoices — also try matching by name for migrated IDs
            # We collect all user_ids that could belong to this person
            cursor = await conn.execute(
                "SELECT DISTINCT discord_user_id FROM invoices WHERE discord_username LIKE ?",
                (f"%{target_name}%",)
            )
            all_ids = [row['discord_user_id'] for row in await cursor.fetchall()]
            if target_id not in all_ids:
                all_ids.append(target_id)

            placeholders = ','.join('?' * len(all_ids))
            cursor = await conn.execute(f'''
                SELECT month, year, staff_type, revenue, total_due, previous_debt, status, paid_at, created_at, verified_by
                FROM invoices
                WHERE discord_user_id IN ({placeholders})
                ORDER BY created_at DESC
            ''', all_ids)
            invoices = await cursor.fetchall()



        if not invoices:
            embed = discord.Embed(
                title=f"📋 Invoice History: {target_name}",
                description="No invoice records found.",
                color=Colors.WARNING
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        total_paid = sum(inv['total_due'] for inv in invoices if inv['status'] == InvoiceStatus.PAID)
        total_pending = sum(inv['total_due'] for inv in invoices if inv['status'] == InvoiceStatus.PENDING)

        embed = discord.Embed(
            title=f"📋 Invoice History: {target_name}",
            color=Colors.INFO,
            timestamp=datetime.now(timezone.utc)
        )

        embed.add_field(name="✅ Total Paid", value=f"`${total_paid:,.0f}`", inline=True)
        embed.add_field(name="❌ Total Pending", value=f"`${total_pending:,.0f}`", inline=True)

        # Build history lines
        lines = []
        for inv in invoices:
            status_icon = "✅" if inv['status'] == InvoiceStatus.PAID else "❌"
            paid_str = ""
            if inv['paid_at']:
                try:
                    dt = datetime.fromisoformat(inv['paid_at'])
                    verifier = f" by {inv['verified_by']}" if inv['verified_by'] else ""
                    paid_str = f" — {dt.strftime('%b %d')}{verifier}"
                except:
                    pass
            lines.append(f"{status_icon} **{inv['month']}** — `${inv['total_due']:,.0f}`{paid_str}")

        # Chunk into fields (Discord limit 1024 chars per field)
        chunks = []
        current, cur_len = [], 0
        for line in lines:
            if cur_len + len(line) + 1 > 1000:
                chunks.append("\n".join(current))
                current, cur_len = [line], len(line)
            else:
                current.append(line)
                cur_len += len(line) + 1
        if current:
            chunks.append("\n".join(current))

        for i, chunk in enumerate(chunks):
            name = "📅 Invoice History" if i == 0 else f"📅 Continued ({i+1})"
            embed.add_field(name=name, value=chunk, inline=False)

        embed.set_footer(text=f"Requested by {interaction.user.display_name} • {len(invoices)} records total")
        await interaction.followup.send(embed=embed, ephemeral=True)

    except Exception as e:
        logger.error(f"Invoice history error: {e}")
        await interaction.followup.send(
            embed=discord.Embed(title="❌ Error", description=str(e), color=Colors.ERROR),
            ephemeral=True
        )

@invoicehistory_command.autocomplete('user')
async def invoicehistory_user_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    return await paid_user_autocomplete(interaction, current)


# ==================== REMIND COMMAND ====================
@bot.tree.command(name="remind", description="Send reminders to unpaid staff for a specific week")
@admin_only()
@app_commands.describe(period="The week to send reminders for (e.g. 'Week 11 - March 2026')")
async def remind_command(interaction: discord.Interaction, period: str = None):
    """Shows a select menu — admin picks who to remind."""
    await interaction.response.defer(ephemeral=True)
    asyncio.ensure_future(WebhookLogger.admin_action(
        interaction.user.display_name, "remind",
        f"Period: {period or 'current week'}"
    ))

    try:
        now = datetime.now(timezone.utc)
        if period:
            target_period, target_year = parse_period_string(period, now.year)
        else:
            target_period = get_period_id(now)
            target_year   = now.year

        pending = await db.get_pending_invoices_for_period(target_period, target_year)

        if not pending:
            await interaction.followup.send(
                embed=discord.Embed(
                    title="✅ No Reminders Needed",
                    description=f"No pending invoices found for **{target_period} {target_year}**.",
                    color=Colors.SUCCESS
                ),
                ephemeral=True
            )
            return

        preview_lines = []
        for inv in pending[:25]:
            days = 0
            if inv.get('created_at'):
                try:
                    created = datetime.fromisoformat(inv['created_at'])
                    if created.tzinfo is None:
                        created = created.replace(tzinfo=timezone.utc)
                    days = (now - created).days
                except Exception:
                    pass
            overdue = f" — {days}d overdue" if days > 0 else ""
            preview_lines.append(f"❌ **{inv['discord_username']}** — ${inv['total_due']:,.0f}{overdue}")

        preview_embed = discord.Embed(
            title=f"🔔 Send Reminders — {target_period} {target_year}",
            description=(
                f"**{len(pending)}** unpaid invoices found.\n\n"
                + "\n".join(preview_lines)
                + (f"\n... +{len(pending) - 25} more" if len(pending) > 25 else "")
                + "\n\nSelect who to remind from the dropdown, or click **Send to ALL**."
            ),
            color=Colors.WARNING,
            timestamp=now
        )
        preview_embed.set_footer(text=f"Requested by {interaction.user.display_name} • Times out in 5 minutes")

        view = RemindSelectView(pending, target_period, target_year, now)
        await interaction.followup.send(embed=preview_embed, view=view, ephemeral=True)

    except Exception as e:
        logger.error(f"Remind error: {e}")
        await interaction.followup.send(
            embed=discord.Embed(title="❌ Error", description=str(e), color=Colors.ERROR),
            ephemeral=True
        )

@remind_command.autocomplete('period')
async def remind_period_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    return await paidlist_period_autocomplete(interaction, current)


# ==================== RUN ====================
def main():
    if not DISCORD_TOKEN:
        logger.error("No DISCORD_BOT_TOKEN found!")
        return
    
    logger.info("Starting Rocky Road Financial Bot...")
    bot.run(DISCORD_TOKEN)


if __name__ == '__main__':
    main()