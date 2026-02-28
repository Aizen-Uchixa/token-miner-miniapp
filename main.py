# What changed:
# - Refactored UI into compact 3-tab navigation (Home / Progress / Settings) with nested submenus.
# - Unified subscription bonus accounting around constants and added clearer verification UX with retry/join actions.
# - Added pipeline economy cycle (produce/move/deliver) for manual and passive processing, plus sell/auto-sell controls.
# - Added multi-resource drops on manual/passive activity and kept diamond chance upgrade with cap.
# - Added museum system (buy/upgrade/24h claim) and fossil collection with 50 species + assemble flow.
# - Added company profile + XP leveling and /setname command for company name updates.
# - Added multi-leaderboard categories (prestige/coins/gems/income/company level).
# - Added free 24h wheel rewards and section image toggle with per-session section rate limiting.
# - Rebalanced shop categories: auto/boost upgrades via gems, manager chests via coins.
# - Added subscription ledger entries (grant/clawback) for auditable bonus accounting.
#
# Assumptions:
# - Subscription bonus uses CREATOR_SUB_REWARD_COINS/GEMS as single source of truth.
# - “Income leaderboard” uses total earned metric for SQL safety/stability in JSON storage.
# - Section images use a public placeholder URL and are optional via settings.
# - Telegram per-user channel reaction verification is unsupported and intentionally not implemented.
#
# Manual test checklist:
# 1. Open Home/Progress/Settings tabs and verify compact keyboard layout.
# 2. From Home, open Upgrades submenu and perform each upgrade.
# 3. Press Work and verify cycle log (produced/moved/delivered/sold).
# 4. Toggle auto-sell and verify warehouse ore auto-converts on cycles.
# 5. Press Sell with non-empty warehouse and verify coins increase.
# 6. Buy Auto Manager in shop and verify passive income appears after interval.
# 7. Buy Diamond Luck and verify displayed chance increases (capped).
# 8. Force high luck and verify resource drops appear over repeated actions.
# 9. Verify no tick spam messages; pending diamond notice appears on menu open only.
# 10. Open Museum before purchase and verify buy flow/cost.
# 11. Buy museum, wait/simulate time, claim museum income after 24h gate.
# 12. Open Collection, gather parts, assemble fossil, verify exhibit boost impact.
# 13. Use /setname NewName and verify company name updates.
# 14. Verify company XP increases on upgrades/mine buys/chest opens/fossil assembly/prestige.
# 15. Open Leaderboards and check all 5 categories return data.
# 16. Open Free Spin menu, spin when ready, verify cooldown and reward result.
# 17. Open Settings -> Subscription, verify status + reaction limitation note shown.
# 18. If channel check fails, verify friendly admin-rights message + retry + join button.
# 19. Subscribe then /start to grant bonus once; unsubscribe then reopen menu to clawback.
# 20. Verify clawback can make balances negative and purchase checks still behave safely.
import asyncio
import json
import copy
import logging
import math
import os
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import aiosqlite
from dotenv import load_dotenv
from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update, WebAppInfo
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

# =========================
# Config
# =========================

load_dotenv()
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

DB_PATH = "game.db"
BACKUP_DIR = "saves"
WEBAPP_URL = (os.getenv("WEBAPP_URL") or "").strip()

BASE_TICK_SECONDS = 90
MIN_TICK_SECONDS = 15
MAX_TICK_SECONDS = 90
OFFLINE_CAP_SECONDS = 8 * 60 * 60
PRESTIGE_THRESHOLD = 1_000_000
PRESTIGE_BONUS_PER_POINT = 0.25
BOT_VERSION = "0.3.0"
BOT_CHANGELOG = "Season pass, quests, and meta progression."
BOT_SEASON_ID = "S1"
SEASON_START_TS = 1761955200.0
SEASON_DURATION_SEC = 30 * 24 * 60 * 60
CREATOR_CHANNEL = "@FGJHSGDU23"
CREATOR_CHANNEL_URL = "https://t.me/FGJHSGDU23"
CREATOR_SUB_REWARD_COINS = 25_000
CREATOR_SUB_REWARD_GEMS = 150
CREATOR_REACTION_REWARD_COINS = 150
ACTION_RATE_LIMIT_SECONDS = 0.4
AUTO_MANAGER_INTERVAL_SECONDS = 180
AUTO_MANAGER_MAX_LEVEL = 10
MANAGER_CHEST_PRICE = 250
SHOP_BOOST_PRICES = {
    "boost_x2": 50,
    "boost_x5": 150,
    "boost_x10": 300,
}
SHOP_CHEST_PRICES = {
    "chest_basic": 200,
    "chest_rare": 450,
}
MANAGER_POOL = ["miner_c", "hauler_c", "keeper_c", "engineer_r", "planner_r", "director_e"]
SUPPORTED_LANGS = {"en", "ru", "uz"}
UNSOLICITED_COOLDOWN_SECONDS = 10 * 60
DAILY_NOTIFY_COOLDOWN_SECONDS = 6 * 60 * 60
PRESTIGE_NOTIFY_COOLDOWN_SECONDS = 12 * 60 * 60
NOTIFIER_BATCH_SIZE = 200
ACTION_LIMIT_PER_MINUTE = 30
ECONOMY_MAX_VALUE = 10**18
PASS_PREMIUM_GEMS_COST = 2000
DIAMOND_BASE_CHANCE = 0.0016
DIAMOND_MAX_CHANCE = 0.05
DIAMOND_MAX_LEVEL = 31

MUSEUM_BUY_COST = 250_000
MUSEUM_UPGRADE_BASE_COST = 100_000
FOSSIL_EXHIBIT_BONUS = 0.15
FOSSIL_EXHIBIT_BONUS_CAP = 5.0
WHEEL_COOLDOWN_SECONDS = 24 * 60 * 60

RESOURCE_BASE_CHANCES = {
    "diamond": 0.0016,
    "gold": 0.0030,
    "iron": 0.0060,
    "copper": 0.0060,
    "silver": 0.0025,
    "aluminum": 0.0025,
    "bone": 0.0040,
    "fossil_fragment": 0.0010,
    "rare_dino_part": 0.0002,
}


def build_fossil_species() -> dict[str, dict[str, Any]]:
    species: dict[str, dict[str, Any]] = {}
    for i in range(1, 51):
        cnt = 3 + (i % 4)
        parts = [f"part_{j}" for j in range(1, cnt + 1)]
        species[f"fossil_{i:02d}"] = {"name": f"Fossil Species {i:02d}", "parts": parts}
    return species


FOSSIL_SPECIES = build_fossil_species()

EXPEDITIONS_CONFIG: dict[str, dict[str, Any]] = {
    "survey_15m": {
        "name": "Survey Run",
        "duration_sec": 15 * 60,
        "reward": {"coins": 2500, "gems": 8},
    },
    "route_30m": {
        "name": "Route Sweep",
        "duration_sec": 30 * 60,
        "reward": {"coins": 6500, "gems": 14, "boost_x2": 1},
    },
    "relay_1h": {
        "name": "Logistics Relay",
        "duration_sec": 60 * 60,
        "reward": {"coins": 15000, "gems": 25, "chest_basic": 1},
    },
    "deep_2h": {
        "name": "Deep Scan",
        "duration_sec": 2 * 60 * 60,
        "reward": {"coins": 34000, "gems": 42, "chest_basic": 1, "manager_fragments": 4},
    },
    "frontier_4h": {
        "name": "Frontier Trip",
        "duration_sec": 4 * 60 * 60,
        "reward": {"coins": 82000, "gems": 70, "chest_rare": 1, "manager_fragments": 7},
    },
    "convoy_8h": {
        "name": "Long Convoy",
        "duration_sec": 8 * 60 * 60,
        "reward": {"coins": 180000, "gems": 120, "chest_rare": 1, "boost_x5": 1, "manager_fragments": 12},
    },
}

CHESTS_CONFIG: dict[str, dict[str, Any]] = {
    "chest_basic": {"name": "Basic Chest"},
    "chest_rare": {"name": "Rare Chest"},
}

MANAGERS_CONFIG: dict[str, dict[str, Any]] = {
    "miner_c": {
        "name": "Tunnel Operator",
        "rarity": "common",
        "slot_affinity": "shaft",
        "passive_base": 1.08,
        "passive_step": 0.04,
        "active_mult": 2.0,
        "active_duration": 120,
        "cooldown": 900,
    },
    "hauler_c": {
        "name": "Lift Coordinator",
        "rarity": "common",
        "slot_affinity": "elevator",
        "passive_base": 1.08,
        "passive_step": 0.04,
        "active_mult": 2.0,
        "active_duration": 120,
        "cooldown": 900,
    },
    "keeper_c": {
        "name": "Warehouse Chief",
        "rarity": "common",
        "slot_affinity": "warehouse",
        "passive_base": 1.08,
        "passive_step": 0.04,
        "active_mult": 2.0,
        "active_duration": 120,
        "cooldown": 900,
    },
    "engineer_r": {
        "name": "Field Engineer",
        "rarity": "rare",
        "slot_affinity": "any",
        "passive_base": 1.14,
        "passive_step": 0.05,
        "active_mult": 2.5,
        "active_duration": 150,
        "cooldown": 1200,
    },
    "planner_r": {
        "name": "Shift Planner",
        "rarity": "rare",
        "slot_affinity": "any",
        "passive_base": 1.13,
        "passive_step": 0.05,
        "active_mult": 2.4,
        "active_duration": 150,
        "cooldown": 1200,
    },
    "director_e": {
        "name": "Operations Director",
        "rarity": "epic",
        "slot_affinity": "any",
        "passive_base": 1.2,
        "passive_step": 0.06,
        "active_mult": 3.0,
        "active_duration": 180,
        "cooldown": 1500,
    },
}

PASS_MAX_LEVEL = 30


def build_pass_rewards() -> dict[int, dict[str, dict[str, int]]]:
    rewards: dict[int, dict[str, dict[str, int]]] = {}
    for lvl in range(1, PASS_MAX_LEVEL + 1):
        free: dict[str, int] = {"coins": 1000 * lvl}
        premium: dict[str, int] = {"coins": 2000 * lvl, "gems": 5 + lvl}

        if lvl % 2 == 0:
            free["gems"] = 3 + lvl // 2
        if lvl % 3 == 0:
            free["chest_basic"] = 1
        if lvl % 5 == 0:
            free["boost_x2"] = 1

        if lvl % 2 == 0:
            premium["chest_basic"] = 1
        if lvl % 4 == 0:
            premium["chest_rare"] = 1
        if lvl % 5 == 0:
            premium["manager_fragments"] = 4 + lvl // 5
        if lvl % 6 == 0:
            premium["boost_x5"] = 1

        rewards[lvl] = {"free": free, "premium": premium}
    return rewards


PASS_REWARDS = build_pass_rewards()

QUESTS_CONFIG: dict[str, list[dict[str, Any]]] = {
    "daily": [
        {"id": "upgrade_any_5", "event": "upgrade_any", "target": 5, "reward": {"season_xp": 60, "gems": 10}},
        {"id": "earn_50000", "event": "earn_coins", "target": 50_000, "reward": {"season_xp": 70, "gems": 12}},
        {"id": "open_chest_1", "event": "open_chest", "target": 1, "reward": {"season_xp": 65, "boost_x2": 1}},
        {"id": "start_expedition_1", "event": "start_expedition", "target": 1, "reward": {"season_xp": 55, "gems": 8}},
        {"id": "activate_ability_1", "event": "activate_ability", "target": 1, "reward": {"season_xp": 55, "gems": 8}},
    ],
    "weekly": [
        {"id": "complete_expeditions_5", "event": "complete_expedition", "target": 5, "reward": {"season_xp": 250, "gems": 35, "chest_basic": 1}},
        {"id": "upgrade_components_50", "event": "upgrade_any", "target": 50, "reward": {"season_xp": 300, "gems": 45, "chest_rare": 1}},
        {"id": "earn_1000000", "event": "earn_coins", "target": 1_000_000, "reward": {"season_xp": 320, "gems": 50, "manager_fragments": 10}},
        {"id": "open_chest_10", "event": "open_chest", "target": 10, "reward": {"season_xp": 280, "gems": 40}},
        {"id": "activate_ability_10", "event": "activate_ability", "target": 10, "reward": {"season_xp": 260, "boost_x5": 1, "gems": 30}},
    ],
}

I18N = {
    "en": {
        "menu_title": "Mine",
        "coins": "Coins",
        "gems": "Gems",
        "prestige": "Prestige",
        "auto_pro": "Auto PRO",
        "tick": "Tick",
        "income_per_tick": "Income per tick",
        "line_stats": "Production line",
        "bottleneck": "Bottleneck",
        "rates": "Rates S/E/W",
        "work": "Work",
        "shaft": "Shaft",
        "elevator": "Elevator",
        "warehouse": "Warehouse",
        "mine_level": "Mine level",
        "boosts": "Boosts",
        "prestige_btn": "Prestige",
        "mines": "Mines",
        "research": "Research",
        "leaders": "Leaders",
        "stats": "Stats",
        "shop": "Shop",
        "advisor": "Advisor",
        "sub_bonus": "Subscribe bonus",
        "refresh": "Refresh",
        "back": "Back",
        "not_enough_coins": "Not enough coins",
        "not_enough_gems": "Not enough gems!",
        "too_fast": "Too fast",
        "updated": "Updated",
        "lang_button": "Language",
        "lang_menu_title": "Language",
        "lang_current": "Current language",
        "lang_changed_en": "Language set to English",
        "lang_changed_ru": "Language set to Russian",
        "lang_changed_uz": "Language set to Uzbek",
        "lang_auto_prompt": "We detected your Telegram language: *{lang_name}*. Switch language?",
        "lang_auto_yes": "Switch to {lang_name}",
        "lang_auto_no": "Keep English",
        "not_enough_gems_plain": "Not enough gems",
        "notifications": "Notifications",
        "notif_menu_title": "Notification settings",
        "notif_master": "Master notifications",
        "notif_manager_ready": "Manager ready",
        "notif_event_end": "Event ended",
        "notif_daily_ready": "Daily reward ready",
        "notif_prestige_ready": "Prestige ready",
        "notif_updates": "Update announcements",
        "notif_test": "Test notification",
        "notif_on": "ON",
        "notif_off": "OFF",
        "notif_test_msg": "This is a test notification.",
        "daily": "Daily",
        "daily_title": "Daily reward",
        "daily_ready": "Daily reward is ready",
        "daily_claim": "Claim reward",
        "daily_not_ready": "Not ready yet. Try again in {hours}h {minutes}m",
        "daily_claimed": "Reward claimed: +{coins} coins, +{gems} gems{boost_text}",
        "welcome_title": "Welcome to your mining project",
        "welcome_lines": "Press Work for instant income.\nUse Upgrades and Mines to scale.\nPassive income runs in the background.\nOffline earnings are calculated when you return.\nBoosts accelerate growth for a limited time.\nPrestige resets progress for permanent bonuses.",
        "update_title": "Bot updated",
        "update_text": "Bot updated to v{version}\n{changelog}",
        "announce_done": "Announcement sent: {ok} delivered, {failed} failed.",
        "announce_denied": "Access denied",
        "announce_usage": "Usage: /announce <text>",
        "daily_notify_text": "Daily reward is ready. Open the Daily menu.",
        "manager_ready_text": "Manager in slot {slot} is ready again.",
        "event_end_text": "Your event activity has ended.",
        "prestige_ready_text": "You can use prestige now.",
        "updates_opted_out": "Updates disabled in your settings.",
        "expeditions": "Expeditions",
        "inventory_menu": "Inventory",
        "managers_menu": "Managers",
        "abilities_menu": "Abilities",
        "exp_menu_title": "Expeditions",
        "exp_no_active": "No active expedition",
        "exp_active": "Active expedition",
        "exp_start": "Start",
        "exp_claim": "Claim",
        "exp_started": "Expedition started",
        "exp_already_active": "Finish current expedition first",
        "exp_not_ready": "Expedition is still running",
        "exp_claimed": "Expedition rewards claimed",
        "exp_done_notify": "Expedition completed. Open Expeditions to claim.",
        "inventory_title": "Inventory",
        "open_chest": "Open chest",
        "no_chest": "No chests of this type",
        "chest_result": "Chest reward: {result}",
        "managers_title": "Managers",
        "manager_assigned": "Assigned to slot {slot}",
        "manager_leveled": "Manager upgraded to level {level}",
        "manager_no_frags": "Not enough fragments",
        "manager_not_owned": "Manager not owned",
        "abilities_title": "Abilities",
        "ability_ready": "READY",
        "ability_active": "ACTIVE ({minutes}m {seconds}s)",
        "ability_cooldown": "COOLDOWN ({minutes}m {seconds}s)",
        "ability_activated": "Ability activated",
        "ability_not_ready": "Ability is not ready",
        "ability_no_manager": "No manager assigned",
        "slot_shaft": "Shaft",
        "slot_elevator": "Elevator",
        "slot_warehouse": "Warehouse",
        "notify_expedition_done": "Expedition done",
        "pass_menu": "Pass",
        "quests_menu": "Quests",
        "pass_title": "Season Pass",
        "pass_claim": "Claim rewards",
        "pass_buy_premium": "Buy Premium",
        "pass_premium_on": "Premium active",
        "pass_premium_off": "Premium inactive",
        "pass_claimed": "Pass rewards claimed",
        "pass_nothing": "No pass rewards available",
        "season_left": "Season time left",
        "season_level": "Season level",
        "season_xp": "Season XP",
        "quests_title": "Quests",
        "daily_quests": "Daily",
        "weekly_quests": "Weekly",
        "quest_claim": "Claim quest",
        "quest_claimed": "Quest reward claimed",
        "quest_not_ready": "Quest is not complete",
        "quest_completed": "Completed",
        "quest_progress": "Progress",
        "too_many_actions": "Action limit reached. Try a bit later.",
        "home_tab": "Home",
        "progress_tab": "Progress",
        "settings_tab": "Settings",
        "subscription_bonus": "Subscription bonus",
        "sub_status_subscribed": "Subscribed",
        "sub_status_not_subscribed": "Not subscribed",
        "sub_bonus_active": "Bonus active",
        "sub_bonus_inactive": "Bonus inactive",
        "sub_check": "Check subscription",
        "join_channel": "Join channel",
        "sub_verify_fail": "Cannot verify subscription right now. Please try later.",
        "sub_bonus_granted": "Subscription bonus granted",
        "sub_bonus_clawed": "Subscription bonus removed due to unsubscribe",
        "auto_manager": "Auto Manager",
        "diamond_luck": "Diamond Luck",
        "diamonds": "Diamonds",
        "diamond_chance": "Diamond chance",
        "about": "About",
        "company": "Company",
        "museum": "Museum",
        "collection": "Collection",
        "sell": "Sell",
        "auto_sell": "Auto-sell",
        "free_spin": "Free Spin",
        "spin_now": "Spin now",
        "spin_wait": "Next spin in",
        "donate": "Donate (coming soon)",
        "section_images": "Section images",
        "settings_title": "Settings",
        "home_title": "Home",
        "progress_title": "Progress",
        "company_name": "Company name",
        "company_level": "Company level",
        "company_xp": "Company XP",
        "museum_buy": "Buy museum",
        "museum_upgrade": "Upgrade museum",
        "museum_claim": "Claim museum income",
        "museum_not_owned": "Museum is not owned yet",
        "museum_claim_not_ready": "Museum claim is not ready yet",
        "museum_claimed": "Museum income claimed",
        "assemble": "Assemble",
        "leaderboards": "Leaderboards",
        "lb_prestige": "Prestige",
        "lb_coins": "Coins",
        "lb_gems": "Gems",
        "lb_income": "Income",
        "lb_company": "Company level",
        "setname_usage": "Usage: /setname <company name>",
        "setname_done": "Company name updated",
        "sub_need_admin": "Verification failed. Bot must be channel admin to check membership.",
        "sub_check_retry": "Retry check",
        "reaction_note": "Per-user channel reaction checks are not supported by Telegram Bot API.",
        "assumptions": "Assumptions",
    },
    "ru": {
        "menu_title": "Шахта",
        "coins": "Монеты",
        "gems": "Гемы",
        "prestige": "Престиж",
        "auto_pro": "Auto PRO",
        "tick": "Тик",
        "income_per_tick": "Доход за тик",
        "line_stats": "Линия добычи",
        "bottleneck": "Узкое место",
        "rates": "Скорости S/E/W",
        "work": "Работать",
        "shaft": "Шахта",
        "elevator": "Лифт",
        "warehouse": "Склад",
        "mine_level": "Уровень шахты",
        "boosts": "Бусты",
        "prestige_btn": "Престиж",
        "mines": "Шахты",
        "research": "Исследования",
        "leaders": "Лидеры",
        "stats": "Статистика",
        "shop": "Магазин",
        "advisor": "Совет",
        "sub_bonus": "Бонус за подписку",
        "refresh": "Обновить",
        "back": "Назад",
        "not_enough_coins": "Недостаточно монет",
        "not_enough_gems": "Недостаточно гемов!",
        "too_fast": "Слишком быстро",
        "updated": "Обновлено",
        "lang_button": "Язык",
        "lang_menu_title": "Язык",
        "lang_current": "Текущий язык",
        "lang_changed_en": "Язык переключен на English",
        "lang_changed_ru": "Язык переключен на Русский",
        "lang_changed_uz": "Язык переключен на O‘zbekcha",
        "lang_auto_prompt": "Мы обнаружили язык Telegram: *{lang_name}*. Переключить язык?",
        "lang_auto_yes": "Переключить на {lang_name}",
        "lang_auto_no": "Оставить English",
        "not_enough_gems_plain": "Недостаточно гемов",
        "notifications": "Уведомления",
        "notif_menu_title": "Настройки уведомлений",
        "notif_master": "Главные уведомления",
        "notif_manager_ready": "Менеджер готов",
        "notif_event_end": "Событие завершено",
        "notif_daily_ready": "Готова ежедневная награда",
        "notif_prestige_ready": "Престиж доступен",
        "notif_updates": "Уведомления об обновлениях",
        "notif_test": "Тест уведомления",
        "notif_on": "ВКЛ",
        "notif_off": "ВЫКЛ",
        "notif_test_msg": "Это тестовое уведомление.",
        "daily": "Ежедневно",
        "daily_title": "Ежедневная награда",
        "daily_ready": "Ежедневная награда готова",
        "daily_claim": "Получить награду",
        "daily_not_ready": "Пока не готово. Осталось {hours}ч {minutes}м",
        "daily_claimed": "Награда получена: +{coins} монет, +{gems} гемов{boost_text}",
        "welcome_title": "Добро пожаловать в ваш майнинг-проект",
        "welcome_lines": "Нажимайте Работать для мгновенного дохода.\nУлучшения и Шахты ускоряют рост.\nПассивный доход идет в фоне.\nОффлайн доход начисляется при возвращении.\nБусты ускоряют прогресс на время.\nПрестиж сбрасывает прогресс ради постоянного бонуса.",
        "update_title": "Бот обновлен",
        "update_text": "Бот обновлен до v{version}\n{changelog}",
        "announce_done": "Рассылка завершена: доставлено {ok}, ошибок {failed}.",
        "announce_denied": "Доступ запрещен",
        "announce_usage": "Использование: /announce <текст>",
        "daily_notify_text": "Ежедневная награда готова. Откройте меню Daily.",
        "manager_ready_text": "Менеджер в слоте {slot} снова готов.",
        "event_end_text": "Ваше событие завершено.",
        "prestige_ready_text": "Престиж уже доступен.",
        "updates_opted_out": "Обновления отключены в настройках.",
        "expeditions": "Экспедиции",
        "inventory_menu": "Инвентарь",
        "managers_menu": "Менеджеры",
        "abilities_menu": "Способности",
        "exp_menu_title": "Экспедиции",
        "exp_no_active": "Активной экспедиции нет",
        "exp_active": "Активная экспедиция",
        "exp_start": "Старт",
        "exp_claim": "Забрать",
        "exp_started": "Экспедиция запущена",
        "exp_already_active": "Сначала завершите текущую экспедицию",
        "exp_not_ready": "Экспедиция еще выполняется",
        "exp_claimed": "Награда экспедиции получена",
        "exp_done_notify": "Экспедиция завершена. Откройте меню Экспедиции.",
        "inventory_title": "Инвентарь",
        "open_chest": "Открыть сундук",
        "no_chest": "Сундуков этого типа нет",
        "chest_result": "Награда сундука: {result}",
        "managers_title": "Менеджеры",
        "manager_assigned": "Назначен в слот {slot}",
        "manager_leveled": "Менеджер улучшен до уровня {level}",
        "manager_no_frags": "Недостаточно фрагментов",
        "manager_not_owned": "Менеджер не открыт",
        "abilities_title": "Способности",
        "ability_ready": "ГОТОВ",
        "ability_active": "АКТИВНО ({minutes}м {seconds}с)",
        "ability_cooldown": "ПЕРЕЗАРЯДКА ({minutes}м {seconds}с)",
        "ability_activated": "Способность активирована",
        "ability_not_ready": "Способность еще не готова",
        "ability_no_manager": "Менеджер не назначен",
        "slot_shaft": "Шахта",
        "slot_elevator": "Лифт",
        "slot_warehouse": "Склад",
        "notify_expedition_done": "Экспедиция завершена",
        "pass_menu": "Пропуск",
        "quests_menu": "Квесты",
        "pass_title": "Сезонный пропуск",
        "pass_claim": "Забрать награды",
        "pass_buy_premium": "Купить Premium",
        "pass_premium_on": "Premium активен",
        "pass_premium_off": "Premium не активен",
        "pass_claimed": "Награды пропуска получены",
        "pass_nothing": "Нет доступных наград пропуска",
        "season_left": "До конца сезона",
        "season_level": "Уровень сезона",
        "season_xp": "XP сезона",
        "quests_title": "Квесты",
        "daily_quests": "Ежедневные",
        "weekly_quests": "Еженедельные",
        "quest_claim": "Забрать квест",
        "quest_claimed": "Награда квеста получена",
        "quest_not_ready": "Квест еще не выполнен",
        "quest_completed": "Выполнено",
        "quest_progress": "Прогресс",
        "too_many_actions": "Лимит действий достигнут. Попробуйте позже.",
        "home_tab": "Главная",
        "progress_tab": "Прогресс",
        "settings_tab": "Настройки",
        "subscription_bonus": "Бонус подписки",
        "sub_status_subscribed": "Подписан",
        "sub_status_not_subscribed": "Не подписан",
        "sub_bonus_active": "Бонус активен",
        "sub_bonus_inactive": "Бонус не активен",
        "sub_check": "Проверить подписку",
        "join_channel": "Перейти в канал",
        "sub_verify_fail": "Сейчас не удалось проверить подписку. Попробуйте позже.",
        "sub_bonus_granted": "Бонус подписки выдан",
        "sub_bonus_clawed": "Бонус подписки списан после отписки",
        "auto_manager": "Авто менеджер",
        "diamond_luck": "Алмазная удача",
        "diamonds": "Алмазы",
        "diamond_chance": "Шанс алмаза",
        "about": "О боте",
        "company": "Компания",
        "museum": "Музей",
        "collection": "Коллекция",
        "sell": "Продать",
        "auto_sell": "Автопродажа",
        "free_spin": "Бесплатное колесо",
        "spin_now": "Крутить",
        "spin_wait": "Следующий спин через",
        "donate": "Донат (скоро)",
        "section_images": "Изображения разделов",
        "settings_title": "Настройки",
        "home_title": "Главная",
        "progress_title": "Прогресс",
        "company_name": "Название компании",
        "company_level": "Уровень компании",
        "company_xp": "XP компании",
        "museum_buy": "Купить музей",
        "museum_upgrade": "Улучшить музей",
        "museum_claim": "Забрать доход музея",
        "museum_not_owned": "Музей еще не куплен",
        "museum_claim_not_ready": "Доход музея пока не готов",
        "museum_claimed": "Доход музея получен",
        "assemble": "Собрать",
        "leaderboards": "Таблица лидеров",
        "lb_prestige": "Престиж",
        "lb_coins": "Монеты",
        "lb_gems": "Гемы",
        "lb_income": "Доход",
        "lb_company": "Уровень компании",
        "setname_usage": "Использование: /setname <название>",
        "setname_done": "Название компании обновлено",
        "sub_need_admin": "Проверка не удалась. Бот должен быть админом канала для проверки.",
        "sub_check_retry": "Повторить проверку",
        "reaction_note": "Проверка реакций пользователей на посты канала недоступна в Telegram Bot API.",
        "assumptions": "Предположения",
    },
    "uz": {
        "menu_title": "Shaxta",
        "coins": "Tangalar",
        "gems": "Olmoslar",
        "prestige": "Prestij",
        "auto_pro": "Auto PRO",
        "tick": "Tik",
        "income_per_tick": "Har tik daromad",
        "line_stats": "Ishlab chiqarish liniyasi",
        "bottleneck": "Tor joy",
        "rates": "Tezliklar S/E/W",
        "work": "Ishlash",
        "shaft": "Shaxta",
        "elevator": "Lift",
        "warehouse": "Ombor",
        "mine_level": "Shaxta darajasi",
        "boosts": "Bustlar",
        "prestige_btn": "Prestij",
        "mines": "Shaxtalar",
        "research": "Tadqiqotlar",
        "leaders": "Liderlar",
        "stats": "Statistika",
        "shop": "Do'kon",
        "advisor": "Maslahat",
        "sub_bonus": "Obuna bonusi",
        "refresh": "Yangilash",
        "back": "Orqaga",
        "not_enough_coins": "Tangalar yetarli emas",
        "not_enough_gems": "Olmos yetarli emas!",
        "too_fast": "Juda tez",
        "updated": "Yangilandi",
        "lang_button": "Til",
        "lang_menu_title": "Til",
        "lang_current": "Joriy til",
        "lang_changed_en": "Til English ga o'zgartirildi",
        "lang_changed_ru": "Til Русский ga o'zgartirildi",
        "lang_changed_uz": "Til O‘zbekcha ga o'zgartirildi",
        "lang_auto_prompt": "Telegram tilingiz aniqlandi: *{lang_name}*. Tilni o'zgartiraymi?",
        "lang_auto_yes": "{lang_name} ga o'tish",
        "lang_auto_no": "English qoldirish",
        "not_enough_gems_plain": "Olmos yetarli emas",
        "notifications": "Bildirishnomalar",
        "notif_menu_title": "Bildirishnoma sozlamalari",
        "notif_master": "Asosiy bildirishnomalar",
        "notif_manager_ready": "Menejer tayyor",
        "notif_event_end": "Voqea tugadi",
        "notif_daily_ready": "Kunlik mukofot tayyor",
        "notif_prestige_ready": "Prestij tayyor",
        "notif_updates": "Yangilanish xabarlari",
        "notif_test": "Test xabarnoma",
        "notif_on": "YOQILGAN",
        "notif_off": "O'CHIQ",
        "notif_test_msg": "Bu test bildirishnoma.",
        "daily": "Kunlik",
        "daily_title": "Kunlik mukofot",
        "daily_ready": "Kunlik mukofot tayyor",
        "daily_claim": "Mukofotni olish",
        "daily_not_ready": "Hali tayyor emas. {hours}soat {minutes}daqiqadan keyin",
        "daily_claimed": "Mukofot olindi: +{coins} tanga, +{gems} olmos{boost_text}",
        "welcome_title": "Shaxta loyihangizga xush kelibsiz",
        "welcome_lines": "Darhol daromad uchun Ishlash tugmasini bosing.\nYangilanishlar va Shaxtalar o'sishni tezlatadi.\nPassiv daromad fon rejimida ishlaydi.\nQaytganingizda offline daromad hisoblanadi.\nBustlar vaqtincha o'sishni tezlashtiradi.\nPrestij taraqqiyotni reset qilib doimiy bonus beradi.",
        "update_title": "Bot yangilandi",
        "update_text": "Bot v{version} ga yangilandi\n{changelog}",
        "announce_done": "Xabar yuborildi: {ok} yetkazildi, {failed} xato.",
        "announce_denied": "Kirish rad etildi",
        "announce_usage": "Foydalanish: /announce <matn>",
        "daily_notify_text": "Kunlik mukofot tayyor. Daily menyusini oching.",
        "manager_ready_text": "{slot} slotidagi menejer yana tayyor.",
        "event_end_text": "Event yakunlandi.",
        "prestige_ready_text": "Hozir prestijdan foydalanishingiz mumkin.",
        "updates_opted_out": "Yangilanishlar sozlamada o'chirilgan.",
        "expeditions": "Ekspeditsiyalar",
        "inventory_menu": "Inventar",
        "managers_menu": "Menejerlar",
        "abilities_menu": "Qobiliyatlar",
        "exp_menu_title": "Ekspeditsiyalar",
        "exp_no_active": "Faol ekspeditsiya yo'q",
        "exp_active": "Faol ekspeditsiya",
        "exp_start": "Boshlash",
        "exp_claim": "Olish",
        "exp_started": "Ekspeditsiya boshlandi",
        "exp_already_active": "Avval joriy ekspeditsiyani yakunlang",
        "exp_not_ready": "Ekspeditsiya hali tugamagan",
        "exp_claimed": "Ekspeditsiya mukofoti olindi",
        "exp_done_notify": "Ekspeditsiya tugadi. Ekspeditsiyalar menyusini oching.",
        "inventory_title": "Inventar",
        "open_chest": "Sandiq ochish",
        "no_chest": "Bu turdagi sandiq yo'q",
        "chest_result": "Sandiq mukofoti: {result}",
        "managers_title": "Menejerlar",
        "manager_assigned": "{slot} slotiga tayinlandi",
        "manager_leveled": "Menejer {level}-darajaga oshirildi",
        "manager_no_frags": "Fragment yetarli emas",
        "manager_not_owned": "Menejer ochilmagan",
        "abilities_title": "Qobiliyatlar",
        "ability_ready": "TAYYOR",
        "ability_active": "FAOL ({minutes}m {seconds}s)",
        "ability_cooldown": "SOVUSH ({minutes}m {seconds}s)",
        "ability_activated": "Qobiliyat ishga tushdi",
        "ability_not_ready": "Qobiliyat hali tayyor emas",
        "ability_no_manager": "Menejer tayinlanmagan",
        "slot_shaft": "Shaxta",
        "slot_elevator": "Lift",
        "slot_warehouse": "Ombor",
        "notify_expedition_done": "Ekspeditsiya tugadi",
        "pass_menu": "Pass",
        "quests_menu": "Kvestlar",
        "pass_title": "Mavsum passi",
        "pass_claim": "Mukofotni olish",
        "pass_buy_premium": "Premium sotib olish",
        "pass_premium_on": "Premium faol",
        "pass_premium_off": "Premium faol emas",
        "pass_claimed": "Pass mukofotlari olindi",
        "pass_nothing": "Pass uchun mukofot yo'q",
        "season_left": "Mavsum qolgan vaqt",
        "season_level": "Mavsum darajasi",
        "season_xp": "Mavsum XP",
        "quests_title": "Kvestlar",
        "daily_quests": "Kunlik",
        "weekly_quests": "Haftalik",
        "quest_claim": "Kvestni olish",
        "quest_claimed": "Kvest mukofoti olindi",
        "quest_not_ready": "Kvest hali bajarilmagan",
        "quest_completed": "Bajarildi",
        "quest_progress": "Jarayon",
        "too_many_actions": "Harakat limiti tugadi. Keyinroq urinib ko'ring.",
        "home_tab": "Asosiy",
        "progress_tab": "Progress",
        "settings_tab": "Sozlamalar",
        "subscription_bonus": "Obuna bonusi",
        "sub_status_subscribed": "Obuna bor",
        "sub_status_not_subscribed": "Obuna yo'q",
        "sub_bonus_active": "Bonus faol",
        "sub_bonus_inactive": "Bonus faol emas",
        "sub_check": "Obunani tekshirish",
        "join_channel": "Kanalga o'tish",
        "sub_verify_fail": "Hozir obunani tekshirib bo'lmadi. Keyinroq urinib ko'ring.",
        "sub_bonus_granted": "Obuna bonusi berildi",
        "sub_bonus_clawed": "Obuna bekor bo'lgani uchun bonus qaytarib olindi",
        "auto_manager": "Auto menejer",
        "diamond_luck": "Olmos omad",
        "diamonds": "Olmoslar",
        "diamond_chance": "Olmos ehtimoli",
        "about": "Bot haqida",
        "company": "Kompaniya",
        "museum": "Muzey",
        "collection": "Kolleksiya",
        "sell": "Sotish",
        "auto_sell": "Avto-sotish",
        "free_spin": "Bepul spin",
        "spin_now": "Aylantirish",
        "spin_wait": "Keyingi spin",
        "donate": "Donat (tez kunda)",
        "section_images": "Bo'lim rasmlari",
        "settings_title": "Sozlamalar",
        "home_title": "Asosiy",
        "progress_title": "Progress",
        "company_name": "Kompaniya nomi",
        "company_level": "Kompaniya darajasi",
        "company_xp": "Kompaniya XP",
        "museum_buy": "Muzey sotib olish",
        "museum_upgrade": "Muzeyni oshirish",
        "museum_claim": "Muzey daromadini olish",
        "museum_not_owned": "Muzey hali sotib olinmagan",
        "museum_claim_not_ready": "Muzey daromadi hali tayyor emas",
        "museum_claimed": "Muzey daromadi olindi",
        "assemble": "Yig'ish",
        "leaderboards": "Liderlar",
        "lb_prestige": "Prestij",
        "lb_coins": "Tangalar",
        "lb_gems": "Olmoslar",
        "lb_income": "Daromad",
        "lb_company": "Kompaniya darajasi",
        "setname_usage": "Foydalanish: /setname <kompaniya nomi>",
        "setname_done": "Kompaniya nomi yangilandi",
        "sub_need_admin": "Tekshiruv amalga oshmadi. Bot kanalda admin bo'lishi kerak.",
        "sub_check_retry": "Qayta tekshirish",
        "reaction_note": "Kanal postlariga kim reaksiya qoldirganini Telegram Bot API bilan tekshirib bo'lmaydi.",
        "assumptions": "Taxminlar",
    },
}

LANG_DISPLAY_NAMES = {
    "en": {"en": "English", "ru": "English", "uz": "English"},
    "ru": {"en": "Russian", "ru": "Русский", "uz": "Ruscha"},
    "uz": {"en": "Uzbek", "ru": "Узбекский", "uz": "O‘zbekcha"},
}

# Per-user write lock to serialize DB writes and avoid overlapping saves.
USER_LOCKS: dict[int, asyncio.Lock] = {}
JOB_QUEUE_WARNING_SHOWN = False

MINES_CONFIG: dict[str, dict[str, Any]] = {
    "coal": {
        "name": "Угольная шахта",
        "continent": "Стартовая",
        "cost": 0,
        "multiplier": 1,
        "color": "⚒",
    },
    "gold": {
        "name": "Золотая шахта",
        "continent": "Континент II",
        "cost": 50_000,
        "multiplier": 5,
        "color": "🟡",
    },
    "ruby": {
        "name": "Рубиновая шахта",
        "continent": "Континент III",
        "cost": 500_000,
        "multiplier": 25,
        "color": "🔴",
    },
    "event": {
        "name": "Экспедиционная шахта",
        "continent": "Временная зона",
        "cost": 1_000_000,
        "multiplier": 100,
        "color": "🔵",
    },
}

RESEARCH_CONFIG: dict[str, dict[str, Any]] = {
    "efficiency": {
        "name": "Эффективность ⚡",
        "max_lvl": 10,
        "cost_base": 100,
        "desc": "+10% к производительности",
        "requires": None,
    },
    "logistics": {
        "name": "Логистика 📦",
        "max_lvl": 10,
        "cost_base": 150,
        "desc": "-5% цена апгрейдов",
        "requires": ("efficiency", 3),
    },
    "automation": {
        "name": "Автоматизация 🤖",
        "max_lvl": 5,
        "cost_base": 500,
        "desc": "-10% КД менеджеров",
        "requires": ("logistics", 5),
    },
    "engineering": {
        "name": "Инженерия 🛠",
        "max_lvl": 10,
        "cost_base": 200,
        "desc": "-2% стоимость уровня шахты",
        "requires": ("logistics", 2),
    },
    "marketing": {
        "name": "Маркетинг 📈",
        "max_lvl": 5,
        "cost_base": 1000,
        "desc": "+20% эффекта престижа",
        "requires": ("efficiency", 5),
    },
}

ACHIEVEMENTS_CONFIG = {
    "first_million": {
        "name": "Миллионер 💰",
        "desc": "Заработать 1,000,000 монет",
        "req": lambda p: p["stats"]["total_earned"] >= 1_000_000,
    },
    "upgrader_50": {
        "name": "Мастер улучшений 🛠",
        "desc": "Сделать 50 улучшений",
        "req": lambda p: p["stats"]["total_upgrades"] >= 50,
    },
    "researcher": {
        "name": "Ученый 🧬",
        "desc": "Открыть 5 уровней исследований",
        "req": lambda p: sum(p["research"].values()) >= 5,
    },
    "traveler": {
        "name": "Путешественник ✈️",
        "desc": "Разблокировать 3 шахты",
        "req": lambda p: len(p["unlocked_mines"]) >= 3,
    },
}

BOOSTS = {
    "boost_x2": {"name": "x2 на 15 мин", "mult": 2, "sec": 15 * 60},
    "boost_x5": {"name": "x5 на 10 мин", "mult": 5, "sec": 10 * 60},
    "boost_x10": {"name": "x10 на 5 мин", "mult": 10, "sec": 5 * 60},
}


def resolve_bot_token() -> str:
    token = (
        os.getenv("TELEGRAM_TOKEN")
        or os.getenv("BOT_TOKEN")
        or os.getenv("TG_TOKEN")
        or os.getenv("TOKEN")
    )
    if token and token.strip():
        return token.strip()

    try:
        entered = input("Введите TELEGRAM токен бота: ").strip()
    except EOFError:
        entered = ""

    if entered:
        return entered

    raise RuntimeError(
        "Токен бота не найден. Добавьте .env в корень проекта "
        "с переменной TELEGRAM_TOKEN=... или введите токен при запуске."
    )


def normalize_lang_code(lang_code: str | None) -> str:
    if not lang_code:
        return "en"
    normalized = lang_code.split("-")[0].lower().strip()
    return normalized if normalized in SUPPORTED_LANGS else "en"


def get_player_lang(player: dict[str, Any]) -> str:
    return normalize_lang_code(player.get("lang", "en"))


def t(player: dict[str, Any], key: str, **kwargs: Any) -> str:
    lang = get_player_lang(player)
    template = I18N.get(lang, {}).get(key) or I18N["en"].get(key) or key
    return template.format(**kwargs)


def lang_name_for_ui(target_lang: str, ui_lang: str) -> str:
    return LANG_DISPLAY_NAMES.get(target_lang, {}).get(ui_lang, target_lang)


def detect_user_lang(update: Update) -> str:
    code = None
    if update.effective_user:
        code = update.effective_user.language_code
    normalized = normalize_lang_code(code)
    return normalized if normalized in SUPPORTED_LANGS else "en"


def get_admin_ids() -> set[int]:
    raw = os.getenv("ADMIN_IDS", "").strip()
    if not raw:
        return set()
    result: set[int] = set()
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if chunk.isdigit():
            result.add(int(chunk))
    return result


# =========================
# DB / persistence
# =========================


def get_user_lock(user_id: int) -> asyncio.Lock:
    lock = USER_LOCKS.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        USER_LOCKS[user_id] = lock
    return lock


def build_default_mine() -> dict[str, Any]:
    return {
        "shaft": {"level": 1},
        "elevator": {"level": 1},
        "warehouse": {"level": 1},
        "level": 1,
        "mine_storage": 0,
        "elevator_storage": 0,
        "warehouse_storage": 0,
        "auto_sell": False,
    }


def build_player_defaults() -> dict[str, Any]:
    return {
        "coins": 500,
        "gems": 50,
        "active_mine": "coal",
        "unlocked_mines": ["coal"],
        "mines": {"coal": build_default_mine()},
        "inventory": {"boost_x2": 1, "boost_x5": 0, "boost_x10": 0, "chest_basic": 0, "chest_rare": 0},
        "active_boost": {"multiplier": 1, "expires_at": 0},
        "assigned_managers": {"shaft": "miner_c", "elevator": None, "warehouse": None},
        "manager_inventory": {"miner_c": {"level": 1, "fragments": 1}},
        "manager_skills": {
            "shaft": {"active_until": 0, "cd_until": 0, "notified": True},
            "elevator": {"active_until": 0, "cd_until": 0, "notified": True},
            "warehouse": {"active_until": 0, "cd_until": 0, "notified": True},
        },
        "manager_automation": {"shaft": True, "elevator": True, "warehouse": True},
        "missions_completed": [],
        "last_offline_check": time.time(),
        "offline_earnings": 0,
        "prestige_points": 0,
        "automation_lvl": 0,
        "auto_manager_lvl": 0,
        "manager_chest_opened": 0,
        "rng_counter": 0,
        "drops": {"diamond_chance_lvl": 0, "diamonds": 0, "pending": 0},
        "research": {
            "efficiency": 0,
            "logistics": 0,
            "automation": 0,
            "engineering": 0,
            "marketing": 0,
        },
        "last_daily": 0,
        "event_active": False,
        "expeditions": {"active": None, "history": []},
        "season": {
            "id": BOT_SEASON_ID,
            "xp": 0,
            "level": 1,
            "premium": False,
            "claimed_free": [],
            "claimed_premium": [],
        },
        "quests": {
            "daily": {"reset_at": 0, "items": {}, "claimed": []},
            "weekly": {"reset_at": 0, "items": {}, "claimed": []},
        },
        "antispam": {"action_ts": []},
        "resources": {k: 0 for k in RESOURCE_BASE_CHANCES.keys()},
        "museum": {
            "owned": False,
            "level": 1,
            "income_pool": 0,
            "last_claim": 0,
            "last_tick": time.time(),
        },
        "fossils": {"parts": {}, "assembled": []},
        "company": {"name": "My Company", "level": 1, "xp": 0},
        "wheel": {"next_at": 0},
        "ledger": [],
        "settings": {
            "notifications": True,
            "notifications_enabled": True,
            "notify_manager_ready": True,
            "notify_event_end": True,
            "notify_daily_ready": True,
            "notify_prestige_ready": False,
            "notify_updates": True,
            "notify_expedition_done": True,
            "last_action_ts": 0,
            "last_unsolicited_ts": 0,
            "section_images": False,
            "last_notified": {
                "manager_ready": 0,
                "event_end": 0,
                "daily_ready": 0,
                "prestige_ready": 0,
                "update": 0,
                "exp_done": 0,
            },
        },
        "stats": {
            "total_earned": 500,
            "total_upgrades": 0,
            "start_date": time.time(),
        },
        "achievements": [],
        "has_started": False,
        "creator_sub_reward_claimed": False,
        "creator_sub_bonus_active": False,
        "channel_rewards": {
            "sub_bonus_given": False,
            "sub_bonus_coins": 10_000,
            "sub_bonus_gems": 25,
            "granted_at": 0,
        },
        "lang": "en",
        "lang_prompted": False,
        "onboarding_done": False,
        "last_seen_version": BOT_VERSION,
    }


def ensure_player_defaults(player: dict[str, Any], username: str | None = None) -> bool:
    changed = False
    defaults = build_player_defaults()

    if "name" not in player:
        player["name"] = username or "Игрок"
        changed = True

    for key, value in defaults.items():
        if key not in player:
            player[key] = value
            changed = True

    inv = player.get("inventory", {})
    for bid in BOOSTS:
        if bid not in inv:
            inv[bid] = 0
            changed = True
    for chest_id in ("chest_basic", "chest_rare"):
        if chest_id not in inv:
            inv[chest_id] = 0
            changed = True
    player["inventory"] = inv

    settings = player.get("settings", {})
    if not isinstance(settings, dict):
        settings = {"notifications": True}
        changed = True
    if "notifications" not in settings and "notifications_enabled" in settings:
        settings["notifications"] = bool(settings.get("notifications_enabled", True))
        changed = True
    if "notifications_enabled" not in settings and "notifications" in settings:
        settings["notifications_enabled"] = bool(settings.get("notifications", True))
        changed = True
    if "notifications" not in settings:
        settings["notifications"] = True
        changed = True
    if "notifications_enabled" not in settings:
        settings["notifications_enabled"] = True
        changed = True
    for key, default_val in {
        "notify_manager_ready": True,
        "notify_event_end": True,
        "notify_daily_ready": True,
        "notify_prestige_ready": False,
        "notify_updates": True,
        "notify_expedition_done": True,
        "section_images": False,
        "last_unsolicited_ts": 0,
    }.items():
        if key not in settings:
            settings[key] = default_val
            changed = True
    if "last_action_ts" not in settings:
        settings["last_action_ts"] = 0
        changed = True
    last_notified = settings.get("last_notified", {})
    if not isinstance(last_notified, dict):
        last_notified = {}
        changed = True
    for key in ("manager_ready", "event_end", "daily_ready", "prestige_ready", "update", "exp_done"):
        if key not in last_notified:
            last_notified[key] = 0
            changed = True
    settings["last_notified"] = last_notified
    settings["notifications"] = bool(settings.get("notifications_enabled", settings.get("notifications", True)))
    settings["notifications_enabled"] = bool(settings["notifications"])
    player["settings"] = settings

    if "mines" not in player or not isinstance(player["mines"], dict):
        player["mines"] = {"coal": build_default_mine()}
        changed = True

    unlocked = player.get("unlocked_mines", ["coal"])
    if "coal" not in unlocked:
        unlocked.insert(0, "coal")
        changed = True
    player["unlocked_mines"] = unlocked

    for mine_id in unlocked:
        if mine_id not in player["mines"]:
            player["mines"][mine_id] = build_default_mine()
            changed = True
        else:
            mine = player["mines"][mine_id]
            if "level" not in mine:
                mine["level"] = 1
                changed = True
            for comp in ("shaft", "elevator", "warehouse"):
                if comp not in mine:
                    mine[comp] = {"level": 1}
                    changed = True
                if "level" not in mine[comp]:
                    mine[comp]["level"] = 1
                    changed = True
            for key, val in {"mine_storage": 0, "elevator_storage": 0, "warehouse_storage": 0, "auto_sell": False}.items():
                if key not in mine:
                    mine[key] = val
                    changed = True

    if player.get("active_mine") not in unlocked:
        player["active_mine"] = "coal"
        changed = True

    normalized_lang = normalize_lang_code(player.get("lang", "en"))
    if player.get("lang") != normalized_lang:
        player["lang"] = normalized_lang
        changed = True
    if "lang_prompted" not in player:
        player["lang_prompted"] = False
        changed = True
    if "onboarding_done" not in player:
        player["onboarding_done"] = False
        changed = True
    if "last_seen_version" not in player:
        player["last_seen_version"] = BOT_VERSION
        changed = True
    if "auto_manager_lvl" not in player:
        player["auto_manager_lvl"] = 0
        changed = True
    drops = player.get("drops", {})
    if not isinstance(drops, dict):
        drops = {"diamond_chance_lvl": 0, "diamonds": 0, "pending": 0}
        changed = True
    for key, default_val in {"diamond_chance_lvl": 0, "diamonds": 0, "pending": 0}.items():
        if key not in drops:
            drops[key] = default_val
            changed = True
    player["drops"] = drops
    resources = player.get("resources", {})
    if not isinstance(resources, dict):
        resources = {}
        changed = True
    for r in RESOURCE_BASE_CHANCES.keys():
        if r not in resources:
            resources[r] = 0
            changed = True
    player["resources"] = resources
    if "creator_sub_bonus_active" not in player:
        player["creator_sub_bonus_active"] = bool(player.get("creator_sub_reward_claimed", False))
        changed = True
    channel_rewards = player.get("channel_rewards", {})
    if not isinstance(channel_rewards, dict):
        channel_rewards = {}
        changed = True
    defaults_channel = {
        "sub_bonus_given": bool(player.get("creator_sub_bonus_active", False)),
        "sub_bonus_coins": CREATOR_SUB_REWARD_COINS,
        "sub_bonus_gems": CREATOR_SUB_REWARD_GEMS,
        "granted_at": 0,
    }
    for key, default_val in defaults_channel.items():
        if key not in channel_rewards:
            channel_rewards[key] = default_val
            changed = True
    player["channel_rewards"] = channel_rewards

    museum = player.get("museum", {})
    if not isinstance(museum, dict):
        museum = {}
        changed = True
    for key, val in {"owned": False, "level": 1, "income_pool": 0, "last_claim": 0, "last_tick": time.time()}.items():
        if key not in museum:
            museum[key] = val
            changed = True
    player["museum"] = museum

    fossils = player.get("fossils", {})
    if not isinstance(fossils, dict):
        fossils = {}
        changed = True
    if "parts" not in fossils or not isinstance(fossils.get("parts"), dict):
        fossils["parts"] = {}
        changed = True
    if "assembled" not in fossils or not isinstance(fossils.get("assembled"), list):
        fossils["assembled"] = []
        changed = True
    player["fossils"] = fossils

    company = player.get("company", {})
    if not isinstance(company, dict):
        company = {}
        changed = True
    for key, val in {"name": "My Company", "level": 1, "xp": 0}.items():
        if key not in company:
            company[key] = val
            changed = True
    player["company"] = company

    wheel = player.get("wheel", {})
    if not isinstance(wheel, dict):
        wheel = {}
        changed = True
    if "next_at" not in wheel:
        wheel["next_at"] = 0
        changed = True
    player["wheel"] = wheel

    if "ledger" not in player or not isinstance(player.get("ledger"), list):
        player["ledger"] = []
        changed = True
    exps = player.get("expeditions", {})
    if not isinstance(exps, dict):
        exps = {"active": None, "history": []}
        changed = True
    if "active" not in exps:
        exps["active"] = None
        changed = True
    if "history" not in exps or not isinstance(exps.get("history"), list):
        exps["history"] = []
        changed = True
    player["expeditions"] = exps

    mgr_inv = player.get("manager_inventory", {})
    if not isinstance(mgr_inv, dict):
        mgr_inv = {}
        changed = True
    for manager_id in MANAGER_POOL:
        if manager_id in mgr_inv:
            entry = mgr_inv.get(manager_id)
            if not isinstance(entry, dict):
                mgr_inv[manager_id] = {"level": 1, "fragments": 0}
                changed = True
            else:
                if "level" not in entry:
                    entry["level"] = 1
                    changed = True
                if "fragments" not in entry:
                    entry["fragments"] = 0
                    changed = True
    player["manager_inventory"] = mgr_inv

    assigned = player.get("assigned_managers", {})
    if not isinstance(assigned, dict):
        assigned = {"shaft": "miner_c", "elevator": None, "warehouse": None}
        changed = True
    for slot in ("shaft", "elevator", "warehouse"):
        if slot not in assigned:
            assigned[slot] = None
            changed = True
    if assigned.get("shaft") is None and "miner_c" in player.get("manager_inventory", {}):
        assigned["shaft"] = "miner_c"
        changed = True
    player["assigned_managers"] = assigned

    skills = player.get("manager_skills", {})
    if not isinstance(skills, dict):
        skills = {}
        changed = True
    for slot in ("shaft", "elevator", "warehouse"):
        state = skills.get(slot)
        if not isinstance(state, dict):
            skills[slot] = {"active_until": 0, "cd_until": 0, "notified": True}
            changed = True
        else:
            for k, v in {"active_until": 0, "cd_until": 0, "notified": True}.items():
                if k not in state:
                    state[k] = v
                    changed = True
    player["manager_skills"] = skills
    mgr_auto = player.get("manager_automation", {})
    if not isinstance(mgr_auto, dict):
        mgr_auto = {}
        changed = True
    for slot in ("shaft", "elevator", "warehouse"):
        if slot not in mgr_auto:
            mgr_auto[slot] = True
            changed = True
    player["manager_automation"] = mgr_auto

    season = player.get("season", {})
    if not isinstance(season, dict):
        season = {}
        changed = True
    if season.get("id") != BOT_SEASON_ID:
        season = {
            "id": BOT_SEASON_ID,
            "xp": 0,
            "level": 1,
            "premium": False,
            "claimed_free": [],
            "claimed_premium": [],
        }
        changed = True
    for key, val in {
        "id": BOT_SEASON_ID,
        "xp": 0,
        "level": 1,
        "premium": False,
        "claimed_free": [],
        "claimed_premium": [],
    }.items():
        if key not in season:
            season[key] = val
            changed = True
    player["season"] = season

    quests = player.get("quests", {})
    if not isinstance(quests, dict):
        quests = {
            "daily": {"reset_at": 0, "items": {}, "claimed": []},
            "weekly": {"reset_at": 0, "items": {}, "claimed": []},
        }
        changed = True
    for bucket in ("daily", "weekly"):
        q = quests.get(bucket)
        if not isinstance(q, dict):
            quests[bucket] = {"reset_at": 0, "items": {}, "claimed": []}
            changed = True
            continue
        if "reset_at" not in q:
            q["reset_at"] = 0
            changed = True
        if "items" not in q or not isinstance(q["items"], dict):
            q["items"] = {}
            changed = True
        if "claimed" not in q or not isinstance(q["claimed"], list):
            q["claimed"] = []
            changed = True
    player["quests"] = quests

    antispam = player.get("antispam", {})
    if not isinstance(antispam, dict):
        antispam = {"action_ts": []}
        changed = True
    if "action_ts" not in antispam or not isinstance(antispam["action_ts"], list):
        antispam["action_ts"] = []
        changed = True
    player["antispam"] = antispam

    if reset_quests_if_needed(player):
        changed = True

    return changed


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS players (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                data TEXT
            )
            """
        )
        await db.commit()
    os.makedirs(BACKUP_DIR, exist_ok=True)


async def get_player(user_id: int, username: str = "Игрок") -> dict[str, Any]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT data FROM players WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()

    if row:
        try:
            player = json.loads(row[0])
        except json.JSONDecodeError:
            player = {"name": username}

        if "onboarding_done" not in player:
            player["onboarding_done"] = True

        if ensure_player_defaults(player, username):
            await save_player(user_id, player, username=player.get("name", username))
        return player

    new_player = {"name": username}
    ensure_player_defaults(new_player, username)
    await save_player(user_id, new_player, username)
    return new_player


async def save_player(user_id: int, player_data: dict[str, Any], username: str | None = None) -> None:
    ensure_player_defaults(player_data, username or player_data.get("name"))
    sanitize_player_economy(player_data)

    for a_id, a_cfg in ACHIEVEMENTS_CONFIG.items():
        if a_id not in player_data["achievements"] and a_cfg["req"](player_data):
            player_data["achievements"].append(a_id)
            player_data["gems"] += 25

    payload = json.dumps(player_data, ensure_ascii=False)

    lock = get_user_lock(user_id)
    async with lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO players (user_id, username, data) VALUES (?, ?, ?)",
                (user_id, username or player_data.get("name"), payload),
            )
            await db.commit()

    backup_path = os.path.join(BACKUP_DIR, f"{user_id}.json")
    with open(backup_path, "w", encoding="utf-8") as f:
        json.dump(player_data, f, ensure_ascii=False, indent=2)


# =========================
# Game logic
# =========================


def clamp(value: int, min_value: int, max_value: int) -> int:
    return max(min_value, min(max_value, value))


def get_prestige_multiplier(player: dict[str, Any]) -> float:
    points = player.get("prestige_points", 0)
    marketing_lvl = player.get("research", {}).get("marketing", 0)
    marketing_bonus = 1 + marketing_lvl * 0.05
    return 1 + (points * PRESTIGE_BONUS_PER_POINT * marketing_bonus)


def get_active_boost_multiplier(player: dict[str, Any]) -> float:
    active = player.get("active_boost", {"multiplier": 1, "expires_at": 0})
    if time.time() < active.get("expires_at", 0):
        return float(active.get("multiplier", 1))
    return 1.0


def get_mine_stats(player: dict[str, Any], mine_id: str) -> tuple[float, float, float, str]:
    mine = player["mines"][mine_id]
    mine_lvl = mine.get("level", 1)

    efficiency_boost = 1 + (player["research"].get("efficiency", 0) * 0.1)
    mine_lvl_bonus = 1 + (mine_lvl - 1) * 0.2  # mild level bonus

    mgr_mult = {"shaft": 1.0, "elevator": 1.0, "warehouse": 1.0}
    for slot in mgr_mult:
        manager_id = player.get("assigned_managers", {}).get(slot)
        base = efficiency_boost * mine_lvl_bonus
        slot_on = bool(player.get("manager_automation", {}).get(slot, True))
        if manager_id and slot_on:
            lvl = int(player.get("manager_inventory", {}).get(manager_id, {}).get("level", 1))
            passive = manager_passive_mult(manager_id, lvl, slot)
            active = manager_active_mult(player, manager_id, slot)
            mgr_mult[slot] = max(1.0, base * passive * active)
        else:
            mgr_mult[slot] = max(1.0, base)

    shaft_rate = mine["shaft"]["level"] * 10 * mgr_mult["shaft"]
    elevator_rate = mine["elevator"]["level"] * 10 * mgr_mult["elevator"]
    warehouse_rate = mine["warehouse"]["level"] * 10 * mgr_mult["warehouse"]

    min_rate = min(shaft_rate, elevator_rate, warehouse_rate)
    if min_rate == shaft_rate:
        bottleneck = "Шахта"
    elif min_rate == elevator_rate:
        bottleneck = "Лифт"
    else:
        bottleneck = "Склад"

    return shaft_rate, elevator_rate, warehouse_rate, bottleneck


def get_total_mine_level(player: dict[str, Any]) -> int:
    total = 0
    unlocked = player.get("unlocked_mines", ["coal"])
    for mine_id in unlocked:
        mine = player.get("mines", {}).get(mine_id, build_default_mine())
        total += int(mine.get("level", 1))
        total += int(mine.get("shaft", {}).get("level", 1))
        total += int(mine.get("elevator", {}).get("level", 1))
        total += int(mine.get("warehouse", {}).get("level", 1))
    return total


def get_effective_tick_interval(player: dict[str, Any]) -> int:
    total_level = get_total_mine_level(player)
    automation_lvl = int(player.get("automation_lvl", 0))
    raw = BASE_TICK_SECONDS - (automation_lvl * 10) - math.floor(total_level / 5)
    return clamp(raw, MIN_TICK_SECONDS, MAX_TICK_SECONDS)


def calc_income_per_tick(player: dict[str, Any], mine_id: str | None = None) -> int:
    mine_id = mine_id or player.get("active_mine", "coal")
    if mine_id not in player.get("mines", {}):
        mine_id = "coal"

    s_rate, e_rate, w_rate, _ = get_mine_stats(player, mine_id)
    base = min(s_rate, e_rate, w_rate)
    base *= MINES_CONFIG[mine_id]["multiplier"]
    base *= get_prestige_multiplier(player)
    base *= 1 + (int(player.get("automation_lvl", 0)) * 0.05)
    base *= get_active_boost_multiplier(player)

    return max(0, int(base))


def get_upgrade_cost(player: dict[str, Any], base_cost: int, level: int, cost_type: str = "normal") -> int:
    discount = 1 - (player["research"].get("logistics", 0) * 0.05)
    if cost_type == "mine_level":
        discount -= player["research"].get("engineering", 0) * 0.02
        return int(base_cost * (5 ** (level - 1)) * max(0.1, discount))
    return int(base_cost * (1.5 ** (level - 1)) * max(0.1, discount))


async def apply_offline_earnings(player: dict[str, Any]) -> tuple[int, int, int]:
    now = time.time()
    last = float(player.get("last_offline_check", now))
    delta = max(0, int(now - last))
    delta = min(delta, OFFLINE_CAP_SECONDS)

    interval = get_effective_tick_interval(player)
    ticks = delta // interval
    earned = ticks * calc_income_per_tick(player, player.get("active_mine", "coal"))

    if earned > 0:
        player["coins"] += earned
        player["stats"]["total_earned"] += earned
        update_quest_progress(player, "earn_coins", earned)

    player["offline_earnings"] = earned
    player["last_offline_check"] = now
    return earned, delta, interval


def reset_player_for_prestige(player: dict[str, Any]) -> None:
    player["coins"] = 500
    player["active_mine"] = "coal"
    player["unlocked_mines"] = ["coal"]
    player["mines"] = {mine_id: build_default_mine() for mine_id in MINES_CONFIG.keys()}
    player["automation_lvl"] = 0
    player["research"] = {k: 0 for k in RESEARCH_CONFIG.keys()}
    player["assigned_managers"] = {"shaft": "miner_c", "elevator": None, "warehouse": None}
    player["active_boost"] = {"multiplier": 1, "expires_at": 0}


def get_settings(player: dict[str, Any]) -> dict[str, Any]:
    settings = player.get("settings", {})
    if not isinstance(settings, dict):
        settings = {}
        player["settings"] = settings
    return settings


def notifications_enabled(player: dict[str, Any]) -> bool:
    settings = get_settings(player)
    return bool(settings.get("notifications_enabled", settings.get("notifications", True)))


def get_last_notified(player: dict[str, Any], key: str) -> float:
    settings = get_settings(player)
    last_notified = settings.get("last_notified", {})
    if not isinstance(last_notified, dict):
        settings["last_notified"] = {}
        return 0
    return float(last_notified.get(key, 0))


def set_last_notified(player: dict[str, Any], key: str, ts: float) -> None:
    settings = get_settings(player)
    last_notified = settings.get("last_notified", {})
    if not isinstance(last_notified, dict):
        last_notified = {}
    last_notified[key] = ts
    settings["last_notified"] = last_notified


def can_send_unsolicited(player: dict[str, Any], now_ts: float) -> bool:
    settings = get_settings(player)
    last_ts = float(settings.get("last_unsolicited_ts", 0))
    return (now_ts - last_ts) >= UNSOLICITED_COOLDOWN_SECONDS


def mark_unsolicited_sent(player: dict[str, Any], now_ts: float) -> None:
    settings = get_settings(player)
    settings["last_unsolicited_ts"] = now_ts


async def safe_send_message(
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    text: str,
    parse_mode: ParseMode | None = None,
) -> bool:
    try:
        await context.application.bot.send_message(
            chat_id=user_id,
            text=text,
            parse_mode=parse_mode,
        )
        return True
    except Exception as exc:
        logger.warning("Send message failed for user %s: %s", user_id, exc)
        return False


async def maybe_send_section_banner(
    update: Update, context: ContextTypes.DEFAULT_TYPE, player: dict[str, Any], section: str
) -> None:
    settings = get_settings(player)
    if not bool(settings.get("section_images", False)):
        return
    sent_sections = context.user_data.get("section_images_sent", set())
    if not isinstance(sent_sections, set):
        sent_sections = set()
    if section in sent_sections:
        return
    try:
        await context.application.bot.send_photo(
            chat_id=update.effective_user.id,
            photo=f"https://picsum.photos/seed/{section}/800/300",
            caption=f"{section.title()}",
        )
        sent_sections.add(section)
        context.user_data["section_images_sent"] = sent_sections
    except Exception:
        return


def is_daily_ready(player: dict[str, Any], now_ts: float | None = None) -> bool:
    now_ts = now_ts or time.time()
    last_daily = float(player.get("last_daily", 0))
    return (now_ts - last_daily) >= 24 * 60 * 60


def get_daily_reward(player: dict[str, Any]) -> tuple[int, int, str | None]:
    day_index = int(time.time() // (24 * 60 * 60))
    coins = 4000 + int(player.get("prestige_points", 0)) * 500
    gems = 8 + int(player.get("automation_lvl", 0))
    boost_id = "boost_x2" if day_index % 3 == 0 else None
    return coins, gems, boost_id


def get_seconds_until_daily(player: dict[str, Any], now_ts: float | None = None) -> int:
    now_ts = now_ts or time.time()
    last_daily = float(player.get("last_daily", 0))
    left = int((24 * 60 * 60) - (now_ts - last_daily))
    return max(0, left)


async def maybe_send_update_note(
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    player: dict[str, Any],
) -> bool:
    settings = get_settings(player)
    if not bool(settings.get("notify_updates", True)):
        return False
    if player.get("last_seen_version", BOT_VERSION) == BOT_VERSION:
        return False

    sent = await safe_send_message(
        context,
        user_id,
        f"📢 *{t(player, 'update_title')}*\n\n"
        f"{t(player, 'update_text', version=BOT_VERSION, changelog=BOT_CHANGELOG)}",
        parse_mode=ParseMode.MARKDOWN,
    )
    if sent:
        player["last_seen_version"] = BOT_VERSION
        set_last_notified(player, "update", time.time())
    return sent


def sanitize_player_economy(player: dict[str, Any]) -> None:
    for field in ("coins", "gems"):
        val = int(player.get(field, 0))
        if val < -ECONOMY_MAX_VALUE:
            logger.warning("Clamping %s for player to %s", field, -ECONOMY_MAX_VALUE)
            val = -ECONOMY_MAX_VALUE
        if val > ECONOMY_MAX_VALUE:
            logger.warning("Clamping %s for player to %s", field, ECONOMY_MAX_VALUE)
            val = ECONOMY_MAX_VALUE
        player[field] = val


def get_season_time_left() -> int:
    now_ts = time.time()
    end_ts = SEASON_START_TS + SEASON_DURATION_SEC
    return max(0, int(end_ts - now_ts))


def season_xp_needed(level: int) -> int:
    return 100 + max(0, level - 1) * 30


def add_season_xp(player: dict[str, Any], amount: int) -> int:
    if amount <= 0:
        return 0
    season = player.get("season", {})
    season["xp"] = int(season.get("xp", 0)) + int(amount)
    gained_levels = 0
    while int(season.get("level", 1)) < PASS_MAX_LEVEL:
        need = season_xp_needed(int(season.get("level", 1)))
        if int(season.get("xp", 0)) < need:
            break
        season["xp"] = int(season.get("xp", 0)) - need
        season["level"] = int(season.get("level", 1)) + 1
        gained_levels += 1
    player["season"] = season
    return gained_levels


def choose_quests(pool: list[dict[str, Any]], n: int) -> list[dict[str, Any]]:
    if len(pool) <= n:
        return [dict(x) for x in pool]
    return [dict(x) for x in random.sample(pool, n)]


def reset_quests_if_needed(player: dict[str, Any]) -> bool:
    now_ts = time.time()
    quests = player.get("quests", {})
    changed = False
    daily = quests.get("daily", {})
    weekly = quests.get("weekly", {})
    if now_ts >= float(daily.get("reset_at", 0)):
        picks = choose_quests(QUESTS_CONFIG["daily"], 3)
        daily_items = {}
        for q in picks:
            daily_items[q["id"]] = {
                "event": q["event"],
                "target": int(q["target"]),
                "progress": 0,
                "reward": dict(q["reward"]),
                "done": False,
            }
        daily["items"] = daily_items
        daily["claimed"] = []
        daily["reset_at"] = now_ts + 24 * 60 * 60
        changed = True
    if now_ts >= float(weekly.get("reset_at", 0)):
        picks = choose_quests(QUESTS_CONFIG["weekly"], 3)
        weekly_items = {}
        for q in picks:
            weekly_items[q["id"]] = {
                "event": q["event"],
                "target": int(q["target"]),
                "progress": 0,
                "reward": dict(q["reward"]),
                "done": False,
            }
        weekly["items"] = weekly_items
        weekly["claimed"] = []
        weekly["reset_at"] = now_ts + 7 * 24 * 60 * 60
        changed = True
    quests["daily"] = daily
    quests["weekly"] = weekly
    player["quests"] = quests
    return changed


def update_quest_progress(player: dict[str, Any], event: str, amount: int = 1) -> None:
    if amount <= 0:
        return
    reset_quests_if_needed(player)
    quests = player.get("quests", {})
    for bucket in ("daily", "weekly"):
        items = quests.get(bucket, {}).get("items", {})
        if not isinstance(items, dict):
            continue
        for qid, item in items.items():
            if not isinstance(item, dict):
                continue
            if item.get("event") != event:
                continue
            target = int(item.get("target", 1))
            progress = int(item.get("progress", 0)) + amount
            if progress > target:
                progress = target
            item["progress"] = progress
            item["done"] = progress >= target
            items[qid] = item
        quests[bucket]["items"] = items
    player["quests"] = quests


def apply_generic_reward(player: dict[str, Any], reward: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    coins = int(reward.get("coins", 0))
    gems = int(reward.get("gems", 0))
    if coins > 0:
        player["coins"] += coins
        player["stats"]["total_earned"] += coins
        update_quest_progress(player, "earn_coins", coins)
        lines.append(f"+{coins} coins")
    if gems > 0:
        player["gems"] += gems
        lines.append(f"+{gems} gems")
    for item_id in ("boost_x2", "boost_x5", "boost_x10", "chest_basic", "chest_rare"):
        qty = int(reward.get(item_id, 0))
        if qty > 0:
            player["inventory"][item_id] = int(player["inventory"].get(item_id, 0)) + qty
            lines.append(f"+{qty} {item_id}")
    frag = int(reward.get("manager_fragments", 0))
    if frag > 0:
        m_id, amt = apply_fragments(player, frag)
        lines.append(f"+{amt} fragments ({get_manager_cfg(m_id).get('name', m_id)})")
    s_xp = int(reward.get("season_xp", 0))
    if s_xp > 0:
        add_season_xp(player, s_xp)
        lines.append(f"+{s_xp} season xp")
    sanitize_player_economy(player)
    return lines


def apply_pass_claims(player: dict[str, Any]) -> tuple[int, list[str]]:
    season = player.get("season", {})
    lvl = int(season.get("level", 1))
    free_claimed = set(int(x) for x in season.get("claimed_free", []))
    premium_claimed = set(int(x) for x in season.get("claimed_premium", []))
    premium_on = bool(season.get("premium", False))
    lines: list[str] = []
    count = 0
    for level in range(1, min(lvl, PASS_MAX_LEVEL) + 1):
        rewards = PASS_REWARDS.get(level, {})
        if level not in free_claimed:
            lines.extend(apply_generic_reward(player, rewards.get("free", {})))
            free_claimed.add(level)
            count += 1
        if premium_on and level not in premium_claimed:
            lines.extend(apply_generic_reward(player, rewards.get("premium", {})))
            premium_claimed.add(level)
            count += 1
    season["claimed_free"] = sorted(free_claimed)
    season["claimed_premium"] = sorted(premium_claimed)
    player["season"] = season
    return count, lines


def register_action_antispam(player: dict[str, Any]) -> bool:
    now_ts = time.time()
    antispam = player.get("antispam", {})
    arr = antispam.get("action_ts", [])
    if not isinstance(arr, list):
        arr = []
    arr = [float(x) for x in arr if (now_ts - float(x)) <= 60]
    if len(arr) >= ACTION_LIMIT_PER_MINUTE:
        antispam["action_ts"] = arr
        player["antispam"] = antispam
        return False
    arr.append(now_ts)
    antispam["action_ts"] = arr
    player["antispam"] = antispam
    return True


def format_remain(seconds: int) -> tuple[int, int, int]:
    s = max(0, int(seconds))
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return h, m, sec


def next_rng(player: dict[str, Any], mod: int) -> int:
    counter = int(player.get("rng_counter", 0))
    player["rng_counter"] = counter + 1
    if mod <= 0:
        return 0
    return counter % mod


def get_manager_cfg(manager_id: str) -> dict[str, Any]:
    return MANAGERS_CONFIG.get(
        manager_id,
        {
            "name": manager_id,
            "rarity": "common",
            "slot_affinity": "any",
            "passive_base": 1.05,
            "passive_step": 0.03,
            "active_mult": 2.0,
            "active_duration": 120,
            "cooldown": 900,
        },
    )


def get_slot_label(player: dict[str, Any], slot: str) -> str:
    key = f"slot_{slot}"
    return t(player, key) if key in I18N.get(get_player_lang(player), {}) or key in I18N["en"] else slot


def manager_passive_mult(manager_id: str, level: int, slot: str) -> float:
    cfg = get_manager_cfg(manager_id)
    base = float(cfg.get("passive_base", 1.05))
    step = float(cfg.get("passive_step", 0.03))
    affinity = cfg.get("slot_affinity", "any")
    level = max(1, int(level))
    mult = base + (level - 1) * step
    if affinity not in ("any", slot):
        mult = 1 + (mult - 1) * 0.35
    return max(1.0, mult)


def manager_active_mult(player: dict[str, Any], manager_id: str, slot: str) -> float:
    skills = player.get("manager_skills", {})
    state = skills.get(slot, {}) if isinstance(skills, dict) else {}
    if time.time() >= float(state.get("active_until", 0)):
        return 1.0
    cfg = get_manager_cfg(manager_id)
    return max(1.0, float(cfg.get("active_mult", 2.0)))


def manager_cooldown_after_research(player: dict[str, Any], base_cd: int) -> int:
    automation_lvl = int(player.get("research", {}).get("automation", 0))
    reduction = min(0.5, automation_lvl * 0.1)
    cd = int(base_cd * (1 - reduction))
    return max(30, cd)


def choose_manager_for_fragments(player: dict[str, Any]) -> str:
    owned = [m_id for m_id in player.get("manager_inventory", {}) if m_id in MANAGER_POOL]
    if owned:
        idx = next_rng(player, len(owned))
        return owned[idx]
    idx = next_rng(player, len(MANAGER_POOL))
    return MANAGER_POOL[idx]


def apply_fragments(player: dict[str, Any], amount: int) -> tuple[str, int]:
    manager_id = choose_manager_for_fragments(player)
    inv = player.get("manager_inventory", {})
    if manager_id not in inv:
        inv[manager_id] = {"level": 1, "fragments": 0}
    inv[manager_id]["fragments"] = int(inv[manager_id].get("fragments", 0)) + amount
    player["manager_inventory"] = inv
    return manager_id, amount


def open_chest(player: dict[str, Any], chest_id: str) -> str:
    inv = player.get("inventory", {})
    if int(inv.get(chest_id, 0)) <= 0:
        return t(player, "no_chest")

    inv[chest_id] -= 1
    player["inventory"] = inv
    roll = next_rng(player, 100)

    if chest_id == "chest_basic":
        if roll < 60:
            player["inventory"]["boost_x2"] = int(player["inventory"].get("boost_x2", 0)) + 1
            return "x2 boost +1"
        if roll < 80:
            player["inventory"]["boost_x5"] = int(player["inventory"].get("boost_x5", 0)) + 1
            return "x5 boost +1"
        if roll < 95:
            gems = 30 + next_rng(player, 51)
            player["gems"] += gems
            return f"{gems} gems"
        manager_id, amount = apply_fragments(player, 3 + next_rng(player, 3))
        m_name = get_manager_cfg(manager_id).get("name", manager_id)
        return f"{m_name} fragments +{amount}"

    if chest_id == "chest_rare":
        if roll < 40:
            player["inventory"]["boost_x5"] = int(player["inventory"].get("boost_x5", 0)) + 1
            return "x5 boost +1"
        if roll < 65:
            player["inventory"]["boost_x10"] = int(player["inventory"].get("boost_x10", 0)) + 1
            return "x10 boost +1"
        if roll < 90:
            gems = 80 + next_rng(player, 121)
            player["gems"] += gems
            return f"{gems} gems"
        missing = [m_id for m_id in MANAGER_POOL if m_id not in player.get("manager_inventory", {})]
        if missing:
            m_id = missing[next_rng(player, len(missing))]
            player["manager_inventory"][m_id] = {"level": 1, "fragments": 0}
            return f"new manager: {get_manager_cfg(m_id).get('name', m_id)}"
        manager_id, amount = apply_fragments(player, 6 + next_rng(player, 5))
        m_name = get_manager_cfg(manager_id).get("name", manager_id)
        return f"{m_name} fragments +{amount}"

    return t(player, "updated")


def get_expedition_cfg(exp_id: str) -> dict[str, Any] | None:
    return EXPEDITIONS_CONFIG.get(exp_id)


def apply_expedition_reward(player: dict[str, Any], reward: dict[str, Any]) -> list[str]:
    with_xp = dict(reward)
    with_xp["season_xp"] = int(with_xp.get("season_xp", 0)) + 40
    lines = apply_generic_reward(player, with_xp)
    update_quest_progress(player, "complete_expedition", 1)
    return lines


# =========================
# Jobs
# =========================


def ensure_user_tick_job(application: Application, user_id: int, interval: int) -> None:
    global JOB_QUEUE_WARNING_SHOWN
    if application.job_queue is None:
        if not JOB_QUEUE_WARNING_SHOWN:
            logger.warning(
                "JobQueue is not available. Install extra: pip install \"python-telegram-bot[job-queue]\""
            )
            JOB_QUEUE_WARNING_SHOWN = True
        return

    job_name = f"tick_{user_id}"
    for job in application.job_queue.jobs():
        if job.name == job_name:
            current = int(job.interval) if hasattr(job, "interval") else interval
            if current != interval:
                job.schedule_removal()
                application.job_queue.run_repeating(
                    tick_income,
                    interval=interval,
                    first=interval,
                    chat_id=user_id,
                    name=job_name,
                )
            return

    application.job_queue.run_repeating(
        tick_income,
        interval=interval,
        first=interval,
        chat_id=user_id,
        name=job_name,
    )


def ensure_user_auto_manager_job(application: Application, user_id: int) -> None:
    global JOB_QUEUE_WARNING_SHOWN
    if application.job_queue is None:
        if not JOB_QUEUE_WARNING_SHOWN:
            logger.warning(
                "JobQueue is not available. Install extra: pip install \"python-telegram-bot[job-queue]\""
            )
            JOB_QUEUE_WARNING_SHOWN = True
        return

    job_name = f"amgr_{user_id}"
    for job in application.job_queue.jobs():
        if job.name == job_name:
            return
    application.job_queue.run_repeating(
        tick_auto_manager_income,
        interval=AUTO_MANAGER_INTERVAL_SECONDS,
        first=AUTO_MANAGER_INTERVAL_SECONDS,
        chat_id=user_id,
        name=job_name,
    )


async def tick_income(context: ContextTypes.DEFAULT_TYPE) -> None:
    if context.application.job_queue is None:
        return

    user_id = context.job.chat_id
    player = await get_player(user_id)

    if not player.get("has_started", False):
        return

    interval = get_effective_tick_interval(player)
    current = int(context.job.interval) if hasattr(context.job, "interval") else interval
    if current != interval:
        context.job.schedule_removal()
        context.application.job_queue.run_repeating(
            tick_income,
            interval=interval,
            first=interval,
            chat_id=user_id,
            name=f"tick_{user_id}",
        )
        return

    mine_id = player.get("active_mine", "coal")
    cycle = simulate_pipeline_cycle(player, mine_id)
    sold = int(cycle.get("sold", 0))
    if sold > 0:
        value = sold * max(1, int(MINES_CONFIG.get(mine_id, {}).get("multiplier", 1)))
        update_quest_progress(player, "earn_coins", value)
    try_roll_diamond_drop(player, 1)
    roll_resource_drops(player, 1)
    update_museum_pool(player)
    player["last_offline_check"] = time.time()
    await save_player(user_id, player)


async def tick_auto_manager_income(context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = context.job.chat_id
    player = await get_player(user_id)
    if not player.get("has_started", False):
        return
    lvl = int(player.get("auto_manager_lvl", 0))
    if lvl <= 0:
        return
    base = calc_income_per_tick(player, player.get("active_mine", "coal"))
    earned = int(base * 0.35 * (1 + lvl * 0.05))
    if earned <= 0:
        return
    mine_id = player.get("active_mine", "coal")
    for _ in range(max(1, int(earned / max(1, calc_income_per_tick(player, mine_id)) * 2))):
        simulate_pipeline_cycle(player, mine_id)
    player["coins"] += earned  # Auto manager keeps slower direct helper income
    player["stats"]["total_earned"] += earned
    update_quest_progress(player, "earn_coins", earned)
    try_roll_diamond_drop(player, 1)
    roll_resource_drops(player, 1)
    update_museum_pool(player)
    player["last_offline_check"] = time.time()
    await save_player(user_id, player)


async def notification_sweep_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    if context.application.job_queue is None:
        return

    offset = int(context.job.data.get("offset", 0)) if context.job and context.job.data else 0
    now_ts = time.time()
    processed = 0
    next_offset = 0

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT user_id, data FROM players ORDER BY user_id LIMIT ? OFFSET ?",
            (NOTIFIER_BATCH_SIZE, offset),
        ) as cursor:
            rows = await cursor.fetchall()

    if not rows:
        next_offset = 0
    else:
        next_offset = offset + len(rows)

    for user_id, data in rows:
        processed += 1
        try:
            player = json.loads(data)
        except Exception:
            continue

        changed = ensure_player_defaults(player)
        if not player.get("has_started", False):
            if changed:
                await save_player(user_id, player)
            continue

        settings = get_settings(player)
        if not notifications_enabled(player):
            if changed:
                await save_player(user_id, player)
            continue

        sent_any = False

        if (
            bool(settings.get("notify_daily_ready", True))
            and is_daily_ready(player, now_ts)
            and (now_ts - get_last_notified(player, "daily_ready")) >= DAILY_NOTIFY_COOLDOWN_SECONDS
            and can_send_unsolicited(player, now_ts)
        ):
            if await safe_send_message(context, user_id, f"🎁 {t(player, 'daily_notify_text')}"):
                set_last_notified(player, "daily_ready", now_ts)
                mark_unsolicited_sent(player, now_ts)
                sent_any = True

        if not sent_any and bool(settings.get("notify_expedition_done", True)) and can_send_unsolicited(player, now_ts):
            active_exp = player.get("expeditions", {}).get("active")
            if isinstance(active_exp, dict):
                if now_ts >= float(active_exp.get("ends_at", 0)) and not bool(active_exp.get("done_notified", False)):
                    if (now_ts - get_last_notified(player, "exp_done")) >= DAILY_NOTIFY_COOLDOWN_SECONDS:
                        if await safe_send_message(context, user_id, f"🧭 {t(player, 'exp_done_notify')}"):
                            active_exp["done_notified"] = True
                            set_last_notified(player, "exp_done", now_ts)
                            mark_unsolicited_sent(player, now_ts)
                            sent_any = True

        if not sent_any and bool(settings.get("notify_manager_ready", True)) and can_send_unsolicited(player, now_ts):
            mgr_skills = player.get("manager_skills", {})
            if isinstance(mgr_skills, dict):
                for slot, state in mgr_skills.items():
                    if not isinstance(state, dict):
                        continue
                    cd_until = float(state.get("cd_until", 0))
                    notified = bool(state.get("notified", True))
                    if cd_until > 0 and cd_until <= now_ts and not notified:
                        if await safe_send_message(
                            context,
                            user_id,
                            f"🤖 {t(player, 'manager_ready_text', slot=slot)}",
                        ):
                            state["notified"] = True
                            set_last_notified(player, "manager_ready", now_ts)
                            mark_unsolicited_sent(player, now_ts)
                            sent_any = True
                        break

        if (
            not sent_any
            and bool(settings.get("notify_event_end", True))
            and can_send_unsolicited(player, now_ts)
            and bool(player.get("event_active", False))
        ):
            event_end_at = float(player.get("event_end_at", 0))
            if event_end_at and now_ts >= event_end_at:
                if await safe_send_message(context, user_id, f"📣 {t(player, 'event_end_text')}"):
                    player["event_active"] = False
                    set_last_notified(player, "event_end", now_ts)
                    mark_unsolicited_sent(player, now_ts)
                    sent_any = True

        if (
            not sent_any
            and bool(settings.get("notify_prestige_ready", False))
            and player.get("coins", 0) >= PRESTIGE_THRESHOLD
            and (now_ts - get_last_notified(player, "prestige_ready")) >= PRESTIGE_NOTIFY_COOLDOWN_SECONDS
            and can_send_unsolicited(player, now_ts)
        ):
            if await safe_send_message(context, user_id, f"⭐ {t(player, 'prestige_ready_text')}"):
                set_last_notified(player, "prestige_ready", now_ts)
                mark_unsolicited_sent(player, now_ts)
                sent_any = True

        if changed or sent_any:
            await save_player(user_id, player)

    if context.job and isinstance(context.job.data, dict):
        context.job.data["offset"] = next_offset
    logger.debug("Notification sweep processed %s users; next_offset=%s", processed, next_offset)


# =========================
# UI / handlers
# =========================


def build_creator_subscribe_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Подписаться на канал", url=CREATOR_CHANNEL_URL)],
            [InlineKeyboardButton("Проверить подписку", callback_data="creator_bonus_check")],
            [InlineKeyboardButton("🔙 Назад", callback_data="main_menu")],
        ]
    )


def build_lang_auto_kb(player: dict[str, Any], detected_lang: str) -> InlineKeyboardMarkup:
    ui_lang = get_player_lang(player)
    lang_name = lang_name_for_ui(detected_lang, ui_lang)
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"✅ {t(player, 'lang_auto_yes', lang_name=lang_name)}", callback_data=f"lang_auto_yes_{detected_lang}")],
            [InlineKeyboardButton(f"❌ {t(player, 'lang_auto_no')}", callback_data="lang_auto_no")],
        ]
    )


async def show_language_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)
    lang = get_player_lang(player)
    current_name = lang_name_for_ui(lang, lang)
    text = f"🌐 *{t(player, 'lang_menu_title')}*\n\n{t(player, 'lang_current')}: *{current_name}*"
    kb = [
        [InlineKeyboardButton("English", callback_data="lang_set_en")],
        [InlineKeyboardButton("Русский", callback_data="lang_set_ru")],
        [InlineKeyboardButton("O‘zbekcha", callback_data="lang_set_uz")],
        [InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="main_menu")],
    ]
    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.MARKDOWN,
    )


def notif_status(player: dict[str, Any], key: str) -> str:
    settings = get_settings(player)
    return t(player, "notif_on") if bool(settings.get(key, False)) else t(player, "notif_off")


async def show_notifications_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)
    settings = get_settings(player)
    text = (
        f"🔔 *{t(player, 'notif_menu_title')}*\n\n"
        f"{t(player, 'notif_master')}: *{notif_status(player, 'notifications_enabled')}*\n"
        f"{t(player, 'notif_manager_ready')}: *{notif_status(player, 'notify_manager_ready')}*\n"
        f"{t(player, 'notif_event_end')}: *{notif_status(player, 'notify_event_end')}*\n"
        f"{t(player, 'notif_daily_ready')}: *{notif_status(player, 'notify_daily_ready')}*\n"
        f"{t(player, 'notif_prestige_ready')}: *{notif_status(player, 'notify_prestige_ready')}*\n"
        f"{t(player, 'notify_expedition_done')}: *{notif_status(player, 'notify_expedition_done')}*\n"
        f"{t(player, 'notif_updates')}: *{notif_status(player, 'notify_updates')}*"
    )
    kb = [
        [InlineKeyboardButton(f"{t(player, 'notif_master')}: {notif_status(player, 'notifications_enabled')}", callback_data="notif_toggle_notifications_enabled")],
        [InlineKeyboardButton(f"{t(player, 'notif_manager_ready')}: {notif_status(player, 'notify_manager_ready')}", callback_data="notif_toggle_notify_manager_ready")],
        [InlineKeyboardButton(f"{t(player, 'notif_event_end')}: {notif_status(player, 'notify_event_end')}", callback_data="notif_toggle_notify_event_end")],
        [InlineKeyboardButton(f"{t(player, 'notif_daily_ready')}: {notif_status(player, 'notify_daily_ready')}", callback_data="notif_toggle_notify_daily_ready")],
        [InlineKeyboardButton(f"{t(player, 'notif_prestige_ready')}: {notif_status(player, 'notify_prestige_ready')}", callback_data="notif_toggle_notify_prestige_ready")],
        [InlineKeyboardButton(f"{t(player, 'notify_expedition_done')}: {notif_status(player, 'notify_expedition_done')}", callback_data="notif_toggle_notify_expedition_done")],
        [InlineKeyboardButton(f"{t(player, 'notif_updates')}: {notif_status(player, 'notify_updates')}", callback_data="notif_toggle_notify_updates")],
        [InlineKeyboardButton(f"🧪 {t(player, 'notif_test')}", callback_data="notif_test")],
        [InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="main_menu")],
    ]
    if update.callback_query:
        await update.callback_query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode=ParseMode.MARKDOWN,
        )


async def show_subscription_bonus_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)
    subscribed = await is_user_subscribed_to_creator_channel(context, update.effective_user.id)
    if subscribed is None:
        status = t(player, "sub_need_admin")
    else:
        status = t(player, "sub_status_subscribed") if subscribed else t(player, "sub_status_not_subscribed")
    rewards = player.get("channel_rewards", {})
    bonus_active = bool(rewards.get("sub_bonus_given", False))
    bonus_text = t(player, "sub_bonus_active") if bonus_active else t(player, "sub_bonus_inactive")
    bonus_coins = int(rewards.get("sub_bonus_coins", 10_000))
    bonus_gems = int(rewards.get("sub_bonus_gems", 25))
    text = (
        f"✅ *{t(player, 'subscription_bonus')}*\n\n"
        f"Статус: *{status}*\n"
        f"Бонус: *{bonus_text}*\n"
        f"Награда: +{bonus_coins} 💰 / +{bonus_gems} 💎\n\n"
        f"_{t(player, 'reaction_note')}_"
    )
    kb = [[InlineKeyboardButton(f"🔄 {t(player, 'sub_check_retry')}", callback_data="creator_bonus_check")]]
    kb.append([InlineKeyboardButton(f"📢 {t(player, 'join_channel')}", url=CREATOR_CHANNEL_URL)])
    if subscribed is False:
        pass
    kb.append([InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="main_menu")])
    try:
        await update.callback_query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode=ParseMode.MARKDOWN,
        )
    except BadRequest as exc:
        if "Message is not modified" in str(exc):
            await update.callback_query.answer(t(player, "updated"))
            return
        raise


async def show_daily_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)
    ready = is_daily_ready(player)
    coins, gems, boost_id = get_daily_reward(player)
    boost_line = f"\n🎁 {BOOSTS[boost_id]['name']}" if boost_id else ""
    text = (
        f"🎁 *{t(player, 'daily_title')}*\n\n"
        f"💰 +{coins}\n"
        f"💎 +{gems}{boost_line}\n\n"
    )
    kb: list[list[InlineKeyboardButton]] = []
    if ready:
        text += f"✅ {t(player, 'daily_ready')}"
        kb.append([InlineKeyboardButton(f"🎁 {t(player, 'daily_claim')}", callback_data="daily_claim")])
    else:
        left = get_seconds_until_daily(player)
        hours = left // 3600
        minutes = (left % 3600) // 60
        text += t(player, "daily_not_ready", hours=hours, minutes=minutes)
    kb.append([InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="main_menu")])
    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.MARKDOWN,
    )


async def show_expeditions_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)
    exp_state = player.get("expeditions", {})
    active = exp_state.get("active")

    if isinstance(active, dict):
        exp_cfg = get_expedition_cfg(active.get("id", ""))
        exp_name = exp_cfg.get("name", active.get("id")) if exp_cfg else active.get("id", "Unknown")
        left = int(float(active.get("ends_at", 0)) - time.time())
        if left <= 0:
            text = (
                f"🧭 *{t(player, 'exp_menu_title')}*\n\n"
                f"{t(player, 'exp_active')}: *{exp_name}*\n"
                "✅ Completed\n"
            )
            kb = [
                [InlineKeyboardButton(f"🎁 {t(player, 'exp_claim')}", callback_data="exp_claim")],
                [InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="main_menu")],
            ]
        else:
            h, m, s = format_remain(left)
            text = (
                f"🧭 *{t(player, 'exp_menu_title')}*\n\n"
                f"{t(player, 'exp_active')}: *{exp_name}*\n"
                f"⏳ {h}h {m}m {s}s"
            )
            kb = [[InlineKeyboardButton(f"🔄 {t(player, 'refresh')}", callback_data="exp_menu")]]
            kb.append([InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="main_menu")])
    else:
        text = f"🧭 *{t(player, 'exp_menu_title')}*\n\n{t(player, 'exp_no_active')}\n\n"
        kb = []
        for exp_id, cfg in EXPEDITIONS_CONFIG.items():
            dur = int(cfg["duration_sec"])
            h = dur // 3600
            m = (dur % 3600) // 60
            reward = cfg["reward"]
            text += (
                f"• *{cfg['name']}*\n"
                f"  ⏱ {h}h {m}m\n"
                f"  💰 +{int(reward.get('coins', 0))} | 💎 +{int(reward.get('gems', 0))}\n\n"
            )
            kb.append([InlineKeyboardButton(f"{t(player, 'exp_start')}: {cfg['name']}", callback_data=f"exp_start_{exp_id}")])
        kb.append([InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="main_menu")])

    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.MARKDOWN,
    )


async def show_inventory_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)
    inv = player.get("inventory", {})
    text = (
        f"🎁 *{t(player, 'inventory_title')}*\n\n"
        f"Boost x2: `{int(inv.get('boost_x2', 0))}`\n"
        f"Boost x5: `{int(inv.get('boost_x5', 0))}`\n"
        f"Boost x10: `{int(inv.get('boost_x10', 0))}`\n\n"
        f"{CHESTS_CONFIG['chest_basic']['name']}: `{int(inv.get('chest_basic', 0))}`\n"
        f"{CHESTS_CONFIG['chest_rare']['name']}: `{int(inv.get('chest_rare', 0))}`\n"
    )
    kb = [
        [InlineKeyboardButton(f"{t(player, 'open_chest')} Basic", callback_data="inv_open_chest_basic")],
        [InlineKeyboardButton(f"{t(player, 'open_chest')} Rare", callback_data="inv_open_chest_rare")],
        [InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="main_menu")],
    ]
    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.MARKDOWN,
    )


async def show_managers_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)
    m_inv = player.get("manager_inventory", {})
    text = f"👷 *{t(player, 'managers_title')}*\n\n"
    kb: list[list[InlineKeyboardButton]] = []

    for manager_id in MANAGER_POOL:
        if manager_id not in m_inv:
            continue
        cfg = get_manager_cfg(manager_id)
        entry = m_inv.get(manager_id, {})
        lvl = int(entry.get("level", 1))
        frags = int(entry.get("fragments", 0))
        needed = 10 * lvl
        text += f"*{cfg['name']}* ({cfg['rarity']})\nLvl `{lvl}` | Fragments `{frags}/{needed}`\n\n"
        kb.append([InlineKeyboardButton(f"⬆ {cfg['name']}", callback_data=f"mgr_level_{manager_id}")])
        for slot in ("shaft", "elevator", "warehouse"):
            state = "ON" if bool(player.get("manager_automation", {}).get(slot, True)) else "OFF"
            kb.append(
                [
                    InlineKeyboardButton(
                        f"{get_slot_label(player, slot)} <- {cfg['name']}",
                        callback_data=f"mgr_assign_{slot}_{manager_id}",
                    )
                ]
            )
            kb.append(
                [
                    InlineKeyboardButton(
                        f"{get_slot_label(player, slot)} auto: {state}",
                        callback_data=f"mgr_auto_{slot}",
                    )
                ]
            )

    if not any(mid in m_inv for mid in MANAGER_POOL):
        text += "No managers yet."
    kb.append([InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="main_menu")])
    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.MARKDOWN,
    )


def ability_status(player: dict[str, Any], slot: str) -> str:
    state = player.get("manager_skills", {}).get(slot, {})
    now_ts = time.time()
    active_until = float(state.get("active_until", 0))
    cd_until = float(state.get("cd_until", 0))
    if now_ts < active_until:
        rem = int(active_until - now_ts)
        h, m, s = format_remain(rem)
        total_m = h * 60 + m
        return t(player, "ability_active", minutes=total_m, seconds=s)
    if now_ts < cd_until:
        rem = int(cd_until - now_ts)
        h, m, s = format_remain(rem)
        total_m = h * 60 + m
        return t(player, "ability_cooldown", minutes=total_m, seconds=s)
    return t(player, "ability_ready")


async def show_abilities_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)
    text = f"⚡ *{t(player, 'abilities_title')}*\n\n"
    kb: list[list[InlineKeyboardButton]] = []
    assigned = player.get("assigned_managers", {})
    for slot in ("shaft", "elevator", "warehouse"):
        manager_id = assigned.get(slot)
        slot_name = get_slot_label(player, slot)
        if manager_id:
            cfg = get_manager_cfg(manager_id)
            status = ability_status(player, slot)
            text += f"*{slot_name}*: {cfg['name']} — `{status}`\n"
            kb.append([InlineKeyboardButton(f"⚡ {slot_name}", callback_data=f"ability_use_{slot}")])
        else:
            text += f"*{slot_name}*: - ({t(player, 'ability_no_manager')})\n"
    kb.append([InlineKeyboardButton(f"🔄 {t(player, 'refresh')}", callback_data="abilities_menu")])
    kb.append([InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="main_menu")])
    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.MARKDOWN,
    )


def format_reward_short(reward: dict[str, Any]) -> str:
    parts: list[str] = []
    for key in ("coins", "gems", "chest_basic", "chest_rare", "boost_x2", "boost_x5", "boost_x10", "manager_fragments"):
        val = int(reward.get(key, 0))
        if val > 0:
            parts.append(f"{key}:{val}")
    return ", ".join(parts) if parts else "-"


async def show_pass_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)
    season = player.get("season", {})
    left = get_season_time_left()
    lh, lm, _ = format_remain(left)
    level = int(season.get("level", 1))
    xp = int(season.get("xp", 0))
    xp_need = season_xp_needed(level) if level < PASS_MAX_LEVEL else 0
    premium = bool(season.get("premium", False))

    text = (
        f"🎟 *{t(player, 'pass_title')}*\n\n"
        f"{t(player, 'season_left')}: `{lh}h {lm}m`\n"
        f"{t(player, 'season_level')}: `{level}`\n"
        f"{t(player, 'season_xp')}: `{xp}`/{xp_need if xp_need else 'MAX'}\n"
        f"Premium: *{t(player, 'pass_premium_on') if premium else t(player, 'pass_premium_off')}*\n\n"
    )
    for lv in range(level, min(PASS_MAX_LEVEL, level + 4) + 1):
        rw = PASS_REWARDS.get(lv, {})
        text += f"*L{lv}* free[{format_reward_short(rw.get('free', {}))}] premium[{format_reward_short(rw.get('premium', {}))}]\n"

    kb = [[InlineKeyboardButton(f"🎁 {t(player, 'pass_claim')}", callback_data="pass_claim")]]
    if not premium:
        kb.append([InlineKeyboardButton(f"💎 {t(player, 'pass_buy_premium')} ({PASS_PREMIUM_GEMS_COST})", callback_data="pass_buy_premium")])
    kb.append([InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="main_menu")])
    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.MARKDOWN,
    )


async def show_quests_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)
    reset_quests_if_needed(player)
    quests = player.get("quests", {})
    text = f"📜 *{t(player, 'quests_title')}*\n\n"
    kb: list[list[InlineKeyboardButton]] = []
    for bucket, label in (("daily", t(player, "daily_quests")), ("weekly", t(player, "weekly_quests"))):
        text += f"*{label}*\n"
        items = quests.get(bucket, {}).get("items", {})
        claimed = set(quests.get(bucket, {}).get("claimed", []))
        for qid, q in items.items():
            progress = int(q.get("progress", 0))
            target = int(q.get("target", 1))
            done = bool(q.get("done", False))
            claimed_mark = "✅" if qid in claimed else ("🟢" if done else "⚪")
            text += f"{claimed_mark} `{qid}` {t(player, 'quest_progress')}: {progress}/{target}\n"
            if done and qid not in claimed:
                kb.append([InlineKeyboardButton(f"{t(player, 'quest_claim')}: {qid}", callback_data=f"quest_claim_{bucket}_{qid}")])
        text += "\n"
    kb.append([InlineKeyboardButton(f"🔄 {t(player, 'refresh')}", callback_data="quests_menu")])
    kb.append([InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="main_menu")])
    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.MARKDOWN,
    )


async def show_company_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)
    company = player.get("company", {})
    lvl = int(company.get("level", 1))
    xp = int(company.get("xp", 0))
    need = 150 + max(0, lvl - 1) * 60
    text = (
        f"🏢 *{t(player, 'company')}*\n\n"
        f"{t(player, 'company_name')}: *{company.get('name', 'My Company')}*\n"
        f"{t(player, 'company_level')}: `{lvl}`\n"
        f"{t(player, 'company_xp')}: `{xp}/{need}`"
    )
    kb = [[InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="menu_progress")]]
    await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)


async def show_museum_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)
    update_museum_pool(player)
    museum = player.get("museum", {})
    if not bool(museum.get("owned", False)):
        text = f"🏛 *{t(player, 'museum')}*\n\n{t(player, 'museum_not_owned')}\nЦена: `{MUSEUM_BUY_COST}`"
        kb = [
            [InlineKeyboardButton(f"🏛 {t(player, 'museum_buy')} ({MUSEUM_BUY_COST})", callback_data="museum_buy")],
            [InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="menu_progress")],
        ]
    else:
        lvl = int(museum.get("level", 1))
        pool = int(museum.get("income_pool", 0))
        boost = get_museum_exhibit_multiplier(player)
        upgrade_cost = int(MUSEUM_UPGRADE_BASE_COST * (1.7 ** (lvl - 1)))
        wait = max(0, int((24 * 60 * 60) - (time.time() - float(museum.get("last_claim", 0))))) if museum.get("last_claim", 0) else 0
        text = (
            f"🏛 *{t(player, 'museum')}*\n\n"
            f"Level: `{lvl}`\n"
            f"Pool: `{pool}`\n"
            f"Exhibit boost: `x{boost:.2f}`\n"
            f"Claim in: `{wait // 3600}h {(wait % 3600) // 60}m`"
        )
        kb = [
            [InlineKeyboardButton(f"⬆ {t(player, 'museum_upgrade')} ({upgrade_cost})", callback_data="museum_upgrade")],
            [InlineKeyboardButton(f"💰 {t(player, 'museum_claim')}", callback_data="museum_claim")],
            [InlineKeyboardButton(f"🦴 {t(player, 'collection')}", callback_data="collection_menu")],
            [InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="menu_progress")],
        ]
    await save_player(update.effective_user.id, player)
    await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)


async def show_collection_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)
    assembled = set(player.get("fossils", {}).get("assembled", []))
    parts = player.get("fossils", {}).get("parts", {})
    text = f"🦴 *{t(player, 'collection')}*\n\nAssembled: `{len(assembled)}/{len(FOSSIL_SPECIES)}`\n\n"
    kb: list[list[InlineKeyboardButton]] = []
    for species_id in list(FOSSIL_SPECIES.keys())[:12]:
        cfg = FOSSIL_SPECIES[species_id]
        done = species_id in assembled
        have = 0
        for p in cfg["parts"]:
            if int(parts.get(f"{species_id}:{p}", 0)) > 0:
                have += 1
        text += f"{'✅' if done else '⚪'} {cfg['name']} `{have}/{len(cfg['parts'])}`\n"
        if (not done) and have >= len(cfg["parts"]):
            kb.append([InlineKeyboardButton(f"{t(player, 'assemble')}: {cfg['name']}", callback_data=f"assemble_{species_id}")])
    kb.append([InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="museum_menu")])
    await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)


async def show_leaderboard_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)
    kb = [
        [InlineKeyboardButton(t(player, "lb_prestige"), callback_data="leaderboard_prestige")],
        [InlineKeyboardButton(t(player, "lb_coins"), callback_data="leaderboard_coins")],
        [InlineKeyboardButton(t(player, "lb_gems"), callback_data="leaderboard_gems")],
        [InlineKeyboardButton(t(player, "lb_income"), callback_data="leaderboard_income")],
        [InlineKeyboardButton(t(player, "lb_company"), callback_data="leaderboard_company")],
        [InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="menu_progress")],
    ]
    await update.callback_query.edit_message_text(
        f"🏆 *{t(player, 'leaderboards')}*",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.MARKDOWN,
    )


async def show_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE, metric: str) -> None:
    metric_sql = {
        "prestige": "json_extract(data, '$.prestige_points')",
        "coins": "json_extract(data, '$.coins')",
        "gems": "json_extract(data, '$.gems')",
        "income": "json_extract(data, '$.stats.total_earned')",
        "company": "json_extract(data, '$.company.level')",
    }.get(metric, "json_extract(data, '$.prestige_points')")
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            f"SELECT username, {metric_sql} as val FROM players ORDER BY val DESC LIMIT 10"
        ) as cursor:
            rows = await cursor.fetchall()
    text = f"🏆 *{metric.upper()}*\n\n"
    for i, (name, val) in enumerate(rows, 1):
        text += f"{i}. {name or 'Игрок'} — {int(val or 0)}\n"
    player = await get_player(update.effective_user.id)
    kb = [[InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="leaderboard_menu")]]
    await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)


async def show_spin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)
    left = get_wheel_ready_in(player)
    if left <= 0:
        text = f"🎡 *{t(player, 'free_spin')}*\n\nReady!"
        kb = [[InlineKeyboardButton(f"🎡 {t(player, 'spin_now')}", callback_data="spin_now")]]
    else:
        text = f"🎡 *{t(player, 'free_spin')}*\n\n{t(player, 'spin_wait')}: `{left // 3600}h {(left % 3600)//60}m`"
        kb = [[InlineKeyboardButton(f"🔄 {t(player, 'refresh')}", callback_data="spin_menu")]]
    kb.append([InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="menu_progress")])
    await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)


async def is_user_subscribed_to_creator_channel(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool | None:
    try:
        member = await context.application.bot.get_chat_member(CREATOR_CHANNEL, user_id)
    except Exception as exc:
        logger.info("Cannot check channel subscription for user %s: %s", user_id, exc)
        return None

    return member.status in ("member", "administrator", "creator")


async def try_grant_creator_subscription_reward(
    context: ContextTypes.DEFAULT_TYPE, user_id: int, player: dict[str, Any]
) -> tuple[bool, str]:
    subscribed = await is_user_subscribed_to_creator_channel(context, user_id)
    if subscribed is None:
        return False, t(player, "sub_need_admin")
    if not subscribed:
        return False, "Сначала подпишитесь на канал, затем нажмите проверку"

    rewards = player.get("channel_rewards", {})
    if bool(rewards.get("sub_bonus_given", False)):
        return False, "Бонус подписки уже активен"

    bonus_coins = int(rewards.get("sub_bonus_coins", 10_000))
    bonus_gems = int(rewards.get("sub_bonus_gems", 25))
    player["coins"] += bonus_coins
    player["gems"] += bonus_gems
    player["creator_sub_reward_claimed"] = True
    player["creator_sub_bonus_active"] = True
    rewards["sub_bonus_given"] = True
    rewards["granted_at"] = time.time()
    player["channel_rewards"] = rewards
    player["last_offline_check"] = time.time()
    await save_player(user_id, player)
    return True, f"+{bonus_coins} 💰 и +{bonus_gems} 💎 начислено"


async def enforce_channel_subscription_policy(
    context: ContextTypes.DEFAULT_TYPE, user_id: int, player: dict[str, Any]
) -> tuple[bool, str | None]:
    rewards = player.get("channel_rewards", {})
    bonus_coins = int(rewards.get("sub_bonus_coins", 10_000))
    bonus_gems = int(rewards.get("sub_bonus_gems", 25))

    subscribed = await is_user_subscribed_to_creator_channel(context, user_id)
    if subscribed is None:
        return False, t(player, "sub_verify_fail")

    if subscribed:
        if not bool(rewards.get("sub_bonus_given", False)):
            player["coins"] += bonus_coins
            player["gems"] += bonus_gems
            player["creator_sub_reward_claimed"] = True
            player["creator_sub_bonus_active"] = True
            rewards["sub_bonus_given"] = True
            rewards["granted_at"] = time.time()
            player["channel_rewards"] = rewards
            player["ledger"].append(
                {
                    "type": "sub_bonus_grant",
                    "amount_coins": bonus_coins,
                    "amount_gems": bonus_gems,
                    "ts": time.time(),
                }
            )
            player["last_offline_check"] = time.time()
            await save_player(user_id, player)
            return True, t(player, "sub_bonus_granted")
        return False, None

    if bool(rewards.get("sub_bonus_given", False)):
        player["coins"] -= bonus_coins
        player["gems"] -= bonus_gems
        player["creator_sub_reward_claimed"] = False
        player["creator_sub_bonus_active"] = False
        rewards["sub_bonus_given"] = False
        player["channel_rewards"] = rewards
        player["ledger"].append(
            {
                "type": "sub_bonus_clawback",
                "amount_coins": -bonus_coins,
                "amount_gems": -bonus_gems,
                "ts": time.time(),
            }
        )
        player["last_offline_check"] = time.time()
        await save_player(user_id, player)
        return True, t(player, "sub_bonus_clawed")

    return False, None


# NOTE: Telegram Bot API cannot reliably verify who reacted to channel posts.
# Reaction-based rewards are intentionally not implemented.


def get_automation_upgrade_price(player: dict[str, Any]) -> int:
    return 200 * (int(player.get("automation_lvl", 0)) + 1)


def get_auto_manager_upgrade_price(player: dict[str, Any]) -> int:
    return 300 * (int(player.get("auto_manager_lvl", 0)) + 1)


def get_diamond_luck_upgrade_price(player: dict[str, Any]) -> int:
    lvl = int(player.get("drops", {}).get("diamond_chance_lvl", 0))
    return 200 * (lvl + 1)


def get_diamond_chance(player: dict[str, Any]) -> float:
    lvl = int(player.get("drops", {}).get("diamond_chance_lvl", 0))
    return min(DIAMOND_MAX_CHANCE, DIAMOND_BASE_CHANCE * (lvl + 1))


def try_roll_diamond_drop(player: dict[str, Any], attempts: int = 1) -> int:
    drops = player.get("drops", {})
    chance = get_diamond_chance(player)
    got = 0
    for _ in range(max(1, attempts)):
        roll = next_rng(player, 10000)
        if roll < int(chance * 10000):
            got += 1
    if got > 0:
        drops["diamonds"] = int(drops.get("diamonds", 0)) + got
        drops["pending"] = int(drops.get("pending", 0)) + got
    player["drops"] = drops
    return got


def add_company_xp(player: dict[str, Any], amount: int) -> None:
    if amount <= 0:
        return
    company = player.get("company", {})
    company["xp"] = int(company.get("xp", 0)) + int(amount)
    while True:
        lvl = int(company.get("level", 1))
        need = 150 + (lvl - 1) * 60
        if int(company.get("xp", 0)) < need:
            break
        company["xp"] = int(company.get("xp", 0)) - need
        company["level"] = lvl + 1
    player["company"] = company


def get_luck_multiplier(player: dict[str, Any]) -> float:
    research_bonus = 1 + int(player.get("research", {}).get("efficiency", 0)) * 0.03
    company_bonus = 1 + max(0, int(player.get("company", {}).get("level", 1)) - 1) * 0.01
    return max(1.0, research_bonus * company_bonus)


def roll_resource_drops(player: dict[str, Any], attempts: int = 1) -> dict[str, int]:
    attempts = max(1, attempts)
    resources = player.get("resources", {})
    luck = get_luck_multiplier(player)
    got: dict[str, int] = {}
    for _ in range(attempts):
        for r_name, base_ch in RESOURCE_BASE_CHANCES.items():
            chance = min(DIAMOND_MAX_CHANCE if r_name == "diamond" else 0.2, base_ch * luck)
            roll = next_rng(player, 100000)
            if roll < int(chance * 100000):
                resources[r_name] = int(resources.get(r_name, 0)) + 1
                got[r_name] = int(got.get(r_name, 0)) + 1
    player["resources"] = resources
    if got:
        drops = player.get("drops", {})
        diamonds = int(got.get("diamond", 0))
        if diamonds > 0:
            drops["diamonds"] = int(drops.get("diamonds", 0)) + diamonds
            drops["pending"] = int(drops.get("pending", 0)) + diamonds
            player["drops"] = drops
        assign_fossil_parts_from_resources(player, got)
    return got


def assign_fossil_parts_from_resources(player: dict[str, Any], resource_gains: dict[str, int]) -> None:
    fossils = player.get("fossils", {})
    parts = fossils.get("parts", {})
    count = int(resource_gains.get("fossil_fragment", 0)) + int(resource_gains.get("rare_dino_part", 0)) * 2
    for _ in range(max(0, count)):
        sp_keys = list(FOSSIL_SPECIES.keys())
        if not sp_keys:
            break
        sp = sp_keys[next_rng(player, len(sp_keys))]
        p_names = FOSSIL_SPECIES[sp]["parts"]
        p = p_names[next_rng(player, len(p_names))]
        key = f"{sp}:{p}"
        parts[key] = int(parts.get(key, 0)) + 1
    fossils["parts"] = parts
    player["fossils"] = fossils


def get_museum_exhibit_multiplier(player: dict[str, Any]) -> float:
    assembled = len(player.get("fossils", {}).get("assembled", []))
    return min(FOSSIL_EXHIBIT_BONUS_CAP, 1 + assembled * FOSSIL_EXHIBIT_BONUS)


def update_museum_pool(player: dict[str, Any]) -> None:
    museum = player.get("museum", {})
    if not bool(museum.get("owned", False)):
        return
    now_ts = time.time()
    last_tick = float(museum.get("last_tick", now_ts))
    delta = max(0, int(now_ts - last_tick))
    if delta <= 0:
        return
    lvl = int(museum.get("level", 1))
    per_hour = 250 * lvl * get_museum_exhibit_multiplier(player)
    income = int((delta / 3600) * per_hour)
    museum["income_pool"] = int(museum.get("income_pool", 0)) + income
    museum["last_tick"] = now_ts
    player["museum"] = museum


def simulate_pipeline_cycle(player: dict[str, Any], mine_id: str) -> dict[str, int]:
    mine = player.get("mines", {}).get(mine_id)
    if not mine:
        return {"produced": 0, "moved": 0, "delivered": 0, "sold": 0}
    s_rate, e_rate, w_rate, _ = get_mine_stats(player, mine_id)
    produced = max(0, int(s_rate))
    moved = min(int(mine.get("mine_storage", 0)) + produced, max(0, int(e_rate)))
    delivered = min(int(mine.get("elevator_storage", 0)) + moved, max(0, int(w_rate)))

    mine["mine_storage"] = int(mine.get("mine_storage", 0)) + produced - moved
    mine["elevator_storage"] = int(mine.get("elevator_storage", 0)) + moved - delivered
    mine["warehouse_storage"] = int(mine.get("warehouse_storage", 0)) + delivered

    sold = 0
    if bool(mine.get("auto_sell", False)) and int(mine.get("warehouse_storage", 0)) > 0:
        sold = int(mine.get("warehouse_storage", 0))
        mine["warehouse_storage"] = 0
        value = sold * max(1, int(MINES_CONFIG.get(mine_id, {}).get("multiplier", 1)))
        player["coins"] += value
        player["stats"]["total_earned"] += value
        update_quest_progress(player, "earn_coins", value)

    player["mines"][mine_id] = mine
    return {"produced": produced, "moved": moved, "delivered": delivered, "sold": sold}


def sell_warehouse_ore(player: dict[str, Any], mine_id: str) -> int:
    mine = player.get("mines", {}).get(mine_id)
    if not mine:
        return 0
    ore = int(mine.get("warehouse_storage", 0))
    if ore <= 0:
        return 0
    mine["warehouse_storage"] = 0
    value = ore * max(1, int(MINES_CONFIG.get(mine_id, {}).get("multiplier", 1)))
    player["coins"] += value
    player["stats"]["total_earned"] += value
    update_quest_progress(player, "earn_coins", value)
    player["mines"][mine_id] = mine
    return value


def get_wheel_ready_in(player: dict[str, Any]) -> int:
    return max(0, int(float(player.get("wheel", {}).get("next_at", 0)) - time.time()))


def spin_free_wheel(player: dict[str, Any]) -> str:
    wheel = player.get("wheel", {})
    if time.time() < float(wheel.get("next_at", 0)):
        return ""
    roll = next_rng(player, 100)
    result = ""
    if roll < 35:
        amount = 2000 + next_rng(player, 5000)
        player["coins"] += amount
        update_quest_progress(player, "earn_coins", amount)
        result = f"+{amount} coins"
    elif roll < 55:
        amount = 15 + next_rng(player, 30)
        player["gems"] += amount
        result = f"+{amount} gems"
    elif roll < 70:
        player["inventory"]["chest_basic"] = int(player["inventory"].get("chest_basic", 0)) + 1
        result = "+1 chest_basic"
    elif roll < 80:
        player["inventory"]["boost_x2"] = int(player["inventory"].get("boost_x2", 0)) + 1
        result = "+1 boost_x2"
    elif roll < 90:
        comp = ["shaft", "elevator", "warehouse", "level"][next_rng(player, 4)]
        mine = player["mines"][player["active_mine"]]
        if comp == "level":
            mine["level"] = int(mine.get("level", 1)) + 1
        else:
            mine[comp]["level"] = int(mine[comp].get("level", 1)) + 1
        result = f"+1 {comp}"
    else:
        museum = player.get("museum", {})
        museum["level"] = int(museum.get("level", 1)) + 1
        player["museum"] = museum
        result = "+1 museum level"
    wheel["next_at"] = time.time() + WHEEL_COOLDOWN_SECONDS
    player["wheel"] = wheel
    add_company_xp(player, 20)
    return result


def open_manager_chest(player: dict[str, Any]) -> str:
    idx = int(player.get("manager_chest_opened", 0))
    manager_id = MANAGER_POOL[idx % len(MANAGER_POOL)]
    player["manager_chest_opened"] = idx + 1

    inventory = player.get("manager_inventory", {})
    if manager_id in inventory:
        inventory[manager_id]["fragments"] = int(inventory[manager_id].get("fragments", 0)) + 3
        player["manager_inventory"] = inventory
        return f"Сундук: +3 фрагмента менеджера `{manager_id}`"

    inventory[manager_id] = {"level": 1, "fragments": 1}
    player["manager_inventory"] = inventory
    return f"Сундук: новый менеджер `{manager_id}`"


def get_upgrade_advice(player: dict[str, Any], mine_id: str) -> dict[str, Any]:
    mine = player["mines"][mine_id]
    current_income = calc_income_per_tick(player, mine_id)
    _, _, _, bottleneck = get_mine_stats(player, mine_id)

    candidates: list[tuple[str, int, int, float]] = []

    for component in ("shaft", "elevator", "warehouse", "mine_level"):
        simulated = copy.deepcopy(player)
        s_mine = simulated["mines"][mine_id]
        if component == "mine_level":
            level = int(s_mine.get("level", 1))
            cost = get_upgrade_cost(simulated, 10_000, level, cost_type="mine_level")
            s_mine["level"] = level + 1
        else:
            level = int(s_mine[component]["level"])
            cost = get_upgrade_cost(simulated, 100, level)
            s_mine[component]["level"] = level + 1

        new_income = calc_income_per_tick(simulated, mine_id)
        gain = new_income - current_income
        roi = (gain / cost) if cost > 0 else 0
        candidates.append((component, cost, gain, roi))

    best = max(candidates, key=lambda x: x[3])
    component, cost, gain, roi = best

    if gain <= 0:
        bottleneck_component = "shaft"
        if bottleneck == "Лифт":
            bottleneck_component = "elevator"
        elif bottleneck == "Склад":
            bottleneck_component = "warehouse"

        b_level = int(mine[bottleneck_component]["level"])
        b_cost = get_upgrade_cost(player, 100, b_level)
        return {
            "bottleneck": bottleneck,
            "recommended": bottleneck_component,
            "cost": b_cost,
            "gain": 0,
            "roi": 0,
            "note": "Прямой прирост не определен, усиливайте узкое место.",
        }

    return {
        "bottleneck": bottleneck,
        "recommended": component,
        "cost": cost,
        "gain": gain,
        "roi": roi,
        "note": "Лучшая окупаемость по текущим данным.",
    }


async def show_shop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)
    auto_price = get_automation_upgrade_price(player)
    automation_lvl = int(player.get("automation_lvl", 0))
    auto_manager_lvl = int(player.get("auto_manager_lvl", 0))
    auto_manager_price = get_auto_manager_upgrade_price(player)
    diamond_lvl = int(player.get("drops", {}).get("diamond_chance_lvl", 0))
    diamond_price = get_diamond_luck_upgrade_price(player)
    diamond_chance = get_diamond_chance(player) * 100

    enough_auto = "доступно" if player["gems"] >= auto_price else "не хватает гемов"
    enough_auto_manager = "доступно" if player["gems"] >= auto_manager_price else "не хватает гемов"
    enough_diamond = "доступно" if player["gems"] >= diamond_price else "не хватает гемов"
    enough_x2 = "доступно" if player["gems"] >= SHOP_BOOST_PRICES["boost_x2"] else "не хватает гемов"
    enough_x5 = "доступно" if player["gems"] >= SHOP_BOOST_PRICES["boost_x5"] else "не хватает гемов"
    enough_x10 = "доступно" if player["gems"] >= SHOP_BOOST_PRICES["boost_x10"] else "не хватает гемов"
    enough_basic = "доступно" if int(player["coins"]) >= SHOP_CHEST_PRICES["chest_basic"] else "не хватает монет"
    enough_rare = "доступно" if int(player["coins"]) >= SHOP_CHEST_PRICES["chest_rare"] else "не хватает монет"
    enough_premium = "доступно" if player["gems"] >= PASS_PREMIUM_GEMS_COST else "не хватает гемов"
    premium_state = t(player, "pass_premium_on") if player.get("season", {}).get("premium", False) else t(player, "pass_premium_off")

    text = (
        f"🛒 *{t(player, 'shop')}*\n\n"
        f"💎 {t(player, 'gems')}: `{player['gems']}`\n"
        f"🤖 Auto PRO уровень: `{automation_lvl}`\n\n"
        f"👔 {t(player, 'auto_manager')}: `{auto_manager_lvl}/{AUTO_MANAGER_MAX_LEVEL}`\n"
        f"💠 {t(player, 'diamond_luck')}: `{diamond_lvl}/{DIAMOND_MAX_LEVEL}` ({diamond_chance:.2f}%)\n\n"
        f"1) *Auto PRO +1*\n"
        f"   Эффект: быстрее тик и +5% к доходу за тик\n"
        f"   Цена: `{auto_price}` 💎 | Статус: *{enough_auto}*\n\n"
        f"2) *{t(player, 'auto_manager')} +1*\n"
        f"   Эффект: доп. авто доход каждые {AUTO_MANAGER_INTERVAL_SECONDS}с\n"
        f"   Цена: `{auto_manager_price}` 💎 | Статус: *{enough_auto_manager}*\n\n"
        f"3) *{t(player, 'diamond_luck')} +1*\n"
        f"   Эффект: шанс выпадения алмаза из действий/тиков\n"
        f"   Цена: `{diamond_price}` 💎 | Статус: *{enough_diamond}*\n\n"
        "4) *Boosts (gems)*\n"
        f"   x2 на 15 мин — `{SHOP_BOOST_PRICES['boost_x2']}` 💎 (*{enough_x2}*)\n"
        f"   x5 на 10 мин — `{SHOP_BOOST_PRICES['boost_x5']}` 💎 (*{enough_x5}*)\n"
        f"   x10 на 5 мин — `{SHOP_BOOST_PRICES['boost_x10']}` 💎 (*{enough_x10}*)\n\n"
        "5) *Managers (coins)*\n"
        f"   Basic chest — `{SHOP_CHEST_PRICES['chest_basic']}` 💰 (*{enough_basic}*)\n"
        f"   Rare chest — `{SHOP_CHEST_PRICES['chest_rare']}` 💰 (*{enough_rare}*)\n\n"
        "6) *Premium Pass (gems)*\n"
        f"   Цена: `{PASS_PREMIUM_GEMS_COST}` 💎 | Статус: *{enough_premium}*\n"
        f"   Состояние: *{premium_state}*\n"
    )

    kb = [
        [InlineKeyboardButton(f"Купить Auto PRO (+1) — {auto_price}💎", callback_data="shop_buy_auto")],
        [InlineKeyboardButton(f"Купить {t(player, 'auto_manager')} (+1) — {auto_manager_price}💎", callback_data="shop_buy_auto_manager")],
        [InlineKeyboardButton(f"Купить {t(player, 'diamond_luck')} (+1) — {diamond_price}💎", callback_data="shop_buy_diamond_luck")],
        [InlineKeyboardButton(f"Купить x2 — {SHOP_BOOST_PRICES['boost_x2']}💎", callback_data="shop_buy_boost_x2")],
        [InlineKeyboardButton(f"Купить x5 — {SHOP_BOOST_PRICES['boost_x5']}💎", callback_data="shop_buy_boost_x5")],
        [InlineKeyboardButton(f"Купить x10 — {SHOP_BOOST_PRICES['boost_x10']}💎", callback_data="shop_buy_boost_x10")],
        [InlineKeyboardButton(f"Купить Basic сундук — {SHOP_CHEST_PRICES['chest_basic']}💰", callback_data="shop_buy_chest_basic")],
        [InlineKeyboardButton(f"Купить Rare сундук — {SHOP_CHEST_PRICES['chest_rare']}💰", callback_data="shop_buy_chest_rare")],
        [InlineKeyboardButton(f"{t(player, 'pass_buy_premium')} — {PASS_PREMIUM_GEMS_COST}💎", callback_data="pass_buy_premium")],
        [InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="main_menu")],
    ]

    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.MARKDOWN,
    )


async def show_advisor(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)
    mine_id = player.get("active_mine", "coal")
    advice = get_upgrade_advice(player, mine_id)

    name_map = {
        "shaft": "Шахта",
        "elevator": "Лифт",
        "warehouse": "Склад",
        "mine_level": "Уровень шахты",
    }
    rec_name = name_map.get(advice["recommended"], advice["recommended"])

    text = (
        f"🧠 *{t(player, 'advisor')}*\n\n"
        f"Текущая локация: *{MINES_CONFIG[mine_id]['name']}*\n"
        f"Узкое место: *{advice['bottleneck']}*\n"
        f"Рекомендуем: *{rec_name}*\n"
        f"Цена: `{advice['cost']}`\n"
        f"Ожидаемый прирост за тик: `+{advice['gain']}`\n"
        f"ROI: `{advice['roi']:.4f}`\n\n"
        f"_{advice['note']}_"
    )
    kb = [[InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="main_menu")]]
    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.MARKDOWN,
    )


async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)
    stats = player.get("stats", {})
    dt_object = datetime.fromtimestamp(stats.get("start_date", time.time()))

    ach_list = ""
    for a_id in player.get("achievements", []):
        ach_cfg = ACHIEVEMENTS_CONFIG.get(a_id)
        if ach_cfg:
            ach_list += f"🏆 {ach_cfg['name']}\n"
    if not ach_list:
        ach_list = "Пока нет достижений"

    text = (
        f"📊 *{t(player, 'stats')} {player['name']}*\n\n"
        f"📅 В игре с: `{dt_object.strftime('%d.%m.%Y')}`\n"
        f"💰 Всего заработано: `{int(stats.get('total_earned', 0))}`\n"
        f"🔼 Всего улучшений: `{stats.get('total_upgrades', 0)}`\n"
        f"⭐ Очки престижа: `{player.get('prestige_points', 0)}`\n\n"
        f"*Достижения:*\n{ach_list}"
    )
    kb = [[InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="main_menu")]]

    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.MARKDOWN,
    )

async def show_locations(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)

    text = f"🌍 *{t(player, 'mines')}*\n\n"
    kb = []
    for mine_id, cfg in MINES_CONFIG.items():
        status = (
            "✅ Активна"
            if player["active_mine"] == mine_id
            else ("🟢 Открыта" if mine_id in player["unlocked_mines"] else f"🔒 {cfg['cost']} 💰")
        )

        text += (
            f"{cfg['color']} *{cfg['name']}*\n"
            f"└ Континент: {cfg['continent']}\n"
            f"└ Статус: {status}\n\n"
        )

        if player["active_mine"] != mine_id:
            if mine_id in player["unlocked_mines"]:
                kb.append([InlineKeyboardButton(f"Перейти: {cfg['name']}", callback_data=f"switch_{mine_id}")])
            else:
                kb.append([
                    InlineKeyboardButton(
                        f"Открыть {cfg['name']} ({cfg['cost']})",
                        callback_data=f"buy_mine_{mine_id}",
                    )
                ])

    kb.append([InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="main_menu")])
    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.MARKDOWN,
    )


async def show_research(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)

    def status(rid: str) -> str:
        return f"({player['research'].get(rid, 0)}/{RESEARCH_CONFIG[rid]['max_lvl']})"

    text = (
        f"🧬 *{t(player, 'research')}*\n\n"
        f"⚡ *{RESEARCH_CONFIG['efficiency']['name']}* {status('efficiency')}\n"
        f"├ 📈 *{RESEARCH_CONFIG['marketing']['name']}* {status('marketing')}\n"
        f"└ 📦 *{RESEARCH_CONFIG['logistics']['name']}* {status('logistics')}\n"
        f"    ├ 🤖 *{RESEARCH_CONFIG['automation']['name']}* {status('automation')}\n"
        f"    └ 🛠 *{RESEARCH_CONFIG['engineering']['name']}* {status('engineering')}\n\n"
        "_Выберите исследование для улучшения:_"
    )

    kb = []
    for r_id, cfg in RESEARCH_CONFIG.items():
        lvl = player["research"].get(r_id, 0)
        if lvl >= cfg["max_lvl"]:
            continue

        req_met = True
        if cfg["requires"]:
            req_id, req_lvl = cfg["requires"]
            req_met = player["research"].get(req_id, 0) >= req_lvl

        if req_met:
            cost = cfg["cost_base"] * (lvl + 1)
            kb.append([InlineKeyboardButton(f"🧬 {cfg['name']} (+1) — {cost}💎", callback_data=f"buy_res_{r_id}")])

    kb.append([InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="main_menu")])

    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.MARKDOWN,
    )


async def show_boosts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)
    inv = player.get("inventory", {})
    active = player.get("active_boost", {"multiplier": 1, "expires_at": 0})
    now = time.time()

    if now < active.get("expires_at", 0):
        left = int(active["expires_at"] - now)
        active_str = f"✅ Активен: x{active['multiplier']} (еще {left // 60} мин)"
    else:
        active_str = "❌ Нет активного буста"

    text = f"🚀 *{t(player, 'boosts')}*\n\n" + active_str + "\n\n*Инвентарь:*\n"
    kb = []

    for bid, cfg in BOOSTS.items():
        qty = inv.get(bid, 0)
        text += f"• {cfg['name']} — `{qty}` шт\n"
        if qty > 0:
            kb.append([InlineKeyboardButton(f"Активировать {cfg['name']}", callback_data=f"use_{bid}")])

    kb.append([InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="main_menu")])

    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.MARKDOWN,
    )


async def show_prestige(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)
    can = player["coins"] >= PRESTIGE_THRESHOLD
    mult = get_prestige_multiplier(player)

    text = (
        f"⭐ *{t(player, 'prestige_btn')}*\n\n"
        f"Текущие очки престижа: `{player['prestige_points']}`\n"
        f"Постоянный множитель: `x{mult:.2f}`\n"
        f"Порог: `{PRESTIGE_THRESHOLD}` монет\n"
        f"Ваши монеты: `{int(player['coins'])}`\n\n"
        "Престиж сбрасывает все шахты и улучшения до базовых значений."
    )

    kb = []
    if can:
        kb.append([InlineKeyboardButton("⭐ Сделать престиж", callback_data="prestige_confirm")])
    kb.append([InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="main_menu")])

    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.MARKDOWN,
    )


async def show_main_menu(
    update: Update, context: ContextTypes.DEFAULT_TYPE, refresh_nonce: str = "", screen: str = "home"
) -> None:
    user_id = update.effective_user.id
    player = await get_player(user_id)
    await maybe_send_section_banner(update, context, player, screen)
    mine_id = player["active_mine"]
    mine_data = player["mines"][mine_id]

    changed, sub_msg = await enforce_channel_subscription_policy(context, user_id, player)
    if changed:
        player = await get_player(user_id)
        mine_id = player["active_mine"]
        mine_data = player["mines"][mine_id]

    if await maybe_send_update_note(context, user_id, player):
        await save_player(user_id, player)

    s_rate, e_rate, w_rate, bottleneck = get_mine_stats(player, mine_id)
    income_tick = calc_income_per_tick(player, mine_id)
    interval = get_effective_tick_interval(player)

    pending_diamonds = int(player.get("drops", {}).get("pending", 0))
    if pending_diamonds > 0:
        player["drops"]["pending"] = 0
        await save_player(user_id, player)

    base_text = (
        f"⚒ *{t(player, 'menu_title')} {MINES_CONFIG[mine_id]['name']}* (lvl. {mine_data.get('level', 1)})\n"
        f"💰 {t(player, 'coins')}: `{int(player['coins'])}`\n"
        f"💎 {t(player, 'gems')}: `{int(player['gems'])}` | 💠 {t(player, 'diamonds')}: `{int(player.get('drops', {}).get('diamonds', 0))}`\n"
        f"⭐ {t(player, 'prestige')}: `{player['prestige_points']}` | 🤖 {t(player, 'auto_pro')}: `{player.get('automation_lvl', 0)}` | 👔 {t(player, 'auto_manager')}: `{player.get('auto_manager_lvl', 0)}`\n"
        f"⏱ {t(player, 'tick')}: `{interval}`s | {t(player, 'income_per_tick')}: `+{income_tick}` | {t(player, 'diamond_chance')}: `{get_diamond_chance(player)*100:.2f}%`\n"
        f"📈 {t(player, 'bottleneck')}: *{bottleneck}* | {t(player, 'rates')}: `{int(s_rate)}/{int(e_rate)}/{int(w_rate)}`\n"
    )
    if pending_diamonds > 0:
        base_text += f"\n💠 +{pending_diamonds} {t(player, 'diamonds')}"
    if sub_msg and screen == "settings":
        base_text += f"\n📢 {sub_msg}"
    if refresh_nonce:
        base_text += f"\n`{refresh_nonce}`"

    cost_s = get_upgrade_cost(player, 100, mine_data["shaft"]["level"])
    cost_e = get_upgrade_cost(player, 100, mine_data["elevator"]["level"])
    cost_w = get_upgrade_cost(player, 100, mine_data["warehouse"]["level"])
    cost_m = get_upgrade_cost(player, 10_000, mine_data.get("level", 1), cost_type="mine_level")

    nav = [
        InlineKeyboardButton("🏠 " + t(player, "home_tab"), callback_data="menu_home"),
        InlineKeyboardButton("📈 " + t(player, "progress_tab"), callback_data="menu_progress"),
        InlineKeyboardButton("⚙️ " + t(player, "settings_tab"), callback_data="menu_settings"),
    ]

    keyboard: list[list[InlineKeyboardButton]] = [[nav[0], nav[1], nav[2]]]
    if screen == "home":
        keyboard.extend(
            [
                [InlineKeyboardButton(f"⛏ {t(player, 'work')}", callback_data="work")],
                [InlineKeyboardButton(f"🛠 Upgrades", callback_data="home_upgrades")],
                [InlineKeyboardButton(f"🎒 {t(player, 'inventory_menu')}", callback_data="inv_menu")],
                [
                    InlineKeyboardButton(f"💰 {t(player, 'sell')}", callback_data="sell_warehouse"),
                    InlineKeyboardButton(
                        f"{t(player, 'auto_sell')}: {'ON' if bool(mine_data.get('auto_sell', False)) else 'OFF'}",
                        callback_data="toggle_auto_sell",
                    ),
                ],
                [
                    InlineKeyboardButton(f"🚀 {t(player, 'boosts')}", callback_data="boosts_menu"),
                    InlineKeyboardButton(f"⚡ {t(player, 'abilities_menu')}", callback_data="abilities_menu"),
                ],
            ]
        )
    elif screen == "progress":
        keyboard.extend(
            [
                [
                    InlineKeyboardButton(f"🌍 {t(player, 'mines')}", callback_data="locations_menu"),
                    InlineKeyboardButton(f"🧬 {t(player, 'research')}", callback_data="research_menu"),
                ],
                [
                    InlineKeyboardButton(f"⭐ {t(player, 'prestige_btn')}", callback_data="prestige_menu"),
                    InlineKeyboardButton(f"🧠 {t(player, 'advisor')}", callback_data="advisor_menu"),
                ],
                [
                    InlineKeyboardButton(f"🧭 {t(player, 'expeditions')}", callback_data="exp_menu"),
                    InlineKeyboardButton(f"👷 {t(player, 'managers_menu')}", callback_data="mgr_menu"),
                ],
                [
                    InlineKeyboardButton(f"🎟 {t(player, 'pass_menu')}", callback_data="pass_menu"),
                    InlineKeyboardButton(f"📜 {t(player, 'quests_menu')}", callback_data="quests_menu"),
                ],
                [
                    InlineKeyboardButton(f"🏛 {t(player, 'museum')}", callback_data="museum_menu"),
                    InlineKeyboardButton(f"🦴 {t(player, 'collection')}", callback_data="collection_menu"),
                ],
                [
                    InlineKeyboardButton(f"🏢 {t(player, 'company')}", callback_data="company_menu"),
                    InlineKeyboardButton(f"🏆 {t(player, 'leaderboards')}", callback_data="leaderboard_menu"),
                ],
                [
                    InlineKeyboardButton(f"🎡 {t(player, 'free_spin')}", callback_data="spin_menu"),
                    InlineKeyboardButton(f"📊 {t(player, 'stats')}", callback_data="stats_menu"),
                ],
            ]
        )
    else:
        keyboard.extend(
            [
                [
                    InlineKeyboardButton(f"🛒 {t(player, 'shop')}", callback_data="shop_menu"),
                    InlineKeyboardButton(f"🎁 {t(player, 'inventory_menu')}", callback_data="inv_menu"),
                ],
                [
                    InlineKeyboardButton(f"🎁 {t(player, 'daily')}", callback_data="daily_menu"),
                    InlineKeyboardButton(f"🔔 {t(player, 'notifications')}", callback_data="notifications_menu"),
                ],
                [
                    InlineKeyboardButton(f"✅ {t(player, 'subscription_bonus')}", callback_data="subscription_menu"),
                    InlineKeyboardButton(f"🌐 {t(player, 'lang_button')}", callback_data="lang_menu"),
                ],
                [
                    InlineKeyboardButton(
                        f"🖼 {t(player, 'section_images')}: {'ON' if bool(player.get('settings', {}).get('section_images', False)) else 'OFF'}",
                        callback_data="toggle_section_images",
                    )
                ],
                [
                    InlineKeyboardButton(f"ℹ️ {t(player, 'about')}", callback_data="about_menu"),
                    InlineKeyboardButton(f"💳 {t(player, 'donate')}", callback_data="donate_menu"),
                ],
                [
                    InlineKeyboardButton(
                        "📱 Open App",
                        web_app=WebAppInfo(url=WEBAPP_URL) if WEBAPP_URL else None,
                        callback_data=None if WEBAPP_URL else "webapp_missing",
                    )
                ],
                [InlineKeyboardButton(f"🔄 {t(player, 'refresh')}", callback_data=f"menu_{screen}")],
            ]
        )

    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(
                base_text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception:
            pass
    else:
        await update.message.reply_text(
            base_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN,
        )


async def show_home_upgrades_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    player = await get_player(update.effective_user.id)
    mine_id = player["active_mine"]
    mine_data = player["mines"][mine_id]
    cost_s = get_upgrade_cost(player, 100, mine_data["shaft"]["level"])
    cost_e = get_upgrade_cost(player, 100, mine_data["elevator"]["level"])
    cost_w = get_upgrade_cost(player, 100, mine_data["warehouse"]["level"])
    cost_m = get_upgrade_cost(player, 10_000, mine_data.get("level", 1), cost_type="mine_level")
    text = (
        f"🛠 *Upgrades*\n\n"
        f"{t(player, 'shaft')}: L{mine_data['shaft']['level']} ({cost_s})\n"
        f"{t(player, 'elevator')}: L{mine_data['elevator']['level']} ({cost_e})\n"
        f"{t(player, 'warehouse')}: L{mine_data['warehouse']['level']} ({cost_w})\n"
        f"{t(player, 'mine_level')}: L{mine_data.get('level', 1)} ({cost_m})"
    )
    kb = [
        [
            InlineKeyboardButton(f"⛏ {t(player, 'shaft')} ({cost_s})", callback_data="up_shaft"),
            InlineKeyboardButton(f"🛗 {t(player, 'elevator')} ({cost_e})", callback_data="up_elevator"),
        ],
        [InlineKeyboardButton(f"🚛 {t(player, 'warehouse')} ({cost_w})", callback_data="up_warehouse")],
        [InlineKeyboardButton(f"🆙 {t(player, 'mine_level')} ({cost_m})", callback_data="up_mine_level")],
        [InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="menu_home")],
    ]
    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.MARKDOWN,
    )


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user_id = query.from_user.id

    player = await get_player(user_id)
    ensure_player_defaults(player)
    now_ts = time.time()
    last_action_ts = float(context.user_data.get("last_action_ts", player.get("settings", {}).get("last_action_ts", 0)))
    if now_ts - last_action_ts < ACTION_RATE_LIMIT_SECONDS:
        await query.answer(t(player, "too_fast"))
        return
    if not register_action_antispam(player):
        await save_player(user_id, player)
        await query.answer(t(player, "too_many_actions"))
        return
    context.user_data["last_action_ts"] = now_ts
    player["settings"]["last_action_ts"] = now_ts

    mine_id = player["active_mine"]

    if query.data == "work":
        cycle = simulate_pipeline_cycle(player, mine_id)
        sold = cycle.get("sold", 0)
        if sold > 0:
            value = sold * max(1, int(MINES_CONFIG.get(mine_id, {}).get("multiplier", 1)))
            update_quest_progress(player, "earn_coins", value)
        try_roll_diamond_drop(player, 1)
        roll_resource_drops(player, 1)
        player["last_offline_check"] = time.time()
        await save_player(user_id, player)
        await query.answer(
            f"prod {cycle['produced']} | move {cycle['moved']} | deliver {cycle['delivered']}"
            + (f" | sold {cycle['sold']}" if cycle["sold"] > 0 else "")
        )
        await show_main_menu(update, context, screen="home")
        return

    if query.data == "menu_home":
        await show_main_menu(update, context, screen="home")
        return

    if query.data == "menu_progress":
        await show_main_menu(update, context, screen="progress")
        return

    if query.data == "menu_settings":
        await show_main_menu(update, context, screen="settings")
        return

    if query.data == "webapp_missing":
        await query.answer("WEBAPP_URL не задан в .env", show_alert=True)
        return

    if query.data == "home_upgrades":
        await show_home_upgrades_menu(update, context)
        return

    if query.data == "sell_warehouse":
        value = sell_warehouse_ore(player, mine_id)
        player["last_offline_check"] = time.time()
        await save_player(user_id, player)
        await query.answer(f"+{value} 💰" if value > 0 else "Склад пуст")
        await show_main_menu(update, context, screen="home")
        return

    if query.data == "toggle_auto_sell":
        mine = player["mines"][mine_id]
        mine["auto_sell"] = not bool(mine.get("auto_sell", False))
        player["mines"][mine_id] = mine
        await save_player(user_id, player)
        await query.answer(t(player, "updated"))
        await show_main_menu(update, context, screen="home")
        return

    if query.data == "locations_menu":
        await show_locations(update, context)
        return

    if query.data.startswith("switch_"):
        new_mine = query.data.replace("switch_", "")
        if new_mine not in player["unlocked_mines"]:
            await query.answer("Локация еще не открыта")
            return

        player["active_mine"] = new_mine
        player["last_offline_check"] = time.time()
        await save_player(user_id, player)
        await query.answer(f"Переход: {MINES_CONFIG[new_mine]['name']}")
        await show_main_menu(update, context, screen="home")
        return

    if query.data.startswith("buy_mine_"):
        mine_to_buy = query.data.replace("buy_mine_", "")
        cfg = MINES_CONFIG.get(mine_to_buy)
        if not cfg:
            await query.answer("Неизвестная шахта")
            return

        cost = cfg["cost"]
        if mine_to_buy in player["unlocked_mines"]:
            await query.answer("Уже открыто")
            return

        if player["coins"] < cost:
            await query.answer(t(player, "not_enough_coins"))
            return

        player["coins"] -= cost
        player["unlocked_mines"].append(mine_to_buy)
        if mine_to_buy not in player["mines"]:
            player["mines"][mine_to_buy] = build_default_mine()

        player["last_offline_check"] = time.time()
        add_company_xp(player, 40)
        await save_player(user_id, player)
        ensure_user_tick_job(context.application, user_id, get_effective_tick_interval(player))
        await query.answer("Локация открыта")
        await show_locations(update, context)
        return

    if query.data == "research_menu":
        await show_research(update, context)
        return

    if query.data.startswith("buy_res_"):
        r_id = query.data.replace("buy_res_", "")
        cfg = RESEARCH_CONFIG.get(r_id)
        if not cfg:
            await query.answer("Неизвестное исследование")
            return

        lvl = player["research"].get(r_id, 0)
        if lvl >= cfg["max_lvl"]:
            await query.answer("Максимальный уровень")
            return

        if cfg["requires"]:
            req_id, req_lvl = cfg["requires"]
            if player["research"].get(req_id, 0) < req_lvl:
                await query.answer("Не выполнены требования")
                return

        cost = cfg["cost_base"] * (lvl + 1)
        if player["gems"] < cost:
            await query.answer(t(player, "not_enough_gems_plain"))
            return

        player["gems"] -= cost
        player["research"][r_id] = lvl + 1
        update_quest_progress(player, "upgrade_any", 1)
        player["last_offline_check"] = time.time()
        await save_player(user_id, player)

        interval = get_effective_tick_interval(player)
        ensure_user_tick_job(context.application, user_id, interval)

        await query.answer("Исследование улучшено")
        await show_research(update, context)
        return

    if query.data == "stats_menu":
        await show_stats(update, context)
        return

    if query.data.startswith("up_"):
        component = query.data.replace("up_", "")
        mine_data = player["mines"][mine_id]

        if component == "mine_level":
            current_level = mine_data.get("level", 1)
            cost = get_upgrade_cost(player, 10_000, current_level, cost_type="mine_level")
            if player["coins"] < cost:
                await query.answer(t(player, "not_enough_coins"))
                return

            player["coins"] -= cost
            mine_data["level"] = current_level + 1
        else:
            if component not in ("shaft", "elevator", "warehouse"):
                await query.answer("Некорректное улучшение")
                return

            current_level = mine_data[component]["level"]
            cost = get_upgrade_cost(player, 100, current_level)
            if player["coins"] < cost:
                await query.answer(t(player, "not_enough_coins"))
                return

            player["coins"] -= cost
            mine_data[component]["level"] = current_level + 1

        player["stats"]["total_upgrades"] += 1
        update_quest_progress(player, "upgrade_any", 1)
        add_company_xp(player, 12)
        player["last_offline_check"] = time.time()
        await save_player(user_id, player)

        interval = get_effective_tick_interval(player)
        ensure_user_tick_job(context.application, user_id, interval)

        await show_main_menu(update, context, screen="home")
        return

    if query.data == "boosts_menu":
        await show_boosts(update, context)
        return

    if query.data == "shop_menu":
        await show_shop(update, context)
        return

    if query.data == "about_menu":
        await query.edit_message_text(
            f"ℹ️ *{t(player, 'about')}*\n\nTycoon bot prototype.\n{t(player, 'reaction_note')}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="menu_settings")]]),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if query.data == "donate_menu":
        await query.edit_message_text(
            f"💳 *{t(player, 'donate')}*",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="menu_settings")]]),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if query.data == "subscription_menu":
        await show_subscription_bonus_menu(update, context)
        return

    if query.data == "toggle_section_images":
        settings = player.get("settings", {})
        settings["section_images"] = not bool(settings.get("section_images", False))
        player["settings"] = settings
        await save_player(user_id, player)
        await query.answer(t(player, "updated"))
        await show_main_menu(update, context, screen="settings")
        return

    if query.data == "advisor_menu":
        await show_advisor(update, context)
        return

    if query.data == "company_menu":
        await show_company_menu(update, context)
        return

    if query.data == "exp_menu":
        await show_expeditions_menu(update, context)
        return

    if query.data.startswith("exp_start_"):
        exp_id = query.data.replace("exp_start_", "")
        if player.get("expeditions", {}).get("active"):
            await query.answer(t(player, "exp_already_active"))
            return
        cfg = get_expedition_cfg(exp_id)
        if not cfg:
            await query.answer(t(player, "updated"))
            return
        now_ts = time.time()
        player["expeditions"]["active"] = {
            "id": exp_id,
            "started_at": now_ts,
            "ends_at": now_ts + int(cfg["duration_sec"]),
            "reward": dict(cfg.get("reward", {})),
            "done_notified": False,
        }
        update_quest_progress(player, "start_expedition", 1)
        player["last_offline_check"] = now_ts
        await save_player(user_id, player)
        await query.answer(t(player, "exp_started"))
        await show_expeditions_menu(update, context)
        return

    if query.data == "exp_claim":
        active = player.get("expeditions", {}).get("active")
        if not isinstance(active, dict):
            await query.answer(t(player, "updated"))
            return
        if time.time() < float(active.get("ends_at", 0)):
            await query.answer(t(player, "exp_not_ready"))
            return
        reward_lines = apply_expedition_reward(player, active.get("reward", {}))
        player["expeditions"]["history"].append(
            {"id": active.get("id", "unknown"), "completed_at": time.time()}
        )
        player["expeditions"]["active"] = None
        player["last_offline_check"] = time.time()
        await save_player(user_id, player)
        await query.answer(t(player, "exp_claimed"))
        msg = "\n".join(f"• {line}" for line in reward_lines) if reward_lines else t(player, "updated")
        await query.edit_message_text(
            f"🧭 *{t(player, 'exp_claimed')}*\n\n{msg}",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton(f"🧭 {t(player, 'expeditions')}", callback_data="exp_menu")],
                    [InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="main_menu")],
                ]
            ),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if query.data == "inv_menu":
        await show_inventory_menu(update, context)
        return

    if query.data.startswith("inv_open_"):
        chest_id = query.data.replace("inv_open_", "")
        if chest_id not in ("chest_basic", "chest_rare"):
            await query.answer(t(player, "updated"))
            return
        if int(player.get("inventory", {}).get(chest_id, 0)) <= 0:
            await query.answer(t(player, "no_chest"))
            return
        result = open_chest(player, chest_id)
        update_quest_progress(player, "open_chest", 1)
        add_company_xp(player, 10)
        roll_resource_drops(player, 1)
        player["last_offline_check"] = time.time()
        await save_player(user_id, player)
        await query.answer(t(player, "updated"))
        await query.edit_message_text(
            f"🎁 *{t(player, 'inventory_title')}*\n\n{t(player, 'chest_result', result=result)}",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton(f"🎁 {t(player, 'inventory_menu')}", callback_data="inv_menu")],
                    [InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="main_menu")],
                ]
            ),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if query.data == "mgr_menu":
        await show_managers_menu(update, context)
        return

    if query.data.startswith("mgr_assign_"):
        payload = query.data.replace("mgr_assign_", "")
        parts = payload.split("_", 1)
        if len(parts) != 2:
            await query.answer(t(player, "updated"))
            return
        slot, manager_id = parts[0], parts[1]
        if slot not in ("shaft", "elevator", "warehouse"):
            await query.answer(t(player, "updated"))
            return
        if manager_id not in player.get("manager_inventory", {}):
            await query.answer(t(player, "manager_not_owned"))
            return
        player["assigned_managers"][slot] = manager_id
        player["last_offline_check"] = time.time()
        await save_player(user_id, player)
        await query.answer(t(player, "manager_assigned", slot=get_slot_label(player, slot)))
        await show_managers_menu(update, context)
        return

    if query.data.startswith("mgr_level_"):
        manager_id = query.data.replace("mgr_level_", "")
        entry = player.get("manager_inventory", {}).get(manager_id)
        if not entry:
            await query.answer(t(player, "manager_not_owned"))
            return
        lvl = int(entry.get("level", 1))
        required = 10 * lvl
        if int(entry.get("fragments", 0)) < required:
            await query.answer(t(player, "manager_no_frags"))
            return
        entry["fragments"] = int(entry.get("fragments", 0)) - required
        entry["level"] = lvl + 1
        player["manager_inventory"][manager_id] = entry
        player["last_offline_check"] = time.time()
        await save_player(user_id, player)
        await query.answer(t(player, "manager_leveled", level=entry["level"]))
        await show_managers_menu(update, context)
        return

    if query.data.startswith("mgr_auto_"):
        slot = query.data.replace("mgr_auto_", "")
        if slot not in ("shaft", "elevator", "warehouse"):
            await query.answer(t(player, "updated"))
            return
        player["manager_automation"][slot] = not bool(player.get("manager_automation", {}).get(slot, True))
        await save_player(user_id, player)
        await query.answer(t(player, "updated"))
        await show_managers_menu(update, context)
        return

    if query.data == "abilities_menu":
        await show_abilities_menu(update, context)
        return

    if query.data.startswith("ability_use_"):
        slot = query.data.replace("ability_use_", "")
        if slot not in ("shaft", "elevator", "warehouse"):
            await query.answer(t(player, "updated"))
            return
        manager_id = player.get("assigned_managers", {}).get(slot)
        if not manager_id:
            await query.answer(t(player, "ability_no_manager"))
            return
        state = player.get("manager_skills", {}).get(slot, {})
        now_ts = time.time()
        if now_ts < float(state.get("cd_until", 0)):
            await query.answer(t(player, "ability_not_ready"))
            return
        cfg = get_manager_cfg(manager_id)
        active_sec = int(cfg.get("active_duration", 120))
        base_cd = int(cfg.get("cooldown", 900))
        cd = manager_cooldown_after_research(player, base_cd)
        state["active_until"] = now_ts + active_sec
        state["cd_until"] = now_ts + cd
        state["notified"] = False
        player["manager_skills"][slot] = state
        update_quest_progress(player, "activate_ability", 1)
        player["last_offline_check"] = now_ts
        add_company_xp(player, 8)
        await save_player(user_id, player)
        await query.answer(t(player, "ability_activated"))
        await show_abilities_menu(update, context)
        return

    if query.data == "pass_menu":
        await show_pass_menu(update, context)
        return

    if query.data == "museum_menu":
        await show_museum_menu(update, context)
        return

    if query.data == "collection_menu":
        await show_collection_menu(update, context)
        return

    if query.data == "museum_buy":
        museum = player.get("museum", {})
        if bool(museum.get("owned", False)):
            await query.answer(t(player, "updated"))
            await show_museum_menu(update, context)
            return
        if int(player.get("coins", 0)) < MUSEUM_BUY_COST:
            await query.answer(t(player, "not_enough_coins"))
            return
        player["coins"] -= MUSEUM_BUY_COST
        museum["owned"] = True
        museum["last_tick"] = time.time()
        player["museum"] = museum
        add_company_xp(player, 70)
        await save_player(user_id, player)
        await query.answer(t(player, "updated"))
        await show_museum_menu(update, context)
        return

    if query.data == "museum_upgrade":
        museum = player.get("museum", {})
        if not bool(museum.get("owned", False)):
            await query.answer(t(player, "museum_not_owned"))
            return
        lvl = int(museum.get("level", 1))
        cost = int(MUSEUM_UPGRADE_BASE_COST * (1.7 ** (lvl - 1)))
        if int(player.get("coins", 0)) < cost:
            await query.answer(t(player, "not_enough_coins"))
            return
        player["coins"] -= cost
        museum["level"] = lvl + 1
        player["museum"] = museum
        add_company_xp(player, 25)
        await save_player(user_id, player)
        await query.answer(t(player, "updated"))
        await show_museum_menu(update, context)
        return

    if query.data == "museum_claim":
        museum = player.get("museum", {})
        if not bool(museum.get("owned", False)):
            await query.answer(t(player, "museum_not_owned"))
            return
        now_ts = time.time()
        last_claim = float(museum.get("last_claim", 0))
        if last_claim > 0 and now_ts - last_claim < 24 * 60 * 60:
            await query.answer(t(player, "museum_claim_not_ready"))
            return
        update_museum_pool(player)
        amount = int(museum.get("income_pool", 0))
        if amount <= 0:
            await query.answer(t(player, "museum_claim_not_ready"))
            return
        museum["income_pool"] = 0
        museum["last_claim"] = now_ts
        player["museum"] = museum
        player["coins"] += amount
        update_quest_progress(player, "earn_coins", amount)
        await save_player(user_id, player)
        await query.answer(f"+{amount} 💰")
        await show_museum_menu(update, context)
        return

    if query.data.startswith("assemble_"):
        species_id = query.data.replace("assemble_", "")
        if species_id not in FOSSIL_SPECIES:
            await query.answer(t(player, "updated"))
            return
        if species_id in player.get("fossils", {}).get("assembled", []):
            await query.answer(t(player, "updated"))
            return
        parts_store = player["fossils"]["parts"]
        ok = True
        for p in FOSSIL_SPECIES[species_id]["parts"]:
            key = f"{species_id}:{p}"
            if int(parts_store.get(key, 0)) <= 0:
                ok = False
                break
        if not ok:
            await query.answer(t(player, "quest_not_ready"))
            return
        for p in FOSSIL_SPECIES[species_id]["parts"]:
            key = f"{species_id}:{p}"
            parts_store[key] = int(parts_store.get(key, 0)) - 1
        player["fossils"]["parts"] = parts_store
        player["fossils"]["assembled"].append(species_id)
        add_company_xp(player, 60)
        await save_player(user_id, player)
        await query.answer(t(player, "updated"))
        await show_collection_menu(update, context)
        return

    if query.data == "leaderboard_menu":
        await show_leaderboard_menu(update, context)
        return

    if query.data.startswith("leaderboard_"):
        metric = query.data.replace("leaderboard_", "")
        await show_leaderboard(update, context, metric)
        return

    if query.data == "spin_menu":
        await show_spin_menu(update, context)
        return

    if query.data == "spin_now":
        wait = get_wheel_ready_in(player)
        if wait > 0:
            await query.answer(f"{t(player, 'spin_wait')}: {wait // 60}m")
            await show_spin_menu(update, context)
            return
        result = spin_free_wheel(player)
        if not result:
            await query.answer(t(player, "updated"))
        else:
            await query.answer(result, show_alert=True)
        await save_player(user_id, player)
        await show_spin_menu(update, context)
        return

    if query.data == "pass_buy_premium":
        season = player.get("season", {})
        if bool(season.get("premium", False)):
            await query.answer(t(player, "updated"))
            await show_pass_menu(update, context)
            return
        if int(player.get("gems", 0)) < PASS_PREMIUM_GEMS_COST:
            await query.answer(t(player, "not_enough_gems"))
            return
        player["gems"] -= PASS_PREMIUM_GEMS_COST
        season["premium"] = True
        player["season"] = season
        await save_player(user_id, player)
        await query.answer(t(player, "updated"))
        await show_pass_menu(update, context)
        return

    if query.data == "pass_claim":
        claimed, lines = apply_pass_claims(player)
        player["last_offline_check"] = time.time()
        await save_player(user_id, player)
        if claimed <= 0:
            await query.answer(t(player, "pass_nothing"))
        else:
            await query.answer(t(player, "pass_claimed"))
        await show_pass_menu(update, context)
        return

    if query.data == "quests_menu":
        await show_quests_menu(update, context)
        return

    if query.data.startswith("quest_claim_"):
        payload = query.data.replace("quest_claim_", "")
        parts = payload.split("_", 1)
        if len(parts) != 2:
            await query.answer(t(player, "updated"))
            return
        bucket, qid = parts[0], parts[1]
        reset_quests_if_needed(player)
        quest_bucket = player.get("quests", {}).get(bucket, {})
        items = quest_bucket.get("items", {})
        claimed = quest_bucket.get("claimed", [])
        qitem = items.get(qid)
        if not isinstance(qitem, dict):
            await query.answer(t(player, "updated"))
            return
        if not bool(qitem.get("done", False)):
            await query.answer(t(player, "quest_not_ready"))
            return
        if qid in claimed:
            await query.answer(t(player, "updated"))
            return
        lines = apply_generic_reward(player, qitem.get("reward", {}))
        claimed.append(qid)
        quest_bucket["claimed"] = claimed
        player["quests"][bucket] = quest_bucket
        player["last_offline_check"] = time.time()
        await save_player(user_id, player)
        await query.answer(t(player, "quest_claimed"))
        await show_quests_menu(update, context)
        return

    if query.data == "daily_menu":
        await show_daily_menu(update, context)
        return

    if query.data == "daily_claim":
        if not is_daily_ready(player):
            left = get_seconds_until_daily(player)
            await query.answer(
                t(
                    player,
                    "daily_not_ready",
                    hours=left // 3600,
                    minutes=(left % 3600) // 60,
                )
            )
            await show_daily_menu(update, context)
            return

        coins, gems, boost_id = get_daily_reward(player)
        player["coins"] += coins
        player["gems"] += gems
        update_quest_progress(player, "earn_coins", coins)
        if boost_id:
            player["inventory"][boost_id] = int(player["inventory"].get(boost_id, 0)) + 1
        player["last_daily"] = time.time()
        player["last_offline_check"] = time.time()
        await save_player(user_id, player)

        boost_text = f", +1 {BOOSTS[boost_id]['name']}" if boost_id else ""
        await query.answer(
            t(player, "daily_claimed", coins=coins, gems=gems, boost_text=boost_text),
            show_alert=True,
        )
        await show_daily_menu(update, context)
        return

    if query.data == "notifications_menu":
        await show_notifications_menu(update, context)
        return

    if query.data.startswith("notif_toggle_"):
        setting_key = query.data.replace("notif_toggle_", "")
        settings = get_settings(player)
        if setting_key not in {
            "notifications_enabled",
            "notify_manager_ready",
            "notify_event_end",
            "notify_daily_ready",
            "notify_prestige_ready",
            "notify_expedition_done",
            "notify_updates",
        }:
            await query.answer(t(player, "updated"))
            return
        settings[setting_key] = not bool(settings.get(setting_key, False))
        settings["notifications"] = bool(settings.get("notifications_enabled", True))
        player["settings"] = settings
        await save_player(user_id, player)
        await query.answer(t(player, "updated"))
        await show_notifications_menu(update, context)
        return

    if query.data == "notif_test":
        if notifications_enabled(player):
            await safe_send_message(context, user_id, f"🔔 {t(player, 'notif_test_msg')}")
            await query.answer(t(player, "updated"))
        else:
            await query.answer(t(player, "updated"), show_alert=False)
        await show_notifications_menu(update, context)
        return

    if query.data == "lang_menu":
        await show_language_menu(update, context)
        return

    if query.data.startswith("lang_set_"):
        new_lang = normalize_lang_code(query.data.replace("lang_set_", ""))
        player["lang"] = new_lang
        player["lang_prompted"] = True
        await save_player(user_id, player)
        confirm_key = f"lang_changed_{new_lang}"
        await query.answer(t(player, confirm_key))
        await show_main_menu(update, context, refresh_nonce=str(int(time.time() * 1000)))
        return

    if query.data.startswith("lang_auto_yes_"):
        new_lang = normalize_lang_code(query.data.replace("lang_auto_yes_", ""))
        player["lang"] = new_lang
        player["lang_prompted"] = True
        await save_player(user_id, player)
        await query.answer(t(player, f"lang_changed_{new_lang}"))
        await show_main_menu(update, context, refresh_nonce=str(int(time.time() * 1000)))
        return

    if query.data == "lang_auto_no":
        player["lang_prompted"] = True
        await save_player(user_id, player)
        await query.answer(t(player, "updated"))
        await show_main_menu(update, context, refresh_nonce=str(int(time.time() * 1000)))
        return

    if query.data == "shop_buy_auto":
        price = get_automation_upgrade_price(player)
        if player["gems"] < price:
            await query.answer(t(player, "not_enough_gems"))
            return

        player["gems"] -= price
        player["automation_lvl"] = int(player.get("automation_lvl", 0)) + 1
        player["last_offline_check"] = time.time()
        await save_player(user_id, player)
        ensure_user_tick_job(context.application, user_id, get_effective_tick_interval(player))
        await query.answer(f"Auto PRO улучшен до {player['automation_lvl']} уровня")
        await show_shop(update, context)
        return

    if query.data == "shop_buy_auto_manager":
        lvl = int(player.get("auto_manager_lvl", 0))
        if lvl >= AUTO_MANAGER_MAX_LEVEL:
            await query.answer(t(player, "updated"))
            return
        price = get_auto_manager_upgrade_price(player)
        if int(player.get("gems", 0)) < price:
            await query.answer(t(player, "not_enough_gems"))
            return
        player["gems"] -= price
        player["auto_manager_lvl"] = lvl + 1
        player["last_offline_check"] = time.time()
        await save_player(user_id, player)
        ensure_user_auto_manager_job(context.application, user_id)
        await query.answer(t(player, "updated"))
        await show_shop(update, context)
        return

    if query.data == "shop_buy_diamond_luck":
        lvl = int(player.get("drops", {}).get("diamond_chance_lvl", 0))
        if lvl >= DIAMOND_MAX_LEVEL:
            await query.answer(t(player, "updated"))
            return
        price = get_diamond_luck_upgrade_price(player)
        if int(player.get("gems", 0)) < price:
            await query.answer(t(player, "not_enough_gems"))
            return
        player["gems"] -= price
        player["drops"]["diamond_chance_lvl"] = lvl + 1
        player["last_offline_check"] = time.time()
        await save_player(user_id, player)
        await query.answer(t(player, "updated"))
        await show_shop(update, context)
        return

    if query.data.startswith("shop_buy_boost_"):
        bid = query.data.replace("shop_buy_", "")
        price = SHOP_BOOST_PRICES.get(bid)
        if not price:
            await query.answer("Неизвестный товар")
            return
        if player["gems"] < price:
            await query.answer(t(player, "not_enough_gems"))
            return

        player["gems"] -= price
        player["inventory"][bid] = int(player["inventory"].get(bid, 0)) + 1
        player["last_offline_check"] = time.time()
        await save_player(user_id, player)
        await query.answer(f"Куплено: {BOOSTS[bid]['name']}")
        await show_shop(update, context)
        return

    if query.data in ("shop_buy_chest", "shop_buy_chest_basic", "shop_buy_chest_rare"):
        chest_id = "chest_basic" if query.data in ("shop_buy_chest", "shop_buy_chest_basic") else "chest_rare"
        price = int(SHOP_CHEST_PRICES[chest_id])
        if int(player["coins"]) < price:
            await query.answer(t(player, "not_enough_coins"))
            return

        player["coins"] -= price
        player["inventory"][chest_id] = int(player["inventory"].get(chest_id, 0)) + 1
        add_company_xp(player, 15)
        player["last_offline_check"] = time.time()
        await save_player(user_id, player)
        await query.answer(t(player, "updated"))
        await show_shop(update, context)
        return

    if query.data == "creator_bonus_check":
        changed, message = await enforce_channel_subscription_policy(context, user_id, player)
        if message:
            await query.answer(message, show_alert=True)
        if not changed:
            subscribed = await is_user_subscribed_to_creator_channel(context, user_id)
            if subscribed is False:
                await query.edit_message_text(
                    "Подпишитесь на канал создателя и нажмите проверку.\n"
                    f"Награда: +{CREATOR_SUB_REWARD_COINS} 💰 и +{CREATOR_SUB_REWARD_GEMS} 💎",
                    reply_markup=build_creator_subscribe_kb(),
                )
            elif subscribed is None:
                await query.edit_message_text(
                    t(player, "sub_need_admin"),
                    reply_markup=InlineKeyboardMarkup(
                        [
                            [InlineKeyboardButton(f"📢 {t(player, 'join_channel')}", url=CREATOR_CHANNEL_URL)],
                            [InlineKeyboardButton(f"🔄 {t(player, 'sub_check_retry')}", callback_data="creator_bonus_check")],
                            [InlineKeyboardButton(f"🔙 {t(player, 'back')}", callback_data="menu_settings")],
                        ]
                    ),
                )
            else:
                await show_subscription_bonus_menu(update, context)
        else:
            await show_subscription_bonus_menu(update, context)
        return

    if query.data.startswith("use_"):
        bid = query.data.replace("use_", "")
        if bid not in BOOSTS:
            await query.answer("Неизвестный буст")
            return

        qty = player.get("inventory", {}).get(bid, 0)
        if qty <= 0:
            await query.answer("Нет такого буста")
            return

        cfg = BOOSTS[bid]
        player["inventory"][bid] -= 1
        player["active_boost"] = {
            "multiplier": cfg["mult"],
            "expires_at": time.time() + cfg["sec"],
        }
        player["last_offline_check"] = time.time()
        await save_player(user_id, player)

        await query.answer(f"Активирован {cfg['name']}")
        await show_boosts(update, context)
        return

    if query.data == "prestige_menu":
        await show_prestige(update, context)
        return

    if query.data == "prestige_confirm":
        kb = [
            [InlineKeyboardButton("✅ Подтвердить", callback_data="do_prestige")],
            [InlineKeyboardButton("🔙 Назад", callback_data="prestige_menu")],
        ]
        await query.edit_message_text(
            "⚠️ Престиж сбросит все шахты и улучшения. Продолжить?",
            reply_markup=InlineKeyboardMarkup(kb),
        )
        return

    if query.data == "do_prestige":
        if player["coins"] < PRESTIGE_THRESHOLD:
            await query.answer("Недостаточно монет для престижа")
            return

        player["prestige_points"] += 1
        reset_player_for_prestige(player)
        add_season_xp(player, 120)
        player["last_offline_check"] = time.time()
        await save_player(user_id, player)

        ensure_user_tick_job(context.application, user_id, get_effective_tick_interval(player))

        await query.answer("Глобальный престиж выполнен")
        await show_main_menu(update, context, screen="progress")
        return

    if query.data == "leaderboard":
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                """
                SELECT username, json_extract(data, '$.prestige_points') as pts
                FROM players
                ORDER BY pts DESC
                LIMIT 10
                """
            ) as cursor:
                top = await cursor.fetchall()

        text = "🏆 *Топ 10 игроков по престижу:*\n\n"
        for i, (name, pts) in enumerate(top, 1):
            text += f"{i}. {name or 'Игрок'} — {int(pts or 0)} ⭐\n"

        kb = [[InlineKeyboardButton("🔙 Назад", callback_data="main_menu")]]
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if query.data == "main_menu":
        earned, delta, _ = await apply_offline_earnings(player)
        await save_player(user_id, player)

        if earned > 0:
            await query.answer(f"Оффлайн доход: +{earned} 💰 за {delta // 60} мин")
        else:
            await query.answer(t(player, "updated"))

        await show_main_menu(update, context, refresh_nonce=str(int(time.time() * 1000)), screen="home")
        return


async def start_session(update: Update, context: ContextTypes.DEFAULT_TYPE, intro: bool = False) -> None:
    user = update.effective_user
    player = await get_player(user.id, user.first_name)

    changed_sub, sub_msg = await enforce_channel_subscription_policy(context, user.id, player)
    if changed_sub:
        player = await get_player(user.id, user.first_name)

    player["has_started"] = True

    earned, delta, interval = await apply_offline_earnings(player)
    welcome_needed = not bool(player.get("onboarding_done", False))
    if welcome_needed:
        player["onboarding_done"] = True
    await save_player(user.id, player)

    ensure_user_tick_job(context.application, user.id, interval)
    ensure_user_auto_manager_job(context.application, user.id)

    if intro:
        frames = [
            "Запуск шахтерской смены...",
            "Проверка оборудования...",
            "Подключение автоматических линий...",
            "Система готова. Вперёд к добыче.",
        ]
        msg = await update.message.reply_text(frames[0])
        for frame in frames[1:]:
            await asyncio.sleep(0.35)
            await msg.edit_text(frame)

    if earned > 0:
        await update.message.reply_text(f"🕒 Оффлайн доход: +{earned} 💰 (за {delta // 60} мин)")

    if sub_msg and update.message:
        await update.message.reply_text(sub_msg)

    if welcome_needed and update.message:
        await update.message.reply_text(
            f"👋 *{t(player, 'welcome_title')}*\n\n{t(player, 'welcome_lines')}",
            parse_mode=ParseMode.MARKDOWN,
        )

    await show_main_menu(update, context, screen="home")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    is_tokenminer_start = bool(context.args) and context.args[0].strip().lower() == "tokenminer"
    if not is_tokenminer_start:
        user = update.effective_user
        player = await get_player(user.id, user.first_name)
        changed_sub, sub_msg = await enforce_channel_subscription_policy(context, user.id, player)
        if changed_sub:
            player = await get_player(user.id, user.first_name)
        detected_lang = detect_user_lang(update)

        need_prompt = (
            not player.get("lang_prompted", False)
            and get_player_lang(player) == "en"
            and detected_lang in {"ru", "uz"}
        )
        if need_prompt and update.message:
            player["has_started"] = True
            _, _, interval = await apply_offline_earnings(player)
            await save_player(user.id, player)
            ensure_user_tick_job(context.application, user.id, interval)
            ensure_user_auto_manager_job(context.application, user.id)

            lang_name = lang_name_for_ui(detected_lang, get_player_lang(player))
            await update.message.reply_text(
                t(player, "lang_auto_prompt", lang_name=lang_name),
                reply_markup=build_lang_auto_kb(player, detected_lang),
                parse_mode=ParseMode.MARKDOWN,
            )
            if sub_msg:
                await update.message.reply_text(sub_msg)
            return

    await start_session(update, context, intro=is_tokenminer_start)


async def startminer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await start_session(update, context, intro=True)


async def tokenminer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await start_session(update, context, intro=True)


async def setname(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    player = await get_player(user.id, user.first_name)
    if not context.args:
        await update.message.reply_text(t(player, "setname_usage"))
        return
    new_name = " ".join(context.args).strip()
    if not new_name:
        await update.message.reply_text(t(player, "setname_usage"))
        return
    player["company"]["name"] = new_name[:32]
    await save_player(user.id, player)
    await update.message.reply_text(t(player, "setname_done"))


async def announce(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return
    admin_ids = get_admin_ids()
    admin_player = await get_player(user.id, user.first_name or "Admin")

    if user.id not in admin_ids:
        await update.message.reply_text(t(admin_player, "announce_denied"))
        return

    text = " ".join(context.args).strip() if context.args else ""
    if not text:
        await update.message.reply_text(t(admin_player, "announce_usage"))
        return

    ok = 0
    failed = 0
    now_ts = time.time()

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id, data FROM players") as cursor:
            rows = await cursor.fetchall()

    for target_user_id, data in rows:
        try:
            player = json.loads(data)
        except Exception:
            failed += 1
            continue

        changed = ensure_player_defaults(player)
        settings = get_settings(player)
        if not bool(settings.get("notify_updates", True)):
            if changed:
                await save_player(target_user_id, player)
            continue
        if not notifications_enabled(player):
            if changed:
                await save_player(target_user_id, player)
            continue
        if not can_send_unsolicited(player, now_ts):
            if changed:
                await save_player(target_user_id, player)
            continue

        sent = await safe_send_message(
            context,
            target_user_id,
            f"📢 {text}",
        )
        if sent:
            mark_unsolicited_sent(player, now_ts)
            set_last_notified(player, "update", now_ts)
            ok += 1
            changed = True
        else:
            failed += 1

        if changed:
            await save_player(target_user_id, player)

        await asyncio.sleep(0.03)

    await update.message.reply_text(t(admin_player, "announce_done", ok=ok, failed=failed))


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error
    if isinstance(err, BadRequest) and "Message is not modified" in str(err):
        return
    logger.exception("Unhandled error while processing update: %s", err)


# =========================
# App bootstrap
# =========================


async def setup_bot_commands(application: Application) -> None:
    commands = [
        BotCommand("start", "Запуск бота (введите: /start tokenminer)"),
        BotCommand("startminer", "Расширенный старт"),
        BotCommand("tokenminer", "Быстрый расширенный старт"),
        BotCommand("setname", "Изменить название компании"),
        BotCommand("announce", "Админ: рассылка обновления"),
    ]
    try:
        await application.bot.set_my_commands(commands)
    except Exception as exc:
        logger.warning("Failed to set bot commands: %s", exc)


async def main_async() -> None:
    await init_db()

    token = resolve_bot_token()

    application = Application.builder().token(token).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("startminer", startminer))
    application.add_handler(CommandHandler("tokenminer", tokenminer))
    application.add_handler(CommandHandler("setname", setname))
    application.add_handler(CommandHandler("announce", announce))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_error_handler(on_error)

    await application.initialize()
    await application.start()
    await setup_bot_commands(application)
    if application.job_queue is not None:
        application.job_queue.run_repeating(
            notification_sweep_job,
            interval=10 * 60,
            first=30,
            name="notification_sweep",
            data={"offset": 0},
        )
    else:
        logger.warning(
            "JobQueue is not available. Install extra: pip install \"python-telegram-bot[job-queue]\""
        )
    await application.updater.start_polling()

    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":
    asyncio.run(main_async())







