"""
Bull Put Spread Bot — GUI Launcher
"""
# ── Self-update Bootstrap (nur im frozen .app) ────────────────────────────────
# Prüft ob eine heruntergeladene launcher.py in Resources liegt und führt
# diese aus statt der eingebackenen Version. Ermöglicht UI-Updates via GitHub.
import os as _os, sys as _sys
if getattr(_sys, 'frozen', False) and '--bootstrap' not in _sys.argv:
    _e = _os.path.dirname(_sys.executable)
    _r = _os.path.join(_os.path.dirname(_e), "Resources")
    _b = _r if _os.path.isdir(_r) else _e
    _custom = _os.path.join(_b, "launcher.py")
    if _os.path.exists(_custom):
        _sys.argv.append('--bootstrap')
        with open(_custom, encoding='utf-8') as _lf:
            exec(compile(_lf.read(), _custom, 'exec'),
                 {'__file__': _custom, '__name__': '__main__', '__spec__': None})
        raise SystemExit(0)
# ─────────────────────────────────────────────────────────────────────────────

import customtkinter as ctk
import threading
import asyncio
import queue as queue_module
import json
import subprocess
import sys
import os
import platform
import shutil
import urllib.request
from datetime import datetime

if getattr(sys, 'frozen', False):
    _exec_dir   = os.path.dirname(sys.executable)
    _resources  = os.path.join(os.path.dirname(_exec_dir), "Resources")
    _BASE = _resources if os.path.isdir(_resources) else _exec_dir
else:
    _BASE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH  = os.path.join(_BASE, "config.json")
BOT_PATH     = os.path.join(_BASE, "bot.py")
VERSION_FILE = os.path.join(_BASE, "version.txt")

# ── Version & Update-URL ─────────────────────────────────────────────────────
# Ersetze DEIN_USERNAME und DEIN_REPO mit deinen GitHub-Daten.
# Das Repo muss öffentlich sein ODER du verwendest einen Personal Access Token.
VERSION         = open(VERSION_FILE).read().strip() if os.path.exists(VERSION_FILE) else "1.0.0"
UPDATE_BASE_URL = "https://raw.githubusercontent.com/xTheRichiNOT/bull-put-spread-bot-releases/main"

# Alle Dateien die beim Auto-Update heruntergeladen werden (inkl. launcher.py)
UPDATE_FILES = ["bot.py", "launcher.py", "version.txt", "requirements.txt"]
# ────────────────────────────────────────────────────────────────────────────

DEFAULT_CONFIG = {
    "ib_host":             "127.0.0.1",
    "ib_port":             7497,
    "ib_account":          "",
    "min_vola":            0.28,
    "abstand_y":           0.10,
    "min_credit":          70,
    "min_risk_reward":     0.20,
    "max_delta":           0.28,
    "max_positions":       8,
    "max_per_sector":      2,
    "scan_intervall":      60,
    "auto_trade":          True,
    "take_profit_pct":     0.50,
    "stop_loss_mult":      2.0,
    "dte_exit":            21,
    "min_available_funds": 2000,
}

def load_config() -> dict:
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH) as f:
                return {**DEFAULT_CONFIG, **json.load(f)}
        except Exception:
            pass
    return dict(DEFAULT_CONFIG)

def save_config(cfg: dict):
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

# ── Design-System ─────────────────────────────────────────────────────────────
C = {
    "bg":        "#080d1a",   # tiefstes Hintergrund
    "surface":   "#0d1526",   # Panel/Card
    "surface2":  "#111e33",   # erhöhte Fläche
    "border":    "#1a2f50",   # subtile Rahmen
    "accent":    "#00c896",   # Cyan-Grün Akzent
    "accent2":   "#0ea5e9",   # Blau für Info
    "green":     "#22c55e",   # Aktiv / Profit
    "green2":    "#4ade80",   # Puls-Hell
    "red":       "#ef4444",   # Gestoppt / Verlust
    "amber":     "#f59e0b",   # Warnung
    "text":      "#e2e8f0",   # Primärtext
    "muted":     "#4a6080",   # Sekundärtext
    "header":    "#060b17",   # Header-Hintergrund
}

IB_SETUP_TEXT = """
╔══════════════════════════════════════════════════════════════════════════╗
║         Interactive Brokers Setup — Schritt für Schritt                 ║
╚══════════════════════════════════════════════════════════════════════════╝

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 SCHRITT 1 — TWS herunterladen & installieren
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  1. Öffne: www.interactivebrokers.com
  2. Navigiere zu: Handelsplattformen → Trader Workstation (TWS)
  3. Lade TWS herunter (empfohlen: "Latest" Version)
  4. Installiere TWS wie eine normale Anwendung

  ℹ  Alternativ: IB Gateway (leichter, läuft dauerhaft im Hintergrund)


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 SCHRITT 2 — Konto erstellen (Paper Trading = kostenlos)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Paper Trading:  Kostenloses Demokonto mit $1.000.000 Testkapital
                  → Kein echtes Geld, ideal zum Testen des Bots
                  → Konto-Nummer beginnt mit "DU"  (z.B. DU1234567)

  Live Trading:   Echtes Brokerkonto
                  → Konto-Nummer beginnt mit "U"   (z.B. U1234567)

  So erstellst du ein Paper Trading Konto:
  1. Einloggen auf interactivebrokers.com
  2. Konto → Paper Trading Konto → Paper Trading Konto beantragen


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 SCHRITT 3 — TWS starten & einloggen
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  1. Starte TWS
  2. Wähle beim Login "Paper Trading" (für Tests)
     ODER  "Live Trading"  (für echten Handel)
  3. Gib deine IB-Zugangsdaten ein


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 SCHRITT 4 — API aktivieren   ← WICHTIGSTER SCHRITT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  In TWS:   Edit → Global Configuration → API → Settings

  Folgende Einstellungen setzen:

  ✅  "Enable ActiveX and Socket Clients"       AKTIVIEREN
  ✅  "Allow connections from localhost only"    AKTIVIEREN
  ❌  "Read-Only API"                            DEAKTIVIEREN
       (sonst kann der Bot KEINE Orders platzieren!)

  Socket Port:
  ┌───────────────────────────────────────────────────────────┐
  │  TWS Paper Trading          →  Port 7497  (Standard)      │
  │  TWS Live Trading           →  Port 7496                  │
  │  IB Gateway Paper           →  Port 4002                  │
  │  IB Gateway Live            →  Port 4001                  │
  └───────────────────────────────────────────────────────────┘

  → Klicke "OK" dann "Apply"
  → TWS muss NICHT neugestartet werden


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 SCHRITT 5 — Account-Nummer herausfinden
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  In TWS:
  • Oben rechts neben deinem Namen steht deine Account-Nummer
  • ODER:  Konto → Kontoauszug → Account-Nummer oben links

  Diese Nummer trägst du in den Einstellungen unter
  "Account-Nummer" ein  (z.B. DU1234567 oder U1234567)


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 SCHRITT 6 — Options-Handel freischalten
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Der Bot handelt Bull Put Spreads (Options-Strategie).
  Options müssen in deinem IB-Konto freigeschaltet sein.

  In TWS:  Konto → Konto-Management → Handelsberechtigungen
           → Options → Stufe 2 (oder höher) beantragen

  ℹ  Paper Trading Konten haben Options standardmäßig freigeschaltet


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 SCHRITT 7 — Bot starten
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Checkliste vor dem ersten Start:

  ☐  TWS läuft und du bist eingeloggt
  ☐  API ist aktiviert  (Schritt 4)
  ☐  Account-Nummer in Einstellungen eingetragen
  ☐  Richtiger Port in Einstellungen  (7497 für Paper)
  ☐  "Auto-Trade" in Einstellungen nach Wunsch gesetzt
     (Aus = nur Signale anzeigen,  An = automatisch Orders platzieren)

  → Tab "Dashboard" → "Bot starten" klicken


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 HÄUFIGE FEHLER & LÖSUNGEN
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ❌  "Connection refused" / "Timeout auf Port 7497"
      → TWS ist nicht geöffnet oder nicht eingeloggt
      → API nicht aktiviert  (Schritt 4 nochmal prüfen)
      → Falscher Port  (Paper=7497, Live=7496)

  ❌  "Error 201: Order rejected"
      → "Read-Only API" ist noch aktiviert → deaktivieren!
      → Options nicht freigeschaltet  (Schritt 6)

  ❌  Bot läuft aber platziert keine Orders
      → Markt ist geschlossen  (NYSE: Mo–Fr 15:30–22:00 MEZ)
      → AUTO_TRADE ist auf "Aus" gestellt
      → IV zu niedrig — kein Signal über den Filter
      → Portfolio voll  (Max. Positionen erreicht)

  ❌  Bot startet nicht (Python-Fehler)
      → Abhängigkeiten nicht installiert
      → install_mac.sh bzw. install_windows.bat nochmal ausführen
""".strip()


class SetupWizard(ctk.CTkToplevel):
    """Erststart-Wizard: führt den Kunden durch Platform / Software / Modus / Account."""

    # Port-Matrix: (software, trading_mode) → port
    _PORTS = {
        ("TWS",     "Paper"): 7497,
        ("TWS",     "Live"):  7496,
        ("Gateway", "Paper"): 4002,
        ("Gateway", "Live"):  4001,
    }

    def __init__(self, parent: ctk.CTk, cfg: dict, on_done):
        super().__init__(parent)
        self.title("Ersteinrichtung")
        self.geometry("560x480")
        self.resizable(False, False)
        self.grab_set()           # Modal — Hauptfenster blockiert
        self.lift()
        self.focus_force()

        self._cfg    = cfg
        self._done   = on_done
        self._step   = 0

        # Wizard-State
        self._platform  = ctk.StringVar(value="Desktop")
        self._software  = ctk.StringVar(value="TWS")
        self._mode      = ctk.StringVar(value="Paper")
        self._account   = ctk.StringVar(value=cfg.get("ib_account", ""))
        self._host      = ctk.StringVar(value=cfg.get("ib_host", "127.0.0.1"))

        self._build()

    def _build(self):
        # Header
        hdr = ctk.CTkFrame(self, height=54, corner_radius=0,
                           fg_color=("#111827", "#111827"))
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        ctk.CTkLabel(hdr, text="  Ersteinrichtung",
                     font=ctk.CTkFont(size=16, weight="bold"),
                     text_color="#38bdf8").pack(side="left", padx=8, pady=10)
        self._step_lbl = ctk.CTkLabel(hdr, text="Schritt 1 / 4",
                                      font=ctk.CTkFont(size=11),
                                      text_color="#64748b")
        self._step_lbl.pack(side="right", padx=16)

        # Content area
        self._content = ctk.CTkFrame(self, fg_color="transparent")
        self._content.pack(fill="both", expand=True, padx=28, pady=10)

        # Navigation
        nav = ctk.CTkFrame(self, fg_color="transparent")
        nav.pack(fill="x", padx=28, pady=(0, 20))
        self._back_btn = ctk.CTkButton(nav, text="← Zurück", width=110, height=36,
                                       fg_color=("#374151", "#374151"),
                                       hover_color=("#4b5563", "#4b5563"),
                                       command=self._back)
        self._back_btn.pack(side="left")
        self._next_btn = ctk.CTkButton(nav, text="Weiter →", width=130, height=36,
                                       fg_color="#0369a1", hover_color="#075985",
                                       font=ctk.CTkFont(weight="bold"),
                                       command=self._next)
        self._next_btn.pack(side="right")

        self._show_step()

    def _clear_content(self):
        for w in self._content.winfo_children():
            w.destroy()

    def _show_step(self):
        self._clear_content()
        self._step_lbl.configure(text=f"Schritt {self._step + 1} / 4")
        self._back_btn.configure(state="normal" if self._step > 0 else "disabled")
        self._next_btn.configure(text="Fertig  ✓" if self._step == 3 else "Weiter →")

        steps = [self._step_platform, self._step_software,
                 self._step_mode,     self._step_account]
        steps[self._step]()

    # ── Step 1: Platform ──────────────────────────────────────────────────────
    def _step_platform(self):
        ctk.CTkLabel(self._content,
                     text="Wo läuft der Bot?",
                     font=ctk.CTkFont(size=15, weight="bold")).pack(anchor="w", pady=(8, 4))
        ctk.CTkLabel(self._content,
                     text="Wähle aus wo IB Gateway / TWS installiert ist.",
                     text_color="#94a3b8", font=ctk.CTkFont(size=12)).pack(anchor="w", pady=(0, 16))

        for val, title, desc in [
            ("Desktop",
             "Gleicher Computer  (Mac / Windows)",
             "Bot und TWS / IB Gateway laufen auf demselben Rechner.\nHost bleibt 127.0.0.1"),
            ("Server",
             "Externer Server  (Linux VPS / Strato)",
             "IB Gateway läuft auf einem anderen Rechner im Netzwerk.\nDu gibst die IP-Adresse des Servers ein."),
        ]:
            f = ctk.CTkFrame(self._content,
                             fg_color=("#1e293b", "#1e293b"),
                             corner_radius=8)
            f.pack(fill="x", pady=4)
            rb = ctk.CTkRadioButton(f, text=title, variable=self._platform, value=val,
                                    font=ctk.CTkFont(size=13, weight="bold"),
                                    command=self._show_step)
            rb.pack(anchor="w", padx=14, pady=(10, 2))
            ctk.CTkLabel(f, text=desc, text_color="#64748b",
                         font=ctk.CTkFont(size=11), justify="left").pack(anchor="w", padx=32, pady=(0, 10))

        if self._platform.get() == "Server":
            ctk.CTkLabel(self._content, text="IP-Adresse des Servers:",
                         font=ctk.CTkFont(size=12)).pack(anchor="w", pady=(12, 2))
            ctk.CTkEntry(self._content, textvariable=self._host,
                         width=200, placeholder_text="z.B. 192.168.1.10").pack(anchor="w")

    # ── Step 2: Software ──────────────────────────────────────────────────────
    def _step_software(self):
        ctk.CTkLabel(self._content,
                     text="Welche IB-Software verwendest du?",
                     font=ctk.CTkFont(size=15, weight="bold")).pack(anchor="w", pady=(8, 4))
        ctk.CTkLabel(self._content,
                     text="Beide funktionieren mit dem Bot. IB Gateway empfohlen für Hintergrundbetrieb.",
                     text_color="#94a3b8", font=ctk.CTkFont(size=12)).pack(anchor="w", pady=(0, 16))

        for val, title, desc in [
            ("TWS",
             "Trader Workstation  (TWS)",
             "Die vollständige Handelsplattform mit grafischer Oberfläche.\n"
             "Muss offen und eingeloggt bleiben."),
            ("Gateway",
             "IB Gateway  (empfohlen)",
             "Leichtgewichtige Version ohne große Oberfläche.\n"
             "Ideal für Hintergrundbetrieb — weniger RAM, kein großes Fenster."),
        ]:
            f = ctk.CTkFrame(self._content,
                             fg_color=("#1e293b", "#1e293b"),
                             corner_radius=8)
            f.pack(fill="x", pady=4)
            rb = ctk.CTkRadioButton(f, text=title, variable=self._software, value=val,
                                    font=ctk.CTkFont(size=13, weight="bold"))
            rb.pack(anchor="w", padx=14, pady=(10, 2))
            ctk.CTkLabel(f, text=desc, text_color="#64748b",
                         font=ctk.CTkFont(size=11), justify="left").pack(anchor="w", padx=32, pady=(0, 10))

    # ── Step 3: Trading Mode ──────────────────────────────────────────────────
    def _step_mode(self):
        sw  = self._software.get()
        ctk.CTkLabel(self._content,
                     text="Paper Trading oder Live Trading?",
                     font=ctk.CTkFont(size=15, weight="bold")).pack(anchor="w", pady=(8, 4))
        ctk.CTkLabel(self._content,
                     text="Paper Trading empfohlen zum Testen — kein echtes Geld.",
                     text_color="#94a3b8", font=ctk.CTkFont(size=12)).pack(anchor="w", pady=(0, 16))

        for val, title, desc, color in [
            ("Paper",
             "Paper Trading  (Demokonto)",
             "Kostenloses Testkonto mit $1.000.000 Spielgeld.\n"
             f"Port: {self._PORTS[(sw, 'Paper')]}",
             "#166534"),
            ("Live",
             "Live Trading  (echtes Geld)",
             "Echte Orders mit echtem Kapital.\n"
             f"Port: {self._PORTS[(sw, 'Live')]}  ⚠️  Nur wenn du weißt was du tust!",
             "#7f1d1d"),
        ]:
            f = ctk.CTkFrame(self._content,
                             fg_color=("#1e293b", "#1e293b"),
                             corner_radius=8)
            f.pack(fill="x", pady=4)
            rb = ctk.CTkRadioButton(f, text=title, variable=self._mode, value=val,
                                    font=ctk.CTkFont(size=13, weight="bold"),
                                    fg_color=color)
            rb.pack(anchor="w", padx=14, pady=(10, 2))
            ctk.CTkLabel(f, text=desc, text_color="#64748b",
                         font=ctk.CTkFont(size=11), justify="left").pack(anchor="w", padx=32, pady=(0, 10))

    # ── Step 4: Account ───────────────────────────────────────────────────────
    def _step_account(self):
        sw, mode = self._software.get(), self._mode.get()
        port = self._PORTS[(sw, mode)]
        prefix = "DU" if mode == "Paper" else "U"

        ctk.CTkLabel(self._content,
                     text="Deine IB Account-Nummer",
                     font=ctk.CTkFont(size=15, weight="bold")).pack(anchor="w", pady=(8, 4))

        # Summary box
        summary = (f"  Software:  {sw}   |   Modus: {mode} Trading   |   Port: {port}\n"
                   f"  Host:  {self._host.get()}")
        sf = ctk.CTkFrame(self._content, fg_color=("#0f172a", "#0f172a"), corner_radius=6)
        sf.pack(fill="x", pady=(0, 16))
        ctk.CTkLabel(sf, text=summary, font=ctk.CTkFont(family="Courier", size=11),
                     text_color="#38bdf8", justify="left").pack(padx=12, pady=8)

        ctk.CTkLabel(self._content,
                     text=f"Account-Nummer  ({prefix}xxxxxxx):",
                     font=ctk.CTkFont(size=12)).pack(anchor="w", pady=(4, 2))
        e = ctk.CTkEntry(self._content, textvariable=self._account,
                         width=200, font=ctk.CTkFont(size=13),
                         placeholder_text=f"{prefix}1234567")
        e.pack(anchor="w")
        e.focus()

        ctk.CTkLabel(self._content,
                     text=f"In TWS/{sw} findest du sie oben rechts neben deinem Namen.",
                     text_color="#64748b", font=ctk.CTkFont(size=11)).pack(anchor="w", pady=(6, 0))

        self._err_lbl = ctk.CTkLabel(self._content, text="",
                                     text_color="#f87171", font=ctk.CTkFont(size=12))
        self._err_lbl.pack(anchor="w", pady=(4, 0))

    # ── Navigation ────────────────────────────────────────────────────────────
    def _back(self):
        if self._step > 0:
            self._step -= 1
            self._show_step()

    def _next(self):
        if self._step == 3:
            self._finish()
        else:
            self._step += 1
            self._show_step()

    def _finish(self):
        acct = self._account.get().strip()
        if not acct:
            self._err_lbl.configure(text="⚠  Bitte Account-Nummer eingeben.")
            return

        sw, mode = self._software.get(), self._mode.get()
        self._cfg["ib_account"] = acct
        self._cfg["ib_host"]    = self._host.get().strip() or "127.0.0.1"
        self._cfg["ib_port"]    = self._PORTS[(sw, mode)]
        save_config(self._cfg)

        self._done()
        self.destroy()


class BotLauncher(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title(f"Bull Put Spread Bot  v{VERSION}")
        self.geometry("960x740")
        self.minsize(820, 620)

        self.cfg           = load_config()
        self._stop_event   = None
        self._bot_thread   = None
        self._queue        = queue_module.Queue()
        self._running      = False
        self._start_time   = None   # für Uptime-Counter
        self._pulse_state  = False

        self.configure(fg_color=C["bg"])
        self._set_icon()
        self._build_ui()
        self._poll_queue()

        # Erststart-Wizard wenn noch keine Account-Nummer gesetzt
        if not self.cfg.get("ib_account", "").strip():
            self.after(200, self._show_wizard)
        else:
            # Check for updates silently in background
            threading.Thread(target=self._check_for_updates, daemon=True).start()

    def _set_icon(self):
        try:
            from PIL import Image, ImageTk
            path = os.path.join(_BASE, "icons", "icon.png")
            img  = Image.open(path).resize((256, 256), Image.LANCZOS)
            photo = ImageTk.PhotoImage(img)
            self.wm_iconphoto(True, photo)
            self._icon_ref = photo          # prevent GC
            if sys.platform == "darwin":
                try:
                    import AppKit
                    ns_img = AppKit.NSImage.alloc().initWithContentsOfFile_(path)
                    AppKit.NSApp.setApplicationIconImage_(ns_img)
                except Exception:
                    pass
        except Exception:
            pass

    def _show_wizard(self):
        SetupWizard(self, self.cfg, on_done=self._wizard_done)

    def _wizard_done(self):
        """Wird aufgerufen wenn Wizard abgeschlossen — Config neu laden + Update prüfen."""
        self.cfg = load_config()
        # Settings-Felder aktualisieren
        for key, widget in getattr(self, "_fields", {}).items():
            if isinstance(widget, ctk.BooleanVar):
                widget.set(bool(self.cfg.get(key, True)))
            elif not isinstance(widget, ctk.CTkOptionMenu):
                widget.delete(0, "end")
                widget.insert(0, str(self.cfg.get(key, "")))
        threading.Thread(target=self._check_for_updates, daemon=True).start()

    # ── UI ───────────────────────────────────────────────────────────────────

    def _build_ui(self):
        # ── Accent-Linie ganz oben ────────────────────────────────────────────
        accent_line = ctk.CTkFrame(self, height=2, corner_radius=0,
                                   fg_color=C["accent"])
        accent_line.pack(fill="x")

        # ── Header ───────────────────────────────────────────────────────────
        self._hdr = ctk.CTkFrame(self, height=62, corner_radius=0,
                                 fg_color=C["header"])
        self._hdr.pack(fill="x")
        self._hdr.pack_propagate(False)

        # Logo-Bereich
        logo_frame = ctk.CTkFrame(self._hdr, fg_color="transparent")
        logo_frame.pack(side="left", padx=(16, 0))

        ctk.CTkLabel(logo_frame, text="⬡",
                     font=ctk.CTkFont(size=22),
                     text_color=C["accent"]).pack(side="left", padx=(0, 8))

        ctk.CTkLabel(logo_frame, text="BULL PUT SPREAD",
                     font=ctk.CTkFont(size=15, weight="bold"),
                     text_color=C["text"]).pack(side="left")

        ctk.CTkLabel(logo_frame, text="BOT",
                     font=ctk.CTkFont(size=15, weight="bold"),
                     text_color=C["accent"]).pack(side="left", padx=(4, 0))

        # Version badge
        ver_badge = ctk.CTkFrame(logo_frame, fg_color=C["surface2"],
                                 corner_radius=4)
        ver_badge.pack(side="left", padx=(12, 0))
        ctk.CTkLabel(ver_badge, text=f" v{VERSION} ",
                     font=ctk.CTkFont(size=10),
                     text_color=C["muted"]).pack(pady=2)

        # Rechte Seite: Uptime + Status
        right = ctk.CTkFrame(self._hdr, fg_color="transparent")
        right.pack(side="right", padx=16)

        self._uptime_lbl = ctk.CTkLabel(right, text="",
                                         font=ctk.CTkFont(family="Courier", size=11),
                                         text_color=C["muted"])
        self._uptime_lbl.pack(side="left", padx=(0, 16))

        self._status_dot = ctk.CTkLabel(right, text="⏹  GESTOPPT",
                                        font=ctk.CTkFont(size=12, weight="bold"),
                                        text_color=C["red"])
        self._status_dot.pack(side="left")

        # ── Update banner (hidden until update found) ─────────────────────
        self._update_bar = ctk.CTkFrame(self, height=0, corner_radius=0,
                                        fg_color=("#0c2340", "#0c2340"))
        self._update_bar.pack(fill="x")
        self._update_bar.pack_propagate(False)

        # ── Tabs ─────────────────────────────────────────────────────────────
        tabs = ctk.CTkTabview(self, anchor="nw",
                              fg_color=C["surface"],
                              segmented_button_fg_color=C["header"],
                              segmented_button_selected_color=C["surface2"],
                              segmented_button_selected_hover_color=C["surface2"],
                              segmented_button_unselected_color=C["header"],
                              segmented_button_unselected_hover_color=C["surface"])
        tabs.pack(fill="both", expand=True, padx=10, pady=(6, 10))
        tabs.add("  Dashboard  ")
        tabs.add("  Historie  ")
        tabs.add("  Einstellungen  ")
        tabs.add("  IB-Setup Guide  ")

        self._build_dashboard(tabs.tab("  Dashboard  "))
        self._build_history(tabs.tab("  Historie  "))
        self._build_settings(tabs.tab("  Einstellungen  "))
        self._build_guide(tabs.tab("  IB-Setup Guide  "))

    # ── Dashboard tab ────────────────────────────────────────────────────────

    def _build_dashboard(self, parent):
        parent.configure(fg_color=C["surface"])

        # ── Steuerleiste ──────────────────────────────────────────────────────
        ctrl = ctk.CTkFrame(parent, fg_color=C["surface2"], corner_radius=10)
        ctrl.pack(fill="x", padx=10, pady=(10, 6))

        btn_area = ctk.CTkFrame(ctrl, fg_color="transparent")
        btn_area.pack(side="left", padx=12, pady=10)

        self._start_btn = ctk.CTkButton(
            btn_area, text="▶  STARTEN", width=160, height=44,
            font=ctk.CTkFont(size=13, weight="bold"),
            fg_color=C["accent"], hover_color="#009e78",
            text_color="#000000",
            corner_radius=8,
            command=self._start_bot)
        self._start_btn.pack(side="left", padx=(0, 8))

        self._stop_btn = ctk.CTkButton(
            btn_area, text="⏹  STOPPEN", width=140, height=44,
            font=ctk.CTkFont(size=13, weight="bold"),
            fg_color=C["surface"], hover_color=C["border"],
            text_color=C["red"], border_width=1, border_color=C["red"],
            corner_radius=8,
            state="disabled", command=self._stop_bot)
        self._stop_btn.pack(side="left", padx=(0, 8))

        ctk.CTkButton(
            btn_area, text="⌫  Log", width=80, height=44,
            font=ctk.CTkFont(size=12),
            fg_color=C["surface"], hover_color=C["border"],
            text_color=C["muted"], corner_radius=8,
            command=self._clear_log).pack(side="left")

        # ── Log-Bereich ───────────────────────────────────────────────────────
        log_header = ctk.CTkFrame(parent, fg_color="transparent")
        log_header.pack(fill="x", padx=12, pady=(4, 2))
        ctk.CTkLabel(log_header, text="▸ LIVE LOG",
                     font=ctk.CTkFont(size=10, weight="bold"),
                     text_color=C["muted"]).pack(side="left")
        self._log_count_lbl = ctk.CTkLabel(log_header, text="",
                     font=ctk.CTkFont(family="Courier", size=10),
                     text_color=C["muted"])
        self._log_count_lbl.pack(side="right")

        self._log = ctk.CTkTextbox(
            parent,
            font=ctk.CTkFont(family="Courier", size=12),
            state="disabled", wrap="word",
            fg_color=C["bg"],
            text_color="#7ca4c0",
            corner_radius=8,
            border_width=1,
            border_color=C["border"])
        self._log.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        # Farb-Tags für den Log (via tkinter-Unterlage)
        tb = self._log._textbox
        tb.tag_configure("green",  foreground="#22c55e")
        tb.tag_configure("red",    foreground="#ef4444")
        tb.tag_configure("amber",  foreground="#f59e0b")
        tb.tag_configure("cyan",   foreground="#00c896")
        tb.tag_configure("blue",   foreground="#60a5fa")
        tb.tag_configure("dim",    foreground="#2d4a6b")
        tb.tag_configure("white",  foreground="#e2e8f0")
        self._log_lines = 0

    # ── History tab ──────────────────────────────────────────────────────────

    def _build_history(self, parent):
        parent.configure(fg_color=C["surface"])

        # Header-Zeile
        hdr = ctk.CTkFrame(parent, fg_color=C["surface2"], corner_radius=8)
        hdr.pack(fill="x", padx=10, pady=(10, 4))

        for col, w in [("Datum", 130), ("Symbol", 70), ("Expiry", 90),
                       ("Short", 70), ("Long", 70),
                       ("Credit", 75), ("Exit", 75), ("P&L", 80), ("Status", 80)]:
            ctk.CTkLabel(hdr, text=col, width=w,
                         font=ctk.CTkFont(size=11, weight="bold"),
                         text_color=C["accent"]).pack(side="left", padx=4, pady=6)

        ctk.CTkButton(hdr, text="↻ Aktualisieren", width=120, height=26,
                      fg_color=C["surface"], hover_color=C["header"],
                      font=ctk.CTkFont(size=11),
                      command=self._refresh_history).pack(side="right", padx=10)

        # Scrollbarer Bereich für Einträge
        self._history_scroll = ctk.CTkScrollableFrame(
            parent, fg_color=C["bg"], corner_radius=8,
            border_width=1, border_color=C["border"])
        self._history_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self._history_empty_lbl = ctk.CTkLabel(
            self._history_scroll,
            text="Noch keine abgeschlossenen Trades vorhanden.",
            text_color=C["dim"], font=ctk.CTkFont(size=13))
        self._history_empty_lbl.pack(pady=40)

        self._refresh_history()

    def _refresh_history(self):
        for w in self._history_scroll.winfo_children():
            w.destroy()

        history_file = os.path.join(_BASE, "trade_history.json")
        trades = []
        if os.path.exists(history_file):
            try:
                with open(history_file) as f:
                    trades = json.load(f)
            except Exception:
                pass

        if not trades:
            ctk.CTkLabel(self._history_scroll,
                         text="Noch keine abgeschlossenen Trades vorhanden.",
                         text_color=C["dim"], font=ctk.CTkFont(size=13)).pack(pady=40)
            return

        # Gesamt-P&L Zeile
        total_pnl = sum(t.get("pnl", 0) for t in trades)
        pnl_color = "#4ade80" if total_pnl >= 0 else "#ef4444"
        summary = ctk.CTkFrame(self._history_scroll, fg_color=C["surface2"], corner_radius=6)
        summary.pack(fill="x", padx=4, pady=(6, 10))
        ctk.CTkLabel(summary,
                     text=f"  Gesamt P&L:  {'+'if total_pnl>=0 else ''}${total_pnl:,.0f}   |   {len(trades)} Trades",
                     font=ctk.CTkFont(size=13, weight="bold"),
                     text_color=pnl_color).pack(side="left", padx=12, pady=6)

        # Trades — neueste zuerst
        for t in reversed(trades):
            pnl = t.get("pnl", 0)
            row_color = "#0d2b1a" if pnl > 0 else ("#2b0d0d" if pnl < 0 else C["surface2"])
            row = ctk.CTkFrame(self._history_scroll, fg_color=row_color, corner_radius=6)
            row.pack(fill="x", padx=4, pady=2)

            def lbl(parent, text, width, color="#e2e8f0"):
                ctk.CTkLabel(parent, text=text, width=width,
                             font=ctk.CTkFont(size=11), text_color=color,
                             anchor="w").pack(side="left", padx=4, pady=5)

            lbl(row, t.get("closed_at", "–"), 130)
            lbl(row, t.get("symbol", "–"), 70, C["accent"])
            lbl(row, t.get("expiry", "–"), 90)
            lbl(row, f"${t.get('short_strike', 0):.0f}", 70)
            lbl(row, f"${t.get('long_strike', 0):.0f}", 70)
            lbl(row, f"${t.get('entry_per_share', 0):.2f}", 75, "#4ade80")
            lbl(row, f"${t.get('exit_per_share', 0):.2f}", 75, "#f87171")
            pnl_str = f"{'+'if pnl>=0 else ''}${pnl:,.0f}"
            lbl(row, pnl_str, 80, "#4ade80" if pnl >= 0 else "#ef4444")
            status = t.get("status", "–")
            status_color = "#4ade80" if status == "done" else "#f59e0b"
            lbl(row, status, 80, status_color)

    # ── Settings tab ─────────────────────────────────────────────────────────

    def _build_settings(self, parent):
        parent.configure(fg_color=C["surface"])
        scroll = ctk.CTkScrollableFrame(parent, fg_color=C["surface"])
        scroll.pack(fill="both", expand=True, padx=8, pady=8)
        scroll.columnconfigure(1, weight=0)
        scroll.columnconfigure(2, weight=1)

        self._fields = {}
        self._row = 0

        def section(title):
            if self._row > 0:
                ctk.CTkFrame(scroll, height=1, fg_color=C["border"]).grid(
                    row=self._row, column=0, columnspan=3,
                    sticky="ew", padx=4, pady=(14, 0))
                self._row += 1
            lbl = ctk.CTkLabel(scroll, text=f"  {title}",
                         font=ctk.CTkFont(size=11, weight="bold"),
                         text_color=C["accent"], anchor="w",
                         fg_color=C["surface2"], corner_radius=4)
            lbl.grid(row=self._row, column=0, columnspan=3,
                     sticky="ew", pady=(10, 6), padx=0, ipady=4)
            self._row += 1

        def field(label, key, width=130, tip=""):
            ctk.CTkLabel(scroll, text=label, anchor="w",
                         font=ctk.CTkFont(size=12),
                         text_color=C["text"]).grid(
                row=self._row, column=0, sticky="w", padx=(10, 16), pady=3)
            e = ctk.CTkEntry(scroll, width=width,
                             font=ctk.CTkFont(family="Courier", size=12),
                             fg_color=C["bg"], border_color=C["border"],
                             text_color=C["text"])
            e.insert(0, str(self.cfg.get(key, "")))
            e.grid(row=self._row, column=1, sticky="w", pady=3)
            if tip:
                ctk.CTkLabel(scroll, text=tip, text_color=C["muted"],
                             font=ctk.CTkFont(size=11), anchor="w").grid(
                    row=self._row, column=2, sticky="w", padx=10)
            self._fields[key] = e
            self._row += 1

        def port_field(label, key):
            ctk.CTkLabel(scroll, text=label, anchor="w",
                         font=ctk.CTkFont(size=12)).grid(
                row=self._row, column=0, sticky="w", padx=(6, 16), pady=3)
            options = [
                "7497  (TWS Paper Trading)",
                "7496  (TWS Live Trading)",
                "4002  (IB Gateway Paper)",
                "4001  (IB Gateway Live)",
            ]
            cur = str(self.cfg.get(key, 7497))
            default = next((o for o in options if o.startswith(cur)), options[0])
            var = ctk.StringVar(value=default)
            m = ctk.CTkOptionMenu(scroll, values=options, variable=var, width=230,
                                  font=ctk.CTkFont(size=12))
            m.grid(row=self._row, column=1, columnspan=2, sticky="w", pady=3)
            self._fields[key] = m
            self._row += 1

        def toggle_field(label, key):
            ctk.CTkLabel(scroll, text=label, anchor="w",
                         font=ctk.CTkFont(size=12)).grid(
                row=self._row, column=0, sticky="w", padx=(6, 16), pady=3)
            var = ctk.BooleanVar(value=bool(self.cfg.get(key, True)))
            sw = ctk.CTkSwitch(scroll, text="", variable=var,
                               onvalue=True, offvalue=False)
            sw.grid(row=self._row, column=1, sticky="w", pady=3)
            self._fields[key] = var
            self._row += 1

        section("IB Verbindung")
        field("Host",           "ib_host",    tip="Normalerweise 127.0.0.1 — nicht ändern")
        port_field("Port",      "ib_port")
        field("Account-Nummer", "ib_account", width=160,
              tip="z.B. DU1234567 (Paper) oder U1234567 (Live)")

        section("Risiko-Management")
        field("Max. gleichzeitige Positionen",  "max_positions",       tip="Empfohlen: 6–10")
        field("Max. Positionen pro Sektor",     "max_per_sector",      tip="Verhindert Klumpenrisiko (empfohlen: 2)")
        field("Min. Kapitalreserve ($)",         "min_available_funds", tip="Kein neuer Trade wenn Konto darunter fällt")

        section("Strategie")
        field("Min. Implied Volatility (IV)",   "min_vola",        tip="0.28 = 28%  — IV-Filter für genug Prämie")
        field("OTM-Abstand Short Strike",       "abstand_y",       tip="0.10 = 10% unter aktuellem Kurs")
        field("Min. Credit pro Kontrakt ($)",   "min_credit",      tip="Empfohlen: 70–100 $")
        field("Min. Risk/Reward Ratio",         "min_risk_reward", tip="0.20 = Prämie ≥ 20% des max. Risikos")
        field("Max. Delta Short-Put",           "max_delta",       tip="0.28 = Short-Put max. 28% Ausübungswahrsch.")
        field("Scan-Intervall (Sekunden)",      "scan_intervall",  tip="Alle X Sekunden neu scannen")

        section("Exit-Regeln")
        field("Take-Profit (%)",   "take_profit_pct", tip="0.50 = schließen wenn 50% des Credits verdient")
        field("Stop-Loss Faktor",  "stop_loss_mult",  tip="2.0 = schließen wenn Verlust = 2× Credit")
        field("DTE-Exit (Tage)",   "dte_exit",        tip="Schließen wenn ≤ X Tage bis Verfall (Gamma-Schutz)")

        section("Automation")
        toggle_field("Auto-Trade — Orders automatisch platzieren", "auto_trade")

        ctk.CTkButton(
            scroll, text="  SPEICHERN  ", height=42,
            font=ctk.CTkFont(size=13, weight="bold"),
            fg_color=C["accent"], hover_color="#009e78",
            text_color="#000000", corner_radius=8,
            command=self._save_settings).grid(
            row=self._row, column=0, columnspan=3,
            sticky="w", padx=6, pady=(20, 4))
        self._row += 1

        self._save_lbl = ctk.CTkLabel(scroll, text="",
                                      font=ctk.CTkFont(size=12))
        self._save_lbl.grid(row=self._row, column=0, columnspan=3,
                            sticky="w", padx=6)

    # ── Guide tab ─────────────────────────────────────────────────────────────

    def _build_guide(self, parent):
        parent.configure(fg_color=C["surface"])
        box = ctk.CTkTextbox(
            parent, font=ctk.CTkFont(family="Courier", size=12),
            wrap="word", state="normal",
            fg_color=C["bg"], text_color="#7ca4c0",
            corner_radius=8, border_width=1, border_color=C["border"])
        box.pack(fill="both", expand=True, padx=10, pady=10)
        box.insert("1.0", IB_SETUP_TEXT)
        box.configure(state="disabled")

    # ── Auto-Updater ──────────────────────────────────────────────────────────

    def _update_bar_set(self, text: str, color: str):
        """Zeigt Text im Update-Banner (Hauptthread)."""
        for w in self._update_bar.winfo_children():
            w.destroy()
        self._update_bar.configure(height=36)
        ctk.CTkLabel(
            self._update_bar, text=text,
            font=ctk.CTkFont(size=12, weight="bold"),
            text_color=color,
        ).pack(side="left", padx=14, pady=6)

    def _update_bar_hide(self):
        """Blendet den Update-Banner aus."""
        for w in self._update_bar.winfo_children():
            w.destroy()
        self._update_bar.configure(height=0)

    def _check_for_updates(self):
        """Hintergrund-Thread: prüft GitHub und lädt Update automatisch herunter."""
        if "DEIN_USERNAME" in UPDATE_BASE_URL:
            return
        try:
            # 1. Version prüfen
            req = urllib.request.Request(
                f"{UPDATE_BASE_URL}/version.txt",
                headers={"User-Agent": "BotLauncher"})
            with urllib.request.urlopen(req, timeout=6) as r:
                remote = r.read().decode().strip()

            if remote == VERSION:
                return  # Aktuell — kein Banner nötig

            # 2. Update gefunden → automatisch herunterladen
            self.after(0, lambda: self._update_bar_set(
                f"  ⬇️  Update v{remote} wird heruntergeladen...", "#7dd3fc"))

            errors = []
            for filename in UPDATE_FILES:
                try:
                    r2 = urllib.request.Request(
                        f"{UPDATE_BASE_URL}/{filename}",
                        headers={"User-Agent": "BotLauncher"})
                    with urllib.request.urlopen(r2, timeout=15) as resp:
                        content = resp.read()
                    dest = os.path.join(_BASE, filename)
                    if os.path.exists(dest):
                        shutil.copy2(dest, dest + ".bak")
                    with open(dest, "wb") as f:
                        f.write(content)
                except Exception as e:
                    errors.append(f"{filename}: {e}")

            # 3. Ergebnis anzeigen und Banner nach 4 s ausblenden
            if errors:
                self.after(0, lambda: self._update_bar_set(
                    f"  ❌  Update fehlgeschlagen: {errors[0]}", "#f87171"))
                self.after(6000, lambda: self.after(0, self._update_bar_hide))
            else:
                self.after(0, lambda: self._update_bar_set(
                    f"  ✅  Aktualisiert auf v{remote} — App startet neu...", "#4ade80"))
                self.after(2500, lambda: self.after(0, self._restart_app))

        except Exception:
            pass  # kein Internet oder Repo nicht erreichbar

        if not errors:
            ctk.CTkButton(
                self._update_bar,
                text="Jetzt neu starten",
                width=160, height=30,
                fg_color="#166534", hover_color="#14532d",
                font=ctk.CTkFont(size=12, weight="bold"),
                command=self._restart_app,
            ).pack(side="left", padx=4)

    def _restart_app(self):
        """Startet den Launcher neu — aktiviert launcher.py.new falls vorhanden."""
        self.on_closing()
        if platform.system() == "Windows":
            subprocess.Popen([sys.executable] + sys.argv)
        else:
            os.execv(sys.executable, [sys.executable] + sys.argv)

    # ── Bot-Steuerung ─────────────────────────────────────────────────────────

    def _save_settings(self):
        try:
            for key, widget in self._fields.items():
                if isinstance(widget, ctk.BooleanVar):
                    self.cfg[key] = widget.get()
                elif isinstance(widget, ctk.CTkOptionMenu):
                    self.cfg[key] = int(widget.get().split()[0])
                else:
                    raw = widget.get().strip()
                    if key in ("ib_host", "ib_account"):
                        self.cfg[key] = raw
                    elif key in ("max_positions", "max_per_sector",
                                 "min_available_funds", "scan_intervall",
                                 "dte_exit", "min_credit"):
                        self.cfg[key] = int(raw)
                    else:
                        self.cfg[key] = float(raw)
            save_config(self.cfg)
            self._save_lbl.configure(text="✅  Einstellungen gespeichert!", text_color="#4ade80")
            self.after(3000, lambda: self._save_lbl.configure(text=""))
        except ValueError as e:
            self._save_lbl.configure(text=f"❌  Fehler: {e}", text_color="#f87171")

    def _start_bot(self):
        if self._running:
            return
        if not self.cfg.get("ib_account", "").strip():
            self._log_append(
                "⚠  Bitte zuerst die Account-Nummer in den Einstellungen eintragen!\n"
                "   (Tab 'Einstellungen' → Account-Nummer → Speichern)\n\n")
            return
        save_config(self.cfg)
        self._running     = True
        self._start_time  = datetime.now()
        self._status_dot.configure(text="⬤  LÄUFT", text_color=C["green"])
        self._start_btn.configure(state="disabled")
        self._stop_btn.configure(
            state="normal", text_color=C["red"],
            border_color=C["red"])
        self._log_append(f"[{datetime.now():%H:%M:%S}]  Bot wird gestartet...\n")
        self._stop_event = threading.Event()
        self._bot_thread = threading.Thread(target=self._run_bot_thread, daemon=True)
        self._bot_thread.start()
        self._pulse()
        self._tick_uptime()

    def _run_bot_thread(self):
        try:
            bot_file = os.path.join(_BASE, "bot.py")
            if os.path.exists(bot_file):
                # Heruntergeladene bot.py von Disk laden (Update-Mechanismus für frozen .app)
                import importlib.util
                spec = importlib.util.spec_from_file_location("bot", bot_file)
                _bot = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(_bot)
            else:
                import bot as _bot
            _bot._log_queue = self._queue
            asyncio.run(_bot.run_bot(self._stop_event))
        except Exception as e:
            self._queue.put(f"❌  Bot-Fehler: {e}\n")
        finally:
            self._queue.put(None)

    def _stop_bot(self):
        if not self._running or self._stop_event is None:
            return
        self._log_append(f"[{datetime.now():%H:%M:%S}]  Stoppe Bot...\n")
        self._stop_event.set()

    def _poll_queue(self):
        try:
            while True:
                item = self._queue.get_nowait()
                if item is None:
                    self._on_bot_stopped()
                else:
                    self._log_append(item)
        except queue_module.Empty:
            pass
        self.after(100, self._poll_queue)

    def _pulse(self):
        if not self._running:
            return
        self._pulse_state = not self._pulse_state
        color = C["green2"] if self._pulse_state else C["green"]
        self._status_dot.configure(text_color=color)
        self.after(700, self._pulse)

    def _tick_uptime(self):
        if not self._running or self._start_time is None:
            self._uptime_lbl.configure(text="")
            return
        delta = datetime.now() - self._start_time
        h, rem = divmod(int(delta.total_seconds()), 3600)
        m, s   = divmod(rem, 60)
        self._uptime_lbl.configure(text=f"⏱ {h:02d}:{m:02d}:{s:02d}")
        self.after(1000, self._tick_uptime)

    def _on_bot_stopped(self):
        self._running     = False
        self._start_time  = None
        self._stop_event  = None
        self._bot_thread  = None
        self._status_dot.configure(text="⏹  GESTOPPT", text_color=C["red"])
        self._uptime_lbl.configure(text="")
        self._start_btn.configure(state="normal")
        self._stop_btn.configure(state="disabled")
        self._log_append(f"[{datetime.now():%H:%M:%S}]  Bot gestoppt.\n")

    # Log-Zeile analysieren und passende Farbe wählen
    _LOG_TAGS = [
        (["✅", "Bracket-Order", "Fill", "geschlossen", "TRADE"], "green"),
        (["❌", "FEHLER", "fehlgeschlagen", "KRITISCHER"], "red"),
        (["⚠️", "⚠", "BLOCKIERT", "Timeout", "übersprungen"], "amber"),
        (["🔔", "TRIGGER", "ZYKLUS", "═"], "cyan"),
        (["🚀", "Trade:", "Score"], "blue"),
        (["─", "Scan abgeschlossen", "Pause"], "dim"),
    ]

    def _log_append(self, text: str):
        self._log.configure(state="normal")
        tb = self._log._textbox
        tag = None
        for keywords, t in self._LOG_TAGS:
            if any(kw in text for kw in keywords):
                tag = t
                break
        if tag:
            tb.insert("end", text, tag)
        else:
            tb.insert("end", text)
        self._log.see("end")
        self._log.configure(state="disabled")
        self._log_lines += text.count("\n")
        if self._log_lines > 0:
            self._log_count_lbl.configure(text=f"{self._log_lines} Zeilen")

    def _clear_log(self):
        self._log.configure(state="normal")
        self._log.delete("1.0", "end")
        self._log.configure(state="disabled")
        self._log_lines = 0
        self._log_count_lbl.configure(text="")

    def _restart_app(self):
        """Startet die App neu — lädt dabei ggf. die neue launcher.py."""
        if self._running:
            self._stop_bot()
        try:
            import subprocess
            subprocess.Popen([sys.executable] + [a for a in sys.argv if a != '--bootstrap'])
        except Exception:
            pass
        self.destroy()

    def on_closing(self):
        if self._running:
            dlg = ctk.CTkToplevel(self)
            dlg.title("Bot aktiv")
            dlg.geometry("380x140")
            dlg.resizable(False, False)
            dlg.configure(fg_color=C["surface"])
            dlg.grab_set()
            dlg.lift()
            ctk.CTkLabel(
                dlg,
                text="⚠️  Der Bot läuft noch.\nWirklich beenden?",
                font=ctk.CTkFont(size=14, weight="bold"),
                text_color=C["amber"],
            ).pack(pady=(24, 12))
            btn_frame = ctk.CTkFrame(dlg, fg_color="transparent")
            btn_frame.pack()
            def _confirm():
                dlg.destroy()
                self._stop_bot()
                if self._bot_thread and self._bot_thread.is_alive():
                    self._bot_thread.join(timeout=5)
                self.destroy()
            ctk.CTkButton(btn_frame, text="Ja, beenden", width=140, height=34,
                          fg_color="#dc2626", hover_color="#b91c1c",
                          command=_confirm).pack(side="left", padx=8)
            ctk.CTkButton(btn_frame, text="Abbrechen", width=140, height=34,
                          fg_color=C["surface2"], hover_color=C["header"],
                          command=dlg.destroy).pack(side="left", padx=8)
        else:
            self.destroy()


if __name__ == "__main__":
    # Aktiviere pending launcher-Update (launcher.py.new → launcher.py)
    _new = os.path.join(_BASE, "launcher.py.new")
    if os.path.exists(_new):
        try:
            shutil.move(_new, __file__)
        except Exception:
            pass  # Wenn es fehlschlägt: altes launcher.py bleibt erhalten

    app = BotLauncher()
    app.protocol("WM_DELETE_WINDOW", app.on_closing)
    app.mainloop()
