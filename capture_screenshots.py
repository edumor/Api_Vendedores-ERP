"""
capture_screenshots.py — Captura automática de todas las secciones
Requisitos: pip install playwright && playwright install chromium

Ejecutar desde la carpeta api_vendedores:
    python capture_screenshots.py
"""

import os
import time
from playwright.sync_api import sync_playwright

BASE_URL   = "http://193.168.160.5:8000"
USUARIO    = "emoreno"
CONTRASENA = "miralla911"
OUT_DIR    = os.path.join(os.path.dirname(__file__), "screenshots")
os.makedirs(OUT_DIR, exist_ok=True)

def shot(page, name):
    path = os.path.join(OUT_DIR, f"{name}.png")
    page.screenshot(path=path, full_page=False)
    print(f"  ✓ {name}.png")

def admin_screenshots(page):
    print("\n[Admin Panel]")

    # Login
    page.goto(f"{BASE_URL}/admin")
    page.wait_for_load_state("networkidle")
    shot(page, "admin-login")

    # Ingresar
    page.fill("input[placeholder='Usuario Flexxus']", USUARIO)
    page.fill("input[placeholder='Contraseña']", CONTRASENA)
    page.click("button:has-text('Ingresar')")
    page.wait_for_load_state("networkidle")
    time.sleep(1)
    shot(page, "admin-dashboard")

    tabs = {
        "Stock":        "admin-stock",
        "Perfiles":     "admin-perfiles",
        "Asignaciones": "admin-asignaciones",
        "Funciones":    "admin-funciones",
        "Multiplazos":  "admin-multiplazos",
        "Catálogos":    "admin-catalogos",
        "Ofertas":      "admin-ofertas",
        "Rotación":     "admin-rotacion",
        "Pedido/Ppto":  "admin-pedido-ppto",
        "Ajuste Stock": "admin-ajuste-stock",
        "Auditoría":    "admin-auditoria",
    }

    for tab_text, filename in tabs.items():
        try:
            page.click(f"text={tab_text}", timeout=5000)
            page.wait_for_load_state("networkidle")
            time.sleep(0.8)
            shot(page, filename)
        except Exception as e:
            print(f"  ✗ {filename} — {e}")

def frontend_screenshots(page):
    print("\n[Portal Vendedores]")

    # Login
    page.goto(f"{BASE_URL}/")
    page.wait_for_load_state("networkidle")
    shot(page, "frontend-login")

    # Ingresar
    page.fill("input[placeholder='Usuario']", USUARIO)
    page.fill("input[type='password']", CONTRASENA)
    page.click("button:has-text('Ingresar')")
    page.wait_for_load_state("networkidle")
    time.sleep(1)

    # Stock con datos
    page.click("text=Stock")
    time.sleep(0.5)
    page.click("button:has-text('Buscar')")
    page.wait_for_load_state("networkidle")
    time.sleep(3)
    shot(page, "frontend-stock")

    tabs = {
        "Cta. Corriente": "frontend-ctacte",
        "Pedidos":        "frontend-pedidos",
        "Presupuestos":   "frontend-presupuestos",
        "¿Qué Vendí?":   "frontend-vendidos",
        "Catálogos":      "frontend-catalogos",
    }

    for tab_text, filename in tabs.items():
        try:
            page.click(f"text={tab_text}", timeout=5000)
            page.wait_for_load_state("networkidle")
            time.sleep(1)
            shot(page, filename)
        except Exception as e:
            print(f"  ✗ {filename} — {e}")

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 860})

        admin_screenshots(page)

        # Nueva página para frontend (sesión separada)
        page2 = browser.new_page(viewport={"width": 1440, "height": 860})
        frontend_screenshots(page2)

        browser.close()
        print(f"\n✅ Capturas guardadas en: {OUT_DIR}")

if __name__ == "__main__":
    main()
