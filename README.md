# 🧾 Api Vendedores — Microbell SA

A full-stack **sales management API** that connects to a **Firebird SQL** external database and exposes its data through a modern web frontend for field sales reps. Built with **FastAPI** and deployed on a Windows Server, it enables real-time access to customer data, pricing, orders, account balances, and catalog distribution — with WhatsApp Business notifications and push alerts.

---

## 🖥️ Architecture

```
┌─────────────────────┐        ┌──────────────────────┐
│   admin.html        │        │   frontend.html       │
│  (Admin Panel)      │◄──────►│  (Sales Rep Portal)   │
│  Managers / Admin   │        │  Field Vendedores      │
└────────┬────────────┘        └──────────┬────────────┘
         │                                │
         └──────────────┬─────────────────┘
                        │  HTTP / JWT Auth
                 ┌──────▼──────┐
                 │  FastAPI    │  ← main.py (~11,000 lines)
                 │  (Python)   │
                 │  Port 8000  │
                 └──────┬──────┘
          ┌─────────────┼──────────────┐
          │             │              │
   ┌──────▼──────┐ ┌────▼────┐ ┌──────▼───────┐
   │ Firebird DB │ │SQLite   │ │ Meta Cloud   │
   │  (external) │ │admin.db │ │ WhatsApp API │
   │ (remote IP) │ │(local)  │ │ + OneSignal  │
   └─────────────┘ └─────────┘ └──────────────┘
```

---

## 🚀 Tech Stack

| Layer | Technology |
|---|---|
| **API Backend** | Python 3.11 · FastAPI · Uvicorn |
| **Database** | Firebird SQL (external) via `firebirdsql` |
| **Local Storage** | SQLite (`admin.db`) — catalogs, users, sessions |
| **Authentication** | JWT (HS256) — role-based: Admin / Gerente / Vendedor |
| **Frontend** | Vanilla JS · HTML5 · CSS3 (Single Page Apps) |
| **Notifications** | WhatsApp Business Cloud API (Meta) · OneSignal Push |
| **Email** | Gmail SMTP via App Password (STARTTLS) |
| **Deployment** | Windows Server · Uvicorn · `.bat` startup scripts |

---

## ✨ Key Features

### 👔 Admin Panel (`admin.html`)
- **User management**: create/edit salespeople, assign territories and customer portfolios
- **Catalog management**: publish PDF/image catalogs, notify via WhatsApp + email + push
- **Order monitoring**: real-time order tracking with ERP status sync
- **Credit control**: customer credit limits and outstanding balance dashboard
- **Sales reporting**: by vendor, by product, by date range — fed directly from Firebird
- **PREVENTA mode**: lock/unlock pre-sale order windows per salesperson

### 📱 Sales Rep Portal (`frontend.html`)
- **Product catalog**: real-time pricing and stock availability from external database
- **Order placement**: create/submit orders that write back to the external database
- **Customer portfolio**: view assigned customers, balances, payment history
- **Account statements**: full current account with comprobantes breakdown
- **WhatsApp catalog**: receive product catalog links directly on WhatsApp

### 🔔 Notifications
- **WhatsApp Business API**: template messages for catalog distribution (`microbell_catalogo`)
- **OneSignal**: push notifications to sales reps for new catalogs and updates
- **Email**: catalog announcements via Gmail SMTP

---

## 📁 Project Structure

```
api_vendedores/
├── main.py                  # FastAPI app (~11k lines) — all API endpoints
├── admin.html               # Admin control panel (SPA)
├── frontend.html            # Sales rep portal (SPA)
├── admin.db                 # SQLite: users, catalogs, sessions (not in repo)
├── .env                     # Secrets — NOT committed (see .env.example)
├── .env.example             # Template for environment variables
├── requirements.txt         # Python dependencies
├── iniciar.bat              # Start uvicorn server
├── instalar.bat             # Install dependencies
├── manifest.json            # PWA manifest
├── sw.js                    # Service Worker (offline support)
└── catalogos/               # Uploaded catalog files (PDF/images)
```

---

## ⚙️ Setup

### Prerequisites
- Python 3.11+
- Firebird client libraries (for `firebirdsql`)
- Access to the Firebird external database
- Meta Business account with WhatsApp Business API approved
- Gmail account with App Password enabled

### Installation

```bash
# 1. Clone the repo
git clone https://github.com/edumor/Api-Flexxus-ERP.git
cd Api-Flexxus-ERP

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment
copy .env.example .env
# Edit .env with your credentials

# 4. Start the server
uvicorn main:app --host 0.0.0.0 --port 8000
# or double-click iniciar.bat on Windows
```

---

## 🔑 Environment Variables

See [`.env.example`](.env.example) for the full list. Key variables:

| Variable | Description |
|---|---|
| `FB_HOST` | Firebird external database server IP |
| `DB_L1` | Main database path (.gdb) |
| `WA_PHONE_NUMBER_ID` | WhatsApp Business phone number ID |
| `WA_ACCESS_TOKEN` | Meta System User permanent token |
| `JWT_SECRET_KEY` | Secret for JWT signing |
| `SMTP_HOST` / `SMTP_PASS` | Email server credentials |

---

## 🔐 Authentication

JWT-based auth with role hierarchy:

| Role | Access |
|---|---|
| `ADMINISTRACION DE VENTAS` | Full admin panel |
| `GERENTE` | Read-only admin view + reports |
| `VENDEDOR` | Sales portal only |

Tokens expire after 10 hours (configurable via `JWT_EXPIRE_HOURS`).

---

## 📸 Screenshots

### 🔐 Login screens

| Admin Panel | Sales Rep Portal |
|---|---|
| ![Admin Login](screenshots/admin-login.png) | ![Frontend Login](screenshots/frontend-login.png) |

---

### 👔 Admin Panel — Control Panel

**Dashboard** — real-time KPIs: active vendor profiles, multiplazos, catalogs, and promotions

![Admin Dashboard](screenshots/admin-dashboard.png)

**Stock by Warehouse** — multi-depot inventory lookup with Rubro/Marca filters and Excel/PDF export

![Stock por Depósito](screenshots/admin-stock.png)

**Vendor Profiles** — manage sales rep profiles (TECNOLOGIA, OUTDOORS, SPORTS, HOGAR, etc.)

![Perfiles de Vendedor](screenshots/admin-perfiles.png)

**Functions Control** — enable/disable features globally or per sales rep (orders, quotes, depots)

![Funciones](screenshots/admin-funciones.png)

**Multiplazos** — assign payment terms per vendor, individually or in bulk

![Multiplazos](screenshots/admin-multiplazos.png)

**Catalog Management** — publish catalogs with one click → WhatsApp + email + push notifications

![Catálogos](screenshots/admin-catalogos.png)

**Promotions Engine** — create and manage time-limited offers with quotas per vendor profile

![Ofertas](screenshots/admin-ofertas.png)

**Stock Rotation Analysis** — detect slow-moving items, calculate margins, compare with MercadoLibre pricing

![Rotación de Stock](screenshots/admin-rotacion.png)

**Stock Adjustment** — bulk CSV import with rollback history (GERENTE/Admin only)

![Ajuste de Stock](screenshots/admin-ajuste-stock.png)

**Audit Log** — full activity trail: user, section, action, timestamp, and IP

![Auditoría](screenshots/admin-auditoria.png)

---

### 📱 Sales Rep Portal

**Live Stock** — real-time pricing and availability per warehouse, with Excel/PDF export

![Stock Vendedores](screenshots/frontend-stock.png)

**Account Statement** — current account lookup by customer name or code

![Cuenta Corriente](screenshots/frontend-ctacte.png)

**Orders & Quotes** — create new orders or quotes, view history

| Orders | Quotes |
|---|---|
| ![Pedidos](screenshots/frontend-pedidos.png) | ![Presupuestos](screenshots/frontend-presupuestos.png) |

**What Did I Sell?** — sales history by customer and date range

![Qué Vendí](screenshots/frontend-vendidos.png)

**Catalogs** — catalogs published by admin, available to assigned sales reps

![Catálogos Vendedores](screenshots/frontend-catalogos.png)

---

## 📡 API Highlights

```
POST /login                    # JWT authentication
GET  /vendedor/articulos       # Product catalog with live pricing from Firebird
POST /vendedor/pedido          # Submit order to external database
GET  /admin/clientes           # Customer portfolio management
GET  /admin/resumen-deudas     # Account balance dashboard
POST /admin/catalogo/publicar  # Publish catalog + send WA/email/push
GET  /admin/ordenes            # Order monitoring
POST /admin/test-whatsapp      # Test WhatsApp template delivery
```

---

## 🏢 About

Built for **Microbell SA** (Argentina) to digitize their field sales operation:
- Sales reps access real-time data from any device
- Managers control catalog distribution and monitor orders
- WhatsApp Business automates catalog delivery to customers

---

## 📄 License

Private — proprietary code for Microbell SA internal use.
