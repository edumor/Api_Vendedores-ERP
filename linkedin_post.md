# Post LinkedIn — Api Vendedores Microbell SA

---

## 🇦🇷 ESPAÑOL

🚀 **Completé el desarrollo de un sistema integral de gestión de ventas para Microbell SA**

Durante los últimos meses construí una plataforma completa que conecta una base de datos Firebird SQL externa con una interfaz web moderna para los corredores de ventas de la empresa.

**¿Qué hace el sistema?**
- Los vendedores consultan precios, stock y cuentas corrientes de clientes en tiempo real desde cualquier dispositivo
- Los gerentes publican catálogos con un solo clic → se envían automáticamente por WhatsApp Business, push notification y email
- Los pedidos se registran directamente en la base de datos de producción en tiempo real
- Control de acceso por rol: Gerente / Administración de Ventas / Vendedor

**Stack técnico:**
🐍 FastAPI (Python) — ~11.000 líneas de lógica de API
🗄️ Firebird SQL (base de datos externa) vía `firebirdsql`
💾 SQLite para estado local (usuarios, catálogos, sesiones)
🔐 Autenticación JWT con jerarquía de roles
📲 WhatsApp Business Cloud API (Meta) — plantillas aprobadas
🔔 OneSignal para push notifications
📧 Gmail SMTP para distribución de catálogos
🖥️ Single Page Apps en Vanilla JS — sin frameworks, control total
⚙️ Deployado en Windows Server con Uvicorn

**El desafío:** la base de datos no tiene API oficial. Cada endpoint requirió reverse-engineering del esquema Firebird sin tocar el sistema de producción.

El resultado: los gerentes distribuyen un catálogo a todo el equipo de ventas en un clic — WhatsApp, push y email se disparan simultáneamente.

Código en GitHub 👉 https://github.com/edumor/Api-Flexxus-ERP

\#FastAPI \#Python \#Firebird \#WhatsAppBusiness \#APIIntegration \#DesarrolloBackend \#SoftwareDevelopment \#Argentina \#Tecnología

---

## 🌐 ENGLISH

🚀 **Just shipped: a full-stack sales management system for a field sales team in Argentina**

Over the past months I built a complete platform connecting an external Firebird SQL database with a modern web interface for field sales reps at Microbell SA.

**What it does:**
- Sales reps access real-time pricing, stock, and customer account statements from any device
- Managers publish catalogs with one click → automated WhatsApp Business messages, push notifications, and emails fire simultaneously
- Orders write back directly to the production database in real time
- Role-based access control: Manager / Sales Admin / Sales Rep

**Tech stack:**
🐍 FastAPI (Python) — ~11,000 lines of API logic
🗄️ Firebird SQL (external database) via `firebirdsql`
💾 SQLite for local state (users, catalogs, sessions)
🔐 JWT authentication with role hierarchy
📲 WhatsApp Business Cloud API (Meta) — approved message templates
🔔 OneSignal push notifications
📧 Gmail SMTP for catalog distribution
🖥️ Vanilla JS single-page apps — no framework, full control
⚙️ Deployed on Windows Server with Uvicorn

**The challenge:** the database had no official API. Every endpoint required reverse-engineering the Firebird schema while keeping the live production system untouched.

The result: managers distribute a catalog to the entire sales team in one click — WhatsApp, push, and email fire at once.

Code on GitHub 👉 https://github.com/edumor/Api-Flexxus-ERP

\#FastAPI \#Python \#Firebird \#WhatsAppBusiness \#APIIntegration \#BackendDevelopment \#SoftwareDevelopment \#Argentina
