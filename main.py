"""
Paginatto — Iara WhatsApp Bot
Z-API + ChatGPT + CartPanda (Webhooks) + Catálogo JSON
Python 3.11+
"""

import os, re, json, unicodedata, logging, uuid, hmac, hashlib, asyncio, random
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import httpx

# ------------------------------ Load env & logging ---------------------------
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

ASSISTANT_NAME = os.getenv("ASSISTANT_NAME", "Iara")
BRAND_NAME = os.getenv("BRAND_NAME", "Paginatto")
SITE_URL = os.getenv("SITE_URL", "https://paginattoebooks.github.io/Paginatto.site.com.br/")
SUPPORT_URL = os.getenv("SUPPORT_URL", SITE_URL)
CNPJ = os.getenv("CNPJ", "57.941.903/0001-94")

SECURITY_BLURB = os.getenv("SECURITY_BLURB", "Checkout com HTTPS e PSP oficial. Não pedimos senhas/códigos.")
DELIVERY_ONE_LINER = "Entrega 100% digital. Acesso por e-mail/WhatsApp após pagamento."
CHECKOUT_RESUME_BASE = os.getenv("CHECKOUT_RESUME_BASE", "https://paginattoebooks.github.io/Paginatto.site.com.br/")

# Z-API
ZAPI_INSTANCE = os.getenv("ZAPI_INSTANCE", "3E2D08AA912D5063906206E9A5181015")
ZAPI_TOKEN = os.getenv("ZAPI_TOKEN", "45351C39E4EDCB47C2466177")
ZAPI_CLIENT_TOKEN = os.getenv("ZAPI_CLIENT_TOKEN", "F8d6942e55c57407e95c2ceae481f6a92S")
SEND_TEXT_PATH = os.getenv("SEND_TEXT_PATH", "/send-text")

# Catálogo
PRODUCTS_JSON_PATH = os.getenv("PRODUCTS_JSON_PATH", "produtos_paginatto.json")
MAX_MENU_ITEMS = int(os.getenv("MAX_MENU_ITEMS", "6"))

# OpenAI
from openai import OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI = OpenAI(api_key=OPENAI_API_KEY)

# CartPanda (somente webhooks; sem chamadas de API)
CARTPANDA_SIG_HEADER = os.getenv("CARTPANDA_SIG_HEADER", "X-Cartpanda-Signature")
CARTPANDA_HMAC_SECRET = os.getenv("CARTPANDA_HMAC_SECRET", "")

# Flags
DRY_RUN = os.getenv("DRY_RUN", "false").strip().lower() in {"1","true","yes","y"}
ENFORCE_CLIENT_TOKEN = os.getenv("ENFORCE_CLIENT_TOKEN", "false").lower() in {"1","true","yes","y"}
EVENT_SET = "seen_cartpanda_events"

# Sanidade de env críticos mínimos
for k, v in {"OPENAI_API_KEY": OPENAI_API_KEY}.items():
    if not v:
        raise RuntimeError(f"Defina {k} no ambiente")

app = FastAPI(title=f"{BRAND_NAME} — {ASSISTANT_NAME} Bot")

# ------------------------------ Redis (com fallback) ------------------------
import redis as _redis
class _Mem:
    def __init__(self):
        self.kv, self.h, self.l, self.s = {}, {}, {}, {}
    def get(self, k): return self.kv.get(k)
    def set(self, k, v): self.kv[k] = v
    def hset(self, name, key, value): self.h.setdefault(name, {})[key] = value
    def hget(self, name, key): return self.h.get(name, {}).get(key)
    def rpush(self, name, value): self.l.setdefault(name, []).append(value)
    def lrange(self, name, start, end):
        arr = self.l.get(name, []); end = len(arr)-1 if end == -1 else end
        return arr[start:end+1]
    def sadd(self, name, value): self.s.setdefault(name, set()).add(value)
    def sismember(self, name, value): return value in self.s.get(name, set())
    def incr(self, k): self.kv[k] = int(self.kv.get(k, 0)) + 1; return self.kv[k]
    def expire(self, k, ttl): return True

try:
    REDIS = _redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"), decode_responses=True)
    REDIS.ping()
except Exception:
    REDIS = _Mem()

# ------------------------------ HTTP client & retry -------------------------
HTTP = httpx.AsyncClient(timeout=httpx.Timeout(20.0))

async def _retry(fn, tries=3, base=0.4, cap=3.0):
    last = None
    for i in range(tries):
        try:
            return await fn()
        except Exception as e:
            last = e
            await asyncio.sleep(min(cap, base * (2 ** i)) + random.random()*0.2)
    raise last

# ------------------------------ Utils ---------------------------------------
CONTINUE_KW = {
    "sim","quero continuar","continuar","retomar","seguir","finalizar","pagar",
    "quero pagar","voltar ao carrinho","confirmar compra","retomar checkout",
    "retomar pedido","link do checkout","link do pedido","checkout","carrinho"
}
def _normalize(s: str) -> str:
    s = (s or "").lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9 ]+", " ", s).strip()

def digits_only(v: str) -> str: return re.sub(r"\D+", "", v or "")
def normalize_phone(v: str) -> str:
    d = digits_only(v)
    if d and not d.startswith("55"): d = "55" + d.lstrip("0")
    return d

def br_greeting() -> str:
    try:
        from zoneinfo import ZoneInfo
        h = datetime.now(ZoneInfo("America/Sao_Paulo")).hour
    except Exception:
        h = datetime.utcnow().hour
    if 6 <= h < 12: return "Bom dia"
    if 12 <= h < 18: return "Boa tarde"
    return "Boa noite"

def first_name(v: Optional[str]) -> str:
    n = (v or "").strip()
    return n.split()[0].title() if n else ""

def _clip(txt: str) -> str:
    txt = (txt or "").strip()
    if not txt: return txt
    parts = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?|!)\s+', txt)
    out = " ".join(parts[:2]).strip()
    if out and not re.search(r"[.!?]$", out): out += "."
    return out

def scrub_links_if_not_requested(user_text: str, reply: str) -> str:
    if any(k in (user_text or "").lower() for k in ["site","link","checkout"]): return reply
    return re.sub(r"https?://\S+", "", reply).strip()

def wants_resume(text: str) -> bool:
    t = _normalize(text)
    return any(k in t for k in _normalize(" ".join(CONTINUE_KW)).split())

# ------------------------------ Catálogo ------------------------------------
def load_products(path: str) -> Dict[str, Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) or []
    except Exception as e:
        logging.error(f"Falha ao carregar catálogo {path}: {e}")
        data = []
    out: Dict[str, Dict[str, Any]] = {}
    for item in data:
        name = (item.get("name") or "").strip()
        checkout = (item.get("checkout") or "").strip()
        if not name or not checkout: continue
        key = _normalize(name)
        p = {
            "name": name,
            "checkout": checkout,
            "image": item.get("image", ""),
            "description": item.get("description", ""),
            "sku": item.get("sku", ""),
        }
        aliases = {_normalize(name)}
        for w in _normalize(name).split():
            if len(w) > 3: aliases.add(w)
        for a in (item.get("aliases") or []): aliases.add(_normalize(a))
        m = re.search(r"(tabib).*(\d+)", name.lower())
        if m:
            n = m.group(2)
            for pat in [f"tabib {n}", f"tabib{n}", f"tab b {n}", f"tabb {n}", f"tabb{n}", f"v{n}", f"volume {n}"]:
                aliases.add(_normalize(pat))
        p["aliases"] = list(aliases)
        out[key] = p
    logging.info(f"Catálogo: {len(out)} itens")
    return out

PRODUCTS = load_products(PRODUCTS_JSON_PATH)

def _top_products(n: int = MAX_MENU_ITEMS) -> List[Dict[str, Any]]:
    return list(PRODUCTS.values())[:n]

def build_product_menu(n: int = MAX_MENU_ITEMS) -> List[Dict[str, str]]:
    menu = []
    for p in _top_products(n):
        name = (p.get("name") or "").strip()
        if name: menu.append({"name": name, "key": _normalize(name)})
    return menu

def find_product_in_text(text: str) -> Optional[Dict[str, Any]]:
    q = _normalize(text)
    if not q: return None
    m = re.search(r"\b(tabib|tabb|tab b)\s*([1-9]\d?)\b", q)
    if m:
        want = m.group(2)
        for p in PRODUCTS.values():
            aliases = set(p.get("aliases", []))
            if aliases & {f"tabib {want}", f"tabib{want}", f"tabb {want}", f"tab b {want}"}:
                return p
    for p in PRODUCTS.values():
        for a in p.get("aliases", []):
            if a and a in q: return p
    best, best_score = None, 0
    q_tokens = set(q.split())
    for p in PRODUCTS.values():
        name_tokens = set(_normalize(p.get("name","")).split())
        score = len(q_tokens & name_tokens)
        if score > best_score: best, best_score = p, score
    return best if best_score >= 2 else None

def suggest_upsell(prod_key: str) -> Optional[Dict[str,str]]:
    k = prod_key.lower()
    if "tabib" in k:
        return {"name":"Coleção TABIB Completa","sku":"TABIB_BUNDLE","pitch":"Leve os volumes com desconto exclusivo.","badge":"-15%"}
    return None

# ------------------------------ PostgreSQL ----------------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "")
if DATABASE_URL:
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
    import sqlalchemy as sa
    engine = create_async_engine(DATABASE_URL, pool_size=5, max_overflow=5)
    Session = async_sessionmaker(engine, expire_on_commit=False)
    meta = sa.MetaData()
    customers_tb = sa.Table("customers", meta,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("phone_e164", sa.String(16), unique=True, index=True, nullable=False),
        sa.Column("name", sa.String(120)),
        sa.Column("last_checkout_url", sa.Text),
    )
    messages_tb = sa.Table("messages", meta,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("customer_id", sa.String(36), index=True),
        sa.Column("role", sa.String(10)),
        sa.Column("text", sa.Text),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    checkouts_tb = sa.Table("checkouts", meta,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("phone_e164", sa.String(16), index=True),
        sa.Column("sku", sa.String(64), index=True),
        sa.Column("url", sa.Text, nullable=False),
        sa.Column("status", sa.String(20)),  # created|paid|expired
        sa.Column("source", sa.String(20)),  # webhook|api|catalog
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
    )
    webhook_events_tb = sa.Table("webhook_events", meta,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("idempotency_key", sa.String(128), unique=True, index=True),
    )
else:
    Session = None
    customers_tb = messages_tb = checkouts_tb = webhook_events_tb = None

@app.on_event("startup")
async def _db_bootstrap():
    try:
        if Session:
            async with engine.begin() as conn:
                await conn.run_sync(meta.create_all)
            logging.info("DB pronto (tabelas verificadas).")
    except Exception:
        logging.exception("DB init failed")

async def pg_ensure_customer(phone_e164: str, name: str="") -> Optional[str]:
    if not Session: return None
    try:
        import sqlalchemy as sa
        async with Session() as s, s.begin():
            row = (await s.execute(sa.select(customers_tb.c.id)
                                   .where(customers_tb.c.phone_e164==phone_e164))).first()
            if row: return row[0]
            cid = str(uuid.uuid4())
            await s.execute(customers_tb.insert().values(id=cid, phone_e164=phone_e164, name=name))
            return cid
    except Exception:
        logging.exception("pg_ensure_customer")
        return None

async def pg_save_message(customer_id: Optional[str], role: str, text: str):
    if not Session: return
    try:
        async with Session() as s, s.begin():
            await s.execute(messages_tb.insert().values(
                id=str(uuid.uuid4()), customer_id=customer_id, role=role, text=text))
    except Exception:
        logging.exception("pg_save_message")

async def pg_upsert_checkout(phone_e164: str, url: str, status: str, source: str, sku: str=""):
    if not Session: return
    try:
        import sqlalchemy as sa
        async with Session() as s, s.begin():
            await s.execute(checkouts_tb.insert().values(
                id=str(uuid.uuid4()), phone_e164=phone_e164, sku=sku, url=url, status=status, source=source))
            await s.execute(customers_tb.update()
                .where(customers_tb.c.phone_e164==phone_e164)
                .values(last_checkout_url=url))
    except Exception:
        logging.exception("pg_upsert_checkout")

async def pg_mark_webhook(idem_key: str) -> bool:
    if not Session: return True
    import sqlalchemy as sa
    async with Session() as s, s.begin():
        exists = (await s.execute(sa.select(webhook_events_tb.c.id)
                                  .where(webhook_events_tb.c.idempotency_key==idem_key))).first()
        if exists: return False
        await s.execute(webhook_events_tb.insert().values(id=str(uuid.uuid4()), idempotency_key=idem_key))
        return True

# ------------------------------ Checkout context (sem API) -------------------
def cp_resume_link_from_token(cart_token: str) -> Optional[str]:
    if not cart_token: return None
    if CHECKOUT_RESUME_BASE:
        return f"{CHECKOUT_RESUME_BASE}{cart_token}"
    return None

def order_summary(ctx: Optional[Dict[str, Any]]) -> str:
    if not ctx: return ""
    parts = []
    if ctx.get("order_no"): parts.append(f"Pedido: {ctx['order_no']}")
    if ctx.get("payment_status"): parts.append(f"Pagamento: {ctx['payment_status']}")
    if (ctx.get("customer") or {}).get("name"): parts.append(f"Cliente: {ctx['customer']['name']}")
    if (ctx.get("customer") or {}).get("email"): parts.append(f"E-mail: {ctx['customer']['email']}")
    if (ctx.get("customer") or {}).get("document"): parts.append(f"CPF: {ctx['customer']['document']}")
    if ctx.get("cart_url"): parts.append(f"Checkout: {ctx['cart_url']}")
    if ctx.get("checkout_url"): parts.append(f"Checkout: {ctx['checkout_url']}")
    if ctx.get("resume_link"): parts.append(f"Retomar: {ctx['resume_link']}")
    return " | ".join(parts)

async def resolve_checkout_context(phone_e164: str, text: str) -> Optional[Dict[str, Any]]:
    # 1) último carrinho conhecido
    token = REDIS.get(f"last_cart_by_phone:{phone_e164}")
    if token:
        ctx_json = REDIS.hget("carts_by_token", token)
        if ctx_json:
            ctx = json.loads(ctx_json)
            if ctx.get("cart_url"): ctx["resume_link"] = ctx.get("cart_url")
            return ctx
    # 2) último pedido conhecido
    order_no = REDIS.get(f"last_order_by_phone:{phone_e164}")
    if order_no:
        js = REDIS.hget("orders_by_no", order_no)
        if js:
            ctx = json.loads(js)
            if ctx.get("cart_url") and not ctx.get("resume_link"):
                ctx["resume_link"] = ctx["cart_url"]
            return ctx
    # 3) pedido citado no texto
    m = re.search(r"\b\d{6,}\b", text or "")
    if m:
        js = REDIS.hget("orders_by_no", m.group(0))
        if js:
            ctx = json.loads(js)
            if ctx.get("cart_url") and not ctx.get("resume_link"):
                ctx["resume_link"] = ctx["cart_url"]
            return ctx
    # 4) último checkout salvo no banco
    if Session:
        import sqlalchemy as sa
        async with Session() as s:
            row = (await s.execute(
                sa.select(customers_tb.c.last_checkout_url)
                  .where(customers_tb.c.phone_e164 == phone_e164)
            )).first()
            if row and row[0]:
                return {"resume_link": row[0], "customer": {"phone": phone_e164}}
    return None

# ------------------------------ LLM policy ----------------------------------
SYSTEM_TEMPLATE = (
    "Você é um assistente comercial curto e objetivo. "
    "Saudação curta: '{greeting}, {name}, tudo bem? Como posso ajudar?' (sem nome: '{greeting}, tudo bem? Como posso ajudar?'). "
    "Responda em 1–2 frases. Sem textão. "
    "Se pedirem produto específico → responda com nome, descrição curta (máx. 2 frases) e checkout direto. "
    "Se pedirem detalhes → até 2 frases. "
    "Se não pedirem link/site, não envie link algum. "
    "Entrega 100% digital. Nunca fale de endereço/frete/correios/rastreio. "
    "Se perguntarem por entrega/prazo/frete/rastreio → diga que é digital e enviada/liberada por e-mail/WhatsApp após pagamento, "
    "e ofereça checar status pelo nº do pedido ou CPF. "
    "Se perguntarem se chega na casa: diga que NÃO, pois é e-book digital. "
    "Se segurança → cite checkout HTTPS/PSP oficial. "
    "Se não recebeu por e-mail → peça nº do pedido ou CPF/CNPJ e ofereça reenvio. "
    "Se pagamento travou → pergunte em que etapa e ofereça ajuda. "
    "Se citar Instagram/engajamento → ofereça bônus após seguir e comentar 3 posts; peça @ para validar. "
    "Nunca peça senhas/códigos. Nunca prometa alterar preço automaticamente."
)

def system_prompt(extra_ctx: Optional[Dict[str, Any]], hints: Optional[Dict[str, Any]]=None) -> str:
    greeting = br_greeting()
    name = first_name(((extra_ctx or {}).get("customer") or {}).get("name") or (extra_ctx or {}).get("name"))
    base = SYSTEM_TEMPLATE.format(greeting=greeting, name=name or "")
    if extra_ctx and extra_ctx.get("resume_link"):
        base += f" Use este resume_link quando apropriado: {extra_ctx['resume_link']}"
    if hints and hints.get("product"):
        p = hints["product"]; base += f" Produto foco: {p.get('name')}. {p.get('description','')}"
    return base

async def llm_reply(history: List[Dict[str,str]], ctx: Optional[Dict[str,Any]], hints: Dict[str,Any]):
    msgs = [{"role":"system","content":system_prompt(ctx,hints)}]
    if ctx: msgs.append({"role":"assistant","content":f"DADOS_DO_PEDIDO: {order_summary(ctx)}"})
    msgs += history[-20:]
    try:
        resp = OPENAI.chat.completions.create(model=OPENAI_MODEL, temperature=0.2, max_tokens=160, messages=msgs)
        txt = (resp.choices[0].message.content or "").strip()
    except Exception:
        logging.exception("OPENAI_FAIL")
        txt = "Como posso ajudar com pagamento, status ou produto?"
    return _clip(txt)

# ------------------------------ Z-API senders -------------------------------
async def zapi_send_text(phone: str, message: str) -> dict:
    if DRY_RUN:
        logging.info(f"[DRY_RUN] -> {phone}: {message}")
        return {"ok": True, "dry_run": True}
    url = f"https://api.z-api.io/instances/{ZAPI_INSTANCE}/token/{ZAPI_TOKEN}{SEND_TEXT_PATH}"
    headers = {"Client-Token": ZAPI_CLIENT_TOKEN, "Content-Type": "application/json"}
    async def do(): return await HTTP.post(url, headers=headers, json={"phone": phone, "message": message})
    try:
        r = await _retry(do)
        data = (r.json() if "application/json" in r.headers.get("content-type","") else {"text": r.text})
        if r.status_code >= 300:
            logging.error(f"Z-API {r.status_code}: {data}")
            return {"ok": False, "status": r.status_code, "error": data}
        return {"ok": True, "status": r.status_code, "data": data}
    except Exception as e:
        logging.exception("ZAPI_SEND_TEXT_FAIL")
        return {"ok": False, "error": str(e)}

async def zapi_send_image(phone: str, image_url: str, caption: str="") -> dict:
    if not (image_url or "").lower().startswith("http"):
        return {"ok": False, "error": "invalid_image_url"}
    if DRY_RUN:
        logging.info(f"[DRY_RUN_IMG] -> {phone}: {image_url} | {caption}")
        return {"ok": True, "dry_run": True}
    url = f"https://api.z-api.io/instances/{ZAPI_INSTANCE}/token/{ZAPI_TOKEN}/send-image"
    headers = {"Client-Token": ZAPI_CLIENT_TOKEN, "Content-Type": "application/json"}
    async def do(): return await HTTP.post(url, headers=headers, json={"phone": phone, "image": image_url, "caption": caption})
    try:
        r = await _retry(do)
        data = (r.json() if "application/json" in r.headers.get("content-type","") else {"text": r.text})
        return {"ok": r.status_code < 300, "status": r.status_code, "data": data}
    except Exception as e:
        logging.exception("ZAPI_SEND_IMAGE_FAIL")
        return {"ok": False, "error": str(e)}

async def zapi_send_file(phone: str, file_url: str, caption: str="") -> dict:
    if not (file_url or "").lower().startswith("http"):
        return {"ok": False, "error": "invalid_file_url"}
    if DRY_RUN:
        logging.info(f"[DRY_RUN_FILE] -> {phone}: {file_url} | {caption}")
        return {"ok": True, "dry_run": True}
    url = f"https://api.z-api.io/instances/{ZAPI_INSTANCE}/token/{ZAPI_TOKEN}/send-file"
    headers = {"Client-Token": ZAPI_CLIENT_TOKEN, "Content-Type": "application/json"}
    async def do(): return await HTTP.post(url, headers=headers, json={"phone": phone, "file": file_url, "caption": caption})
    try:
        r = await _retry(do)
        data = (r.json() if "application/json" in r.headers.get("content-type","") else {"text": r.text})
        return {"ok": r.status_code < 300, "status": r.status_code, "data": data}
    except Exception as e:
        logging.exception("ZAPI_SEND_FILE_FAIL")
        return {"ok": False, "error": str(e)}

# ------------------------------ Segurança Webhooks --------------------------
def verify_cartpanda_hmac(raw_body: bytes, signature: str) -> bool:
    if not CARTPANDA_HMAC_SECRET:  # verificação opcional
        return True
    if not signature:
        return False
    mac = hmac.new(CARTPANDA_HMAC_SECRET.encode(), msg=raw_body, digestmod=hashlib.sha256).hexdigest()
    try:
        return hmac.compare_digest(mac, signature)
    except Exception:
        return mac == signature

# ------------------------------ Rate limit ----------------------------------
def rate_limit_ok(phone: str) -> bool:
    m1 = f"rl:1m:{phone}"
    m60 = f"rl:60m:{phone}"
    c1 = REDIS.incr(m1); c60 = REDIS.incr(m60)
    try:
        REDIS.expire(m1, 60); REDIS.expire(m60, 3600)
    except Exception:
        pass
    return c1 <= 40 and c60 <= 600

# ------------------------------ Endpoints -----------------------------------
def _get_client_token(request: Request, x_client_token: Optional[str], client_token: Optional[str]) -> str:
    return (
        x_client_token
        or client_token
        or request.headers.get("Client-Token")
        or request.query_params.get("client_token")
        or ""
    )

@app.get("/health")
async def health(): return {"ok": True}

@app.post("/webhook/zapi/receive")
async def zapi_receive(request: Request,
                       x_client_token: Optional[str] = Header(None),
                       client_token: Optional[str] = Header(None)):
    try:
        token = _get_client_token(request, x_client_token, client_token)
        if ENFORCE_CLIENT_TOKEN and token != ZAPI_CLIENT_TOKEN:
            raise HTTPException(status_code=401, detail="unauthorized")

        try:
            data = await request.json()
            logging.info(f"ZAPI RX -> {data}")
        except Exception:
            return JSONResponse({"status":"ignored","reason":"invalid json"})

        msg = data.get("message") or data.get("body") or data.get("text") or ""
        msg_id = data.get("messageId") or data.get("id")
        if isinstance(msg, dict):
            msg_id = msg.get("id") or msg_id
            msg = msg.get("text") or msg.get("body") or msg.get("message") or ""

        phone = normalize_phone(data.get("phone") or (data.get("sender") or {}).get("phone") or "")
        if not phone or not msg:
            return JSONResponse({"status":"ignored","reason":"missing phone or text"})

        if not rate_limit_ok(phone):
            await zapi_send_text(phone, "Muitas mensagens agora. Vou responder por partes, ok?")
            return JSONResponse({"status":"rate_limited"})

        if msg_id:
            if REDIS.sismember("seen_ids", msg_id): return JSONResponse({"status":"duplicate"})
            REDIS.sadd("seen_ids", msg_id)

        cid = await pg_ensure_customer(phone) if Session else None
        await pg_save_message(cid, "user", msg)

        # retomada de checkout
        if wants_resume(msg):
            ctx = await resolve_checkout_context(phone, msg)
            if ctx and (ctx.get("resume_link") or ctx.get("cart_url") or ctx.get("checkout_url")):
                link = ctx.get("resume_link") or ctx.get("cart_url") or ctx.get("checkout_url")
                await pg_upsert_checkout(phone, link, "created", "api")
                send = await zapi_send_text(phone, f"Perfeito. Seu checkout: {link}")
                await pg_save_message(cid, "assistant", f"Perfeito. Seu checkout: {link}")
                return JSONResponse({"status":"sent","route":"resume","send":send})
            ai = "Me envia nº do pedido ou CPF para puxar seu checkout."
            send = await zapi_send_text(phone, ai); await pg_save_message(cid, "assistant", ai)
            return JSONResponse({"status":"need_id","send":send})

        # intenções rápidas
        t = _normalize(msg)
        def _has(*xs): return any(x in t for x in xs)

        if _has("entrega","frete","chega","prazo","rastreio","rastreamento","correios","endereco","endereço"):
            ai = "É digital. Você recebe por e-mail/WhatsApp após o pagamento. Quer ajuda para finalizar?"
            send = await zapi_send_text(phone, ai); await pg_save_message(cid, "assistant", ai)
            return JSONResponse({"status":"sent","route":"quick","send":send})

        if _has("nao chegou","não chegou","nao recebi","não recebi","email","e mail","e-mail"):
            ai = "Me envia o nº do pedido ou CPF para eu checar."
            send = await zapi_send_text(phone, ai); await pg_save_message(cid, "assistant", ai)
            return JSONResponse({"status":"sent","route":"quick","send":send})

        if _has("seguran","golpe","fraude","medo"):
            ai = "Checkout seguro com HTTPS e PSP oficial. Não pedimos senhas."
            send = await zapi_send_text(phone, ai); await pg_save_message(cid, "assistant", ai)
            return JSONResponse({"status":"sent","route":"quick","send":send})

        if _has("nao consegui pagar","não consegui pagar","pagamento","pix","boleto","cartao","cartão"):
            ai = "Em que etapa o pagamento travou? PIX, cartão ou boleto?"
            send = await zapi_send_text(phone, ai); await pg_save_message(cid, "assistant", ai)
            return JSONResponse({"status":"sent","route":"quick","send":send})

        if _has("instagram","comentar","seguir","post"):
            ai = "Siga @Paginatto e comente em 3 posts para bônus. Qual seu @?"
            send = await zapi_send_text(phone, ai); await pg_save_message(cid, "assistant", ai)
            return JSONResponse({"status":"sent","route":"quick","send":send})

        if _has("suporte","ajuda","atendimento","catalogo","catálogo","produto","produtos","selecionar produto","escolher produto"):
            menu = build_product_menu(MAX_MENU_ITEMS)
            REDIS.set(f"menu:{phone}", json.dumps(menu))
            linhas = [f"{i+1}) {item['name']}" for i,item in enumerate(menu)]
            text = "Digite o nome do produto ou um número:\n" + "\n".join(linhas)
            send = await zapi_send_text(phone, text); await pg_save_message(cid, "assistant", text)
            return JSONResponse({"status":"sent","route":"menu","send":send})

        # compra concluída
        if _has("ja comprei","já comprei","acabei de comprar","comprei","ja paguei","já paguei","paguei","efetuei o pagamento",
                "ja realizei a compra","já realizei a compra","realizei a compra","ja fiz o pedido","já fiz o pedido","fiz o pedido","pedido feito"):
            ai = "Parabéns pela compra. Verifique seu e-mail. Se precisar, eu reenvio o acesso."
            send = await zapi_send_text(phone, ai); await pg_save_message(cid, "assistant", ai)
            return JSONResponse({"status":"sent","route":"purchase_done","send":send})

        # seleção por número do menu
        if re.fullmatch(r"\d{1,2}", t or ""):
            idx = int(t) - 1
            menu_raw = REDIS.get(f"menu:{phone}")
            if menu_raw:
                menu = json.loads(menu_raw)
                if 0 <= idx < len(menu):
                    sel_key = menu[idx]["key"]
                    prod = PRODUCTS.get(sel_key) or next((p for p in PRODUCTS.values()
                                                          if _normalize(p.get("name","")) == sel_key), None)
                    if prod:
                        ctx = await resolve_checkout_context(phone, msg)
                        link = (ctx and (ctx.get("resume_link") or ctx.get("cart_url") or ctx.get("checkout_url"))) or prod["checkout"]
                        await pg_upsert_checkout(phone, link, "created", "api" if ctx else "catalog", prod.get("sku",""))
                        reply = f"{prod['name']}\n{_clip(prod['description'])}\nCheckout: {link}\n{DELIVERY_ONE_LINER}"
                        if (prod.get("image") or "").lower().startswith("http"):
                            await zapi_send_image(phone, prod["image"], caption=prod["name"])
                        send = await zapi_send_text(phone, reply); await pg_save_message(cid, "assistant", reply)
                        u = suggest_upsell(_normalize(prod["name"]))
                        if u:
                            ups = f"{u['name']} {u['badge']} — {u['pitch']}"
                            await zapi_send_text(phone, ups); await pg_save_message(cid, "assistant", ups)
                        return JSONResponse({"status":"sent","route":"product_select","product":prod["name"],"send":send})

        # produto citado no texto
        prod = find_product_in_text(msg)
        if prod:
            ctx = await resolve_checkout_context(phone, msg)
            link = (ctx and (ctx.get("resume_link") or ctx.get("cart_url") or ctx.get("checkout_url"))) or prod["checkout"]
            await pg_upsert_checkout(phone, link, "created", "api" if ctx else "catalog", prod.get("sku",""))
            reply = f"{prod['name']}\n{_clip(prod['description'])}\nCheckout: {link}\n{DELIVERY_ONE_LINER}"
            if (prod.get("image") or "").lower().startswith("http"):
                await zapi_send_image(phone, prod["image"], caption=prod["name"])
            send = await zapi_send_text(phone, reply); await pg_save_message(cid, "assistant", reply)
            u = suggest_upsell(_normalize(prod["name"]))
            if u:
                ups = f"{u['name']} {u['badge']} — {u['pitch']}"
                await zapi_send_text(phone, ups); await pg_save_message(cid, "assistant", ups)
            return JSONResponse({"status":"sent","route":"product","product":prod["name"],"send":send})

        # LLM fallback
        history_key = f"sessions:{phone}"
        history = json.loads(REDIS.get(history_key) or "[]")
        history.append({"role":"user","content":msg})
        ctx = await resolve_checkout_context(phone, msg)
        ai = await llm_reply(history, ctx, hints={})
        if len(history) == 1 and _normalize(msg) in {"oi","ola","olá","bom dia","boa tarde","boa noite","oii","oie"}:
            ai = f"{br_greeting()}! Como posso ajudar?"
        ai = _clip(scrub_links_if_not_requested(msg, ai))

        send = await zapi_send_text(phone, ai); await pg_save_message(cid, "assistant", ai)
        if not send.get("ok"):
            logging.error(f"send_fail: {send}")

        history.append({"role":"assistant","content":ai})
        REDIS.set(history_key, json.dumps(history))
        return JSONResponse({"status":"sent","route":"llm","send":send})

    except HTTPException:
        raise
    except Exception:
        logging.exception("UNHANDLED_receive")
        # nunca derruba o webhook
        return JSONResponse({"status":"error_handled"}, status_code=200)

@app.post("/webhook/zapi/status")
async def zapi_status(request: Request,
                      x_client_token: Optional[str] = Header(None),
                      client_token: Optional[str] = Header(None)):
    token = _get_client_token(request, x_client_token, client_token)
    if ENFORCE_CLIENT_TOKEN and token != ZAPI_CLIENT_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")
    data = await request.json()
    logging.info(f"Webhook Z-API status: {data}")
    return {"ok": True}

# ----------------------------- Webhooks CartPanda ----------------------------
@app.post("/webhook/cartpanda/order")
async def cartpanda_order(request: Request):
    raw = await request.body()
    signature = request.headers.get(CARTPANDA_SIG_HEADER, "")
    if not verify_cartpanda_hmac(raw, signature):
        raise HTTPException(status_code=401, detail="invalid signature")

    data = await request.json()
    logging.info(f"Webhook CartPanda: {data}")

    # idempotência por event id
    evt_id = str((data.get("id") or (data.get("data") or {}).get("id") or "")).strip() or str(uuid.uuid4())
    if evt_id:
        if REDIS.sismember(EVENT_SET, evt_id): return JSONResponse({"ok": True, "dup": True})
        REDIS.sadd(EVENT_SET, evt_id)
    if Session:
        try:
            ok = await pg_mark_webhook(evt_id)
            if not ok: return JSONResponse({"ok": True, "dup": True})
        except Exception:
            logging.exception("pg_mark_webhook")

    # lista de abandonados (payload agregado)
    carts = (data.get("abandoned_carts") or {}).get("data")
    if isinstance(carts, list):
        for c in carts:
            cart_token = (c.get("cart_token") or "").strip()
            phone = normalize_phone(((c.get("customer") or {}).get("phone")) or "")
            if not phone or not cart_token: continue
            ctx = {
                "cart_token": cart_token,
                "cart_url": c.get("cart_url"),
                "resume_link": cp_resume_link_from_token(cart_token) or c.get("cart_url"),
                "customer": c.get("customer") or {},
            }
            REDIS.set(f"last_cart_by_phone:{phone}", cart_token)
            REDIS.hset("carts_by_token", cart_token, json.dumps(ctx))
            if Session and ctx.get("resume_link"):
                await pg_upsert_checkout(phone, ctx["resume_link"], "created", "webhook")
        return JSONResponse({"ok": True, "mode": "abandoned_list", "count": len(carts)})

    # evento único
    d = data.get("data") or data
    customer = d.get("customer") or {}
    phone = normalize_phone(customer.get("phone") or d.get("phone") or "")
    cart_token = (d.get("cart_token") or d.get("cartToken") or "").strip()
    cart_url = d.get("cart_url") or d.get("checkout_url") or ""
    order_no = str(d.get("order_no") or d.get("number") or d.get("id") or d.get("orderNumber") or "").strip()

    if not phone and not cart_token and not order_no:
        return JSONResponse({"ok": False, "reason": "missing identifiers"}, status_code=400)

    ctx = {
        "order_no": order_no or None,
        "cart_token": cart_token or None,
        "cart_url": cart_url or None,
        "customer": {
            "name": customer.get("full_name") or (customer.get("first_name","")+" "+customer.get("last_name","")).strip(),
            "email": (customer.get("email") or d.get("email") or "").strip().lower(),
            "phone": phone,
            "document": digits_only(customer.get("cpf") or d.get("cpf") or ""),
        },
    }
    if phone and cart_token:
        REDIS.set(f"last_cart_by_phone:{phone}", cart_token)
        ctx["resume_link"] = cp_resume_link_from_token(cart_token) or cart_url
        REDIS.hset("carts_by_token", cart_token, json.dumps(ctx))
        if Session and ctx.get("resume_link"):
            await pg_upsert_checkout(phone, ctx["resume_link"], "created", "webhook")
    if order_no:
        REDIS.hset("orders_by_no", order_no, json.dumps(ctx))
        if phone: REDIS.set(f"last_order_by_phone:{phone}", order_no)

    return JSONResponse({"ok": True, "mode": "single", "has_token": bool(cart_token), "has_order": bool(order_no)})

@app.post("/webhook/cartpanda/support")
async def cartpanda_support(request: Request):
    raw = await request.body()
    signature = request.headers.get(CARTPANDA_SIG_HEADER, "")
    if not verify_cartpanda_hmac(raw, signature):
        raise HTTPException(status_code=401, detail="invalid signature")

    data = await request.json()
    logging.info(f"Webhook CartPanda suporte: {data}")
    d = data.get("data") or data
    order_no = str(d.get("order_no") or d.get("number") or d.get("id") or "").strip()
    phone = normalize_phone((d.get("customer") or {}).get("phone") or d.get("phone") or "")
    if phone and order_no: REDIS.set(f"last_order_by_phone:{phone}", order_no)
    if order_no and not REDIS.hget("orders_by_no", order_no):
        REDIS.hset("orders_by_no", order_no, json.dumps({"order_no": order_no, "customer": d.get("customer") or {}}))
    return {"ok": True, "linked_phone": phone, "order_no": order_no}

# ------------------------------ Lifespan ------------------------------------
@app.on_event("shutdown")
async def shutdown_event():
    try: await HTTP.aclose()
    except Exception: pass

# ------------------------------ Run (local) ---------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))








