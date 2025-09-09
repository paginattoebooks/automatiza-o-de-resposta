"""
Paginatto — Iara WhatsApp Bot
Z-API + ChatGPT + CartPanda (somente via Webhook) + Catálogo JSON
Python 3.11+
"""

import os
import re
import json
import unicodedata
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import httpx

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# ---------------------------------------------------------------------------
# Redis (com fallback em memória)
# ---------------------------------------------------------------------------
import redis as _redis

try:
    REDIS = _redis.Redis.from_url(
        os.getenv("REDIS_URL", "redis://localhost:6379"),
        decode_responses=True
    )
    REDIS.ping()
except Exception:
    class _Mem:
        def __init__(self):
            self.kv = {}
            self.h = {}
            self.l = {}
            self.s = {}
        # KV
        def get(self, k): return self.kv.get(k)
        def set(self, k, v): self.kv[k] = v
        # HASH
        def hset(self, name, key, value): self.h.setdefault(name, {})[key] = value
        def hget(self, name, key): return self.h.get(name, {}).get(key)
        # LIST
        def rpush(self, name, value): self.l.setdefault(name, []).append(value)
        def lrange(self, name, start, end):
            arr = self.l.get(name, [])
            if end == -1: end = len(arr) - 1
            return arr[start:end+1]
        # SET
        def sadd(self, name, value): self.s.setdefault(name, set()).add(value)
        def sismember(self, name, value): return value in self.s.get(name, set())
    REDIS = _Mem()

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
load_dotenv()

ASSISTANT_NAME = os.getenv("ASSISTANT_NAME", "Iara")
BRAND_NAME = os.getenv("BRAND_NAME", "Paginatto")
SITE_URL = os.getenv("SITE_URL", "https://paginattoebooks.github.io/Paginatto.site.com.br/")
SUPPORT_URL = os.getenv("SUPPORT_URL", SITE_URL)
CNPJ = os.getenv("CNPJ", "57.941.903/0001-94")

SECURITY_BLURB = os.getenv("SECURITY_BLURB", "Checkout com HTTPS e PSP oficial. Não pedimos senhas ou códigos.")
CHECKOUT_RESUME_BASE = os.getenv("CHECKOUT_RESUME_BASE", "https://somasoundsolutions.mycartpanda.com/resume/")
DELIVERY_ONE_LINER = (
    "Entrega 100% digital. Enviamos/liberamos o e-book por e-mail e WhatsApp após o pagamento. "
    "Não pedimos endereço e não existe rastreio."
)

ZAPI_INSTANCE = os.getenv("ZAPI_INSTANCE", "3E2D08AA912D5063906206E9A5181015")
ZAPI_TOKEN = os.getenv("ZAPI_TOKEN", "45351C39E4EDCB47C2466177")
ZAPI_CLIENT_TOKEN = os.getenv("ZAPI_CLIENT_TOKEN", "F8d6942e55c57407e95c2ceae481f6a92S")
SEND_TEXT_PATH = os.getenv("SEND_TEXT_PATH", "/send-text")

PRODUCTS_JSON_PATH = os.getenv("PRODUCTS_JSON_PATH", "produtos_paginatto.json")
MAX_MENU_ITEMS = int(os.getenv("MAX_MENU_ITEMS", "6"))

DRY_RUN = os.getenv("DRY_RUN", "false").strip().lower() in {"1", "true", "yes", "y"}

# OpenAI
from openai import OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
MODEL_NAME = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI = OpenAI(api_key=OPENAI_API_KEY)

# Sanidade de env críticos
for k, v in {
    "OPENAI_API_KEY": OPENAI_API_KEY,
    "ZAPI_INSTANCE": ZAPI_INSTANCE,
    "ZAPI_TOKEN": ZAPI_TOKEN,
    "ZAPI_CLIENT_TOKEN": ZAPI_CLIENT_TOKEN,
}.items():
    if not v:
        raise RuntimeError(f"Defina {k} no .env")

app = FastAPI(title=f"{BRAND_NAME} — {ASSISTANT_NAME} Bot")

# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------
CONTINUE_KW = {
    "sim", "quero continuar", "continuar", "retomar", "seguir",
    "finalizar", "pagar", "quero pagar", "voltar ao carrinho", "confirmar compra",
    "retomar checkout", "retomar pedido", "link do checkout", "link do pedido"
}

def _normalize(s: str) -> str:
    s = (s or "").lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9 ]+", " ", s).strip()

def digits_only(v: str) -> str:
    return re.sub(r"\D+", "", v or "")

def normalize_phone(v: str) -> str:
    d = digits_only(v)
    if d and not d.startswith("55"):
        d = "55" + d
    return d

def br_greeting() -> str:
    try:
        h = datetime.now(ZoneInfo("America/Sao_Paulo")).hour if ZoneInfo else datetime.utcnow().hour
        m = datetime.now(ZoneInfo("America/Sao_Paulo")).minute if ZoneInfo else datetime.utcnow().minute
    except Exception:
        h = datetime.utcnow().hour
        m = datetime.utcnow().minute
    if (h > 6 or (h == 6 and m >= 0)) and (h < 12):
        return "Bom dia"
    if 12 <= h < 18:
        return "Boa tarde"
    return "Boa noite"

def first_name(v: Optional[str]) -> str:
    n = (v or "").strip()
    return n.split()[0].title() if n else ""

def _clip(txt: str) -> str:
    txt = (txt or "").strip()
    if not txt:
        return txt
    sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?|!)\s+', txt)
    clipped = ' '.join(sentences[:2]).strip()
    if clipped and not re.search(r'[.!?]$', clipped):
        clipped += '.'
    return clipped

def scrub_links_if_not_requested(user_text: str, reply: str) -> str:
    if any(k in (user_text or "").lower() for k in ["site", "link", "checkout"]):
        return reply
    return re.sub(r"https?://\S+", "", reply).strip()

def wants_resume(text: str) -> bool:
    t = _normalize(text)
    return any(k in t for k in _normalize(" ".join(CONTINUE_KW)).split())

# ---------------------------------------------------------------------------
# Catálogo
# ---------------------------------------------------------------------------
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
        if not name or not checkout:
            continue

        key = _normalize(name)
        p: Dict[str, Any] = {
            "name": name,
            "checkout": checkout,
            "image": item.get("image", ""),
            "description": item.get("description", ""),
        }

        aliases = {_normalize(name)}
        # cada palavra significativa do nome
        for w in _normalize(name).split():
            if len(w) > 3:
                aliases.add(w)
        # aliases vindos do JSON
        for a in (item.get("aliases") or []):
            aliases.add(_normalize(a))

        # padrões para TabibX e variantes digitadas
        m = re.search(r"(tabib).*(\d+)", name.lower())
        if m:
            n = m.group(2)
            for pat in [
                f"tabib {n}", f"tabib{n}",
                f"tabib v {n}", f"tabib volume {n}",
                f"volume {n}", f"v{n}",
                f"bibi {n}", f"tab b {n}", f"tabb {n}", f"tabb{n}",
                f"tabibit {n}", f"tabibit{n}"
            ]:
                aliases.add(_normalize(pat))

        # exemplo comum: antídoto
        if "antidoto" in _normalize(name):
            aliases.update({"antidoto", "antídoto", "o livro antidoto", "qual é o livro antidoto"})

        p["aliases"] = list(aliases)
        out[key] = p
    logging.info(f"Catálogo carregado: {len(out)} itens de {path}")
    return out

PRODUCTS = load_products(PRODUCTS_JSON_PATH)

def _top_products(n: int = MAX_MENU_ITEMS) -> List[Dict[str, Any]]:
    return list(PRODUCTS.values())[:n]

def build_product_menu(n: int = MAX_MENU_ITEMS) -> List[Dict[str, str]]:
    menu: List[Dict[str, str]] = []
    for p in _top_products(n):
        name = (p.get("name") or "").strip()
        if not name:
            continue
        menu.append({"name": name, "key": _normalize(name)})
    return menu

def find_product_in_text(text: str) -> Optional[Dict[str, Any]]:
    q = _normalize(text)
    if not q:
        return None

    # 1) padrão "tabib 2", "tabb 3" etc.
    m = re.search(r"\b(tabib|tabb|tab b|tabibit)\s*([1-9]\d?)\b", q)
    if m:
        want = m.group(2)
        for p in PRODUCTS.values():
            aliases = set(p.get("aliases", []))
            probe = {
                f"tabib {want}", f"tabib{want}", f"tabb {want}",
                f"tabb{want}", f"tab b {want}", f"tabibit {want}", f"tabibit{want}"
            }
            if aliases & probe:
                return p

    # 2) match de alias contido no texto
    for p in PRODUCTS.values():
        for a in p.get("aliases", []):
            if a and a in q:
                return p

    # 3) fallback simples por sobreposição de tokens do nome
    best = None
    best_score = 0
    q_tokens = set(q.split())
    for p in PRODUCTS.values():
        name_tokens = set(_normalize(p.get("name", "")).split())
        score = len(q_tokens & name_tokens)
        if score > best_score:
            best = p
            best_score = score
    if best_score >= 2:
        return best

    return None

# ---------------------------------------------------------------------------
# Contexto de pedido (CartPanda)
# ---------------------------------------------------------------------------
def cpf_from_text(text: str) -> Optional[str]:
    m = re.compile(r"\d{3}\.?\d{3}\.?\d{3}-?\d{2}").search(text or "")
    return digits_only(m.group(0)) if m else None

def orderno_from_text(text: str) -> Optional[str]:
    m = re.compile(r"\b\d{6,}\b").search(text or "")
    return m.group(0) if m else None

def order_context_by_keys(phone: str, text: str) -> Optional[Dict[str, Any]]:
    # 1) último carrinho por telefone
    token = REDIS.get(f"last_cart_by_phone:{phone}")
    if token:
        ctx_json = REDIS.hget("carts_by_token", token)
        if ctx_json:
            ctx = json.loads(ctx_json)
            if ctx.get("cart_url"):
                ctx["resume_link"] = ctx["cart_url"]
            return ctx

    # 2) último pedido por telefone
    order_no = REDIS.get(f"last_order_by_phone:{phone}")
    if order_no:
        ctx_json = REDIS.hget("orders_by_no", order_no)
        if ctx_json:
            ctx = json.loads(ctx_json)
            if ctx.get("cart_token") and CHECKOUT_RESUME_BASE:
                ctx["resume_link"] = f"{CHECKOUT_RESUME_BASE}{ctx['cart_token']}"
            return ctx

    # 3) nº do pedido no texto
    order_no = orderno_from_text(text or "")
    if order_no:
        ctx_json = REDIS.hget("orders_by_no", order_no)
        if ctx_json:
            ctx = json.loads(ctx_json)
            if ctx.get("cart_token") and CHECKOUT_RESUME_BASE:
                ctx["resume_link"] = f"{CHECKOUT_RESUME_BASE}{ctx['cart_token']}"
            return ctx

    # 4) CPF no texto
    cpf = cpf_from_text(text or "")
    if cpf:
        nos = REDIS.lrange(f"orders_by_cpf:{cpf}", 0, -1)
        if nos:
            ctx_json = REDIS.hget("orders_by_no", nos[-1])
            if ctx_json:
                ctx = json.loads(ctx_json)
                if ctx.get("cart_token") and CHECKOUT_RESUME_BASE:
                    ctx["resume_link"] = f"{CHECKOUT_RESUME_BASE}{ctx['cart_token']}"
                return ctx
    return None

def order_summary(ctx: Optional[Dict[str, Any]]) -> str:
    if not ctx:
        return ""
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

# ---------------------------------------------------------------------------
# Prompt/LLM
# ---------------------------------------------------------------------------
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

def system_prompt(extra_context: Optional[Dict[str, Any]], hints: Optional[Dict[str, Any]] = None) -> str:
    greeting = br_greeting()
    name = first_name(((extra_context or {}).get("customer") or {}).get("name") or (extra_context or {}).get("name"))
    base = SYSTEM_TEMPLATE.format(greeting=greeting, name=name or "", site=SITE_URL)
    if hints and hints.get("product"):
        p = hints["product"]
        base += f" Produto foco: {p.get('name')}. Descrição: {p.get('description','')}. Entrega digital imediata."
    if extra_context and extra_context.get("resume_link"):
        base += f" Use este resume_link quando apropriado: {extra_context['resume_link']}"
    if hints:
        focos = [k for k, v in hints.items() if isinstance(v, bool) and v]
        if focos:
            base += " | FOCO: " + ",".join(focos)
    return base

async def llm_reply(history, ctx, hints):
    msgs = [{"role": "system", "content": system_prompt(ctx, hints)}]
    if ctx:
        msgs.append({"role": "assistant", "content": f"DADOS_DO_PEDIDO: {order_summary(ctx)}"})
    msgs += history[-20:]

    resp = OPENAI.chat.completions.create(
        model=MODEL_NAME,
        temperature=0.2,
        max_tokens=160,
        messages=msgs,
    )
    txt = (resp.choices[0].message.content or "").strip()

    last_user = (history[-1]["content"] if history else "").lower()
    if any(k in last_user for k in ["nao chegou","não chegou","nao recebi","não recebi"]) and not ctx:
        return _clip("Me passa o nº do pedido? Se não tiver, pode ser CPF/CNPJ. Vou verificar no sistema.")
    if any(k in last_user for k in ["acesso","nao consigo","não consigo"]) and not ctx:
        return _clip("Me passa o nº do pedido ou CPF/CNPJ para eu localizar.")
    return _clip(txt)

# ---------------------------------------------------------------------------
# Envio Z-API
# ---------------------------------------------------------------------------
async def zapi_send_text(phone: str, message: str) -> dict:
    if DRY_RUN:
        logging.info(f"[DRY_RUN] -> {phone}: {message}")
        return {"ok": True, "dry_run": True}

    url = f"https://api.z-api.io/instances/{ZAPI_INSTANCE}/token/{ZAPI_TOKEN}{SEND_TEXT_PATH}"
    payload = {"phone": phone, "message": message}
    headers = {"Client-Token": ZAPI_CLIENT_TOKEN, "Content-Type": "application/json"}

    async with httpx.AsyncClient(timeout=20) as http:
        r = await http.post(url, headers=headers, json=payload)
        try:
            data = r.json()
        except Exception:
            data = {"text": r.text}
        if r.status_code >= 300:
            logging.error(f"Z-API {r.status_code}: {data}")
            return {"ok": False, "status": r.status_code, "error": data}
        return {"ok": True, "status": r.status_code, "data": data}

# ---------------------------------------------------------------------------
# FastAPI Endpoints
# ---------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"ok": True}

@app.post("/webhook/zapi/receive")
async def zapi_receive(request: Request):
    data = await request.json()
    logging.info(f"Webhook Z-API: {data}")

    # mensagem e id podem vir em formatos diferentes
    msg = data.get("message") or data.get("body") or data.get("text") or ""
    msg_id = data.get("messageId") or data.get("id")
    if isinstance(msg, dict):
        msg_id = msg.get("id") or msg_id
        msg = msg.get("text") or msg.get("body") or msg.get("message") or ""

    phone = normalize_phone(data.get("phone") or (data.get("sender") or {}).get("phone") or "")
    if not phone or not msg:
        return JSONResponse({"status": "ignored", "reason": "missing phone or text"})

    # Idempotência
    if msg_id:
        if REDIS.sismember("seen_ids", msg_id):
            return JSONResponse({"status": "duplicate"})
        REDIS.sadd("seen_ids", msg_id)

    # Retomar checkout rapidamente
    if wants_resume(msg):
        ctx = order_context_by_keys(phone, msg)
        if ctx and (ctx.get("resume_link") or ctx.get("cart_url") or ctx.get("checkout_url")):
            link = ctx.get("resume_link") or ctx.get("cart_url") or ctx.get("checkout_url")
            send = await zapi_send_text(phone, f"Perfeito. Seu checkout: {link}")
            return JSONResponse({"status": "sent", "route": "resume", "send": send})
        send = await zapi_send_text(phone, "Me envia nº do pedido ou CPF para puxar seu checkout.")
        return JSONResponse({"status": "need_id", "send": send})

    # Rotas rápidas (intenções)
    t = _normalize(msg)
    def _has(*xs): return any(x in t for x in xs)

    if _has("entrega","frete","chega","prazo","rastreio","rastreamento","correios","transportadora","endereco","endereço"):
        send = await zapi_send_text(phone, "É digital. Você recebe por e-mail/WhatsApp após o pagamento. Quer ajuda para finalizar?")
        return JSONResponse({"status": "sent", "route": "quick", "send": send})

    if _has("nao chegou","não chegou","nao recebi","não recebi","email","e mail","e-mail"):
        send = await zapi_send_text(phone, "Me envia o nº do pedido ou CPF para eu checar.")
        return JSONResponse({"status": "sent", "route": "quick", "send": send})

    if _has("seguran","golpe","fraude","medo"):
        send = await zapi_send_text(phone, "Checkout seguro com HTTPS e PSP oficial. Não pedimos senhas.")
        return JSONResponse({"status": "sent", "route": "quick", "send": send})

    if _has("nao consegui pagar","não consegui pagar","pagamento","pix","boleto","cartao","cartão","checkout"):
        send = await zapi_send_text(phone, "Em que etapa o pagamento travou? Posso ajudar a finalizar.")
        return JSONResponse({"status": "sent", "route": "quick", "send": send})

    if _has("instagram","comentar","seguir","post"):
        send = await zapi_send_text(phone, "Siga @Paginatto e comente em 3 posts para o bônus! Qual seu @?")
        return JSONResponse({"status": "sent", "route": "quick", "send": send})

    if _has("suporte","ajuda","atendimento","catalogo","catálogo","produto","produtos","selecionar produto","escolher produto"):
        menu = build_product_menu(MAX_MENU_ITEMS)
        REDIS.set(f"menu:{phone}", json.dumps(menu))
        linhas = [f"{i+1}) {item['name']}" for i, item in enumerate(menu)]
        text = "Posso te ajudar com um produto. Digite o nome (ex.: 'antidoto') ou um número:\n" + "\n".join(linhas)
        send = await zapi_send_text(phone, text)
        return JSONResponse({"status":"sent","route":"menu","send":send})
    
    # compra já realizada

    if _has("ja comprei","já comprei","acabei de comprar","comprei","ja paguei","já paguei","paguei","efetuei o pagamento",
    "ja realizei a compra","já realizei a compra","realizei a compra",
    "ja fiz o pedido","já fiz o pedido","fiz o pedido","pedido feito"):
    send = await zapi_send_text(phone, "Parabéns pela sua compra. Confirme para mim se chegou tudo certinho no seu e-mail. Qualquer dúvida, estou à disposição.")
    return JSONResponse({"status": "sent", "route": "purchase_done", "send": send})

    
    # Seleção por número do menu (1..N)
    if re.fullmatch(r"\d{1,2}", t or ""):
        idx = int(t) - 1
        menu_raw = REDIS.get(f"menu:{phone}")
        if menu_raw:
            menu = json.loads(menu_raw)
            if 0 <= idx < len(menu):
                sel_key = menu[idx]["key"]
                prod = PRODUCTS.get(sel_key)
                if not prod:
                    # fallback: busca por nome normalizado
                    for p in PRODUCTS.values():
                        if _normalize(p.get("name", "")) == sel_key:
                            prod = p
                            break
                if prod:
                    reply = f"{prod['name']}\n{_clip(prod['description'])}\nCheckout: {prod['checkout']}\n{DELIVERY_ONE_LINER}"
                    send = await zapi_send_text(phone, reply)
                    return JSONResponse({"status":"sent","route":"product_select","product":prod["name"],"send":send})

    # Produto citado no texto (ex.: “qual é o livro antídoto”, “quero antídoto”)
    prod = find_product_in_text(msg)
    if prod:
        reply = f"{prod['name']}\n{_clip(prod['description'])}\nCheckout: {prod['checkout']}\n{DELIVERY_ONE_LINER}"
        send = await zapi_send_text(phone, reply)
        return JSONResponse({"status": "sent", "route": "product", "product": prod["name"], "send": send})

    # LLM com fallback
    history_key = f"sessions:{phone}"
    history = json.loads(REDIS.get(history_key) or "[]")
    history.append({"role": "user", "content": msg})

    try:
        ctx = order_context_by_keys(phone, msg)
        ai = await llm_reply(history, ctx, hints={})
    except Exception:
        logging.exception("LLM error")
        ai = f"{br_greeting()}! Como posso ajudar?"

    # saudação só na 1ª interação e se texto for saudação
    if len(history) <= 1 and _normalize(msg) in {"oi","ola","olá","bom dia","boa tarde","boa noite","oii","oie"}:
        ai = f"{br_greeting()}! Como posso ajudar?"

    ai = _clip(scrub_links_if_not_requested(msg, ai))
    send = await zapi_send_text(phone, ai)
    if not send.get("ok"):
        return JSONResponse({"status": "error", "route": "llm", "send": send}, status_code=502)

    history.append({"role": "assistant", "content": ai})
    REDIS.set(history_key, json.dumps(history))
    return JSONResponse({"status":"sent","route":"llm","send":send})

@app.post("/webhook/zapi/status")
async def zapi_status(request: Request):
    data = await request.json()
    logging.info(f"Webhook Z-API status: {data}")
    return {"ok": True}

# ---------------------------------------------------------------------------
# Webhooks CartPanda
# ---------------------------------------------------------------------------
@app.post("/webhook/cartpanda/order")
async def cartpanda_order(request: Request):
    data = await request.json()
    logging.info(f"Webhook CartPanda: {data}")

    # 1) lista de abandonados
    carts = (data.get("abandoned_carts") or {}).get("data")
    if isinstance(carts, list):
        for c in carts:
            cart_token = (c.get("cart_token") or "").strip()
            phone = normalize_phone(((c.get("customer") or {}).get("phone")) or "")
            if not phone or not cart_token:
                continue
            cart_url = c.get("cart_url") or ""
            order_data = {
                "cart_token": cart_token,
                "cart_url": cart_url,
                "customer": c.get("customer") or {},
                "cart_total": c.get("cart_total"),
                "currency": c.get("currency"),
                "products": c.get("cart_line_items") or [],
            }
            REDIS.set(f"last_cart_by_phone:{phone}", cart_token)
            REDIS.hset("carts_by_token", cart_token, json.dumps(order_data))
        return JSONResponse({"ok": True, "mode": "abandoned_list", "count": len(carts)})

    # 2) evento único
    d = data.get("data") or data
    customer = d.get("customer") or {}
    phone = normalize_phone(customer.get("phone") or d.get("phone") or "")
    cart_token = (d.get("cart_token") or d.get("cartToken") or "").strip()
    cart_url = d.get("cart_url") or d.get("checkout_url") or ""
    order_no = str(d.get("order_no") or d.get("number") or d.get("id") or d.get("orderNumber") or "").strip()

    if not phone and not cart_token and not order_no:
        return JSONResponse({"ok": False, "reason": "missing identifiers"}, status_code=400)

    order_data = {
        "order_no": order_no or None,
        "cart_token": cart_token or None,
        "cart_url": cart_url or None,
        "customer": {
            "name": customer.get("name") or d.get("name") or "",
            "email": (customer.get("email") or d.get("email") or "").strip().lower(),
            "phone": phone,
            "document": digits_only(customer.get("document") or d.get("document") or d.get("cpf") or ""),
        },
    }

    if phone and cart_token:
        REDIS.set(f"last_cart_by_phone:{phone}", cart_token)
        REDIS.hset("carts_by_token", cart_token, json.dumps(order_data))

    if order_no:
        REDIS.hset("orders_by_no", order_no, json.dumps(order_data))
        if phone:
            REDIS.set(f"last_order_by_phone:{phone}", order_no)

    return JSONResponse({"ok": True, "mode": "single", "has_token": bool(cart_token), "has_order": bool(order_no)})

@app.post("/webhook/cartpanda/support")
async def cartpanda_support(request: Request):
    data = await request.json()
    logging.info(f"Webhook CartPanda suporte: {data}")
    d = data.get("data") or data
    order_no = str(d.get("order_no") or d.get("number") or d.get("id") or "").strip()
    phone = normalize_phone((d.get("customer") or {}).get("phone") or d.get("phone") or "")

    if phone and order_no:
        REDIS.set(f"last_order_by_phone:{phone}", order_no)
    if order_no:
        ctx_json = REDIS.hget("orders_by_no", order_no)
        if not ctx_json:
            REDIS.hset("orders_by_no", order_no, json.dumps({
                "order_no": order_no,
                "customer": d.get("customer") or {}
            }))

    return {"ok": True, "linked_phone": phone, "order_no": order_no}

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
