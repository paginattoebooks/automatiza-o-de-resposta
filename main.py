"""
Paginatto — Iara WhatsApp Bot
Z-API + ChatGPT + CartPanda + Catálogo JSON
Python 3.11+

Requisitos no .env (Render → Environment):
  OPENAI_API_KEY=
  ASSISTANT_NAME=Iara
  BRAND_NAME=Paginatto
  SITE_URL=https://paginattoebooks.github.io/Paginatto.site.com.br/
  SUPPORT_URL=https://paginattoebooks.github.io/Paginatto.site.com.br/
  CNPJ=57.941.903/0001-94
  SECURITY_BLURB=Checkout com HTTPS, PSP oficial. Não pedimos senhas/códigos.
  CHECKOUT_RESUME_BASE=https://seu-checkout.cartpanda.com/resume/

  ZAPI_INSTANCE=3E2D08AA912D5063906206E9A5181015
  ZAPI_TOKEN=45351C39E4EDCB47C2466177
  ZAPI_CLIENT_TOKEN=F8d6942e55c57407e95c2ceae481f6a92S
  SEND_TEXT_PATH=/send-text

  MAX_DISCOUNT_PCT=10
  INSTAGRAM_HANDLE=@Paginatto
  INSTAGRAM_URL=https://instagram.com/Paginatto

  # Catálogo
  PRODUCTS_JSON_PATH=produtos_paginatto.json
"""

import os
import re
import json
import unicodedata
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import httpx

import json, os, re, unicodedata

PRODUCTS_JSON_PATH = os.getenv("PRODUCTS_JSON_PATH", "produtos_paginatto.json")

def _norm(s: str) -> str:
    s = (s or "").lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", " ", s).strip()

def _make_aliases(name: str) -> set[str]:
    a = {_norm(name)}
    m = re.search(r"(tabib).*?(?:volume|vol)?\s*(\d+)", name.lower())
    if m:
        n = m.group(2)
        a |= {_norm(x) for x in [
            f"tabib {n}", f"tabib vol {n}", f"tabib volume {n}",
            f"tabib{n}", f"volume {n}", f"v{n}", f"bibi {n}"
        ]}
    return a

def load_products(path: str) -> list[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            items = json.load(f)
    except Exception:
        items = []
    prods = []
    for it in items:
        name = (it.get("name") or "").strip()
        checkout = (it.get("checkout") or "").strip()
        if not name or not checkout:
            continue
        prods.append({
            "name": name,
            "checkout": checkout,
            "aliases": list(_make_aliases(name)),
        })
    return prods

PRODUCTS = load_products(PRODUCTS_JSON_PATH)

def find_product_in_text(text: str) -> dict | None:
    q = _norm(text)
    for p in PRODUCTS:
        for a in p["aliases"]:
            if a and a in q:
                return p
    # fallback: nome parcialmente contido
    for p in PRODUCTS:
        if _norm(p["name"]) in q or any(w in q for w in _norm(p["name"]).split()):
            return p
    return None

# --- Catálogo embutido (sem arquivo externo) ---
PRODUCTS = {
    "tabib1": {"name": "Tabib Volume 1: Tratamento de Dores e Inflamações",
               "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919679:1"},
    "tabib2": {"name": "Tabib Volume 2: Saúde Respiratória e Imunidade",
               "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919682:1"},
    "tabib3": {"name": "Tabib Volume 3: Saúde Digestiva e Metabólica",
               "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919686:1"},
    "tabib4": {"name": "Tabib Volume 4: Saúde Mental e Energética",
               "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919707:1"},
    "tabib_full": {"name": "Tabib completo",
                   "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/184229277:1"},
    "tabib_2025_combo": {"name": "Tabib 2025 + Bônus 19,90 + Tabib 2024",
                         "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/184229263:1"},
    "antidoto": {"name": "Antídoto - Antídotos indígenas",
                 "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919637:1"},
    "kurima": {"name": "Kurimã - Óleos essenciais",
               "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919661:1"},
    "balsamo": {"name": "Bálsamo - Pomadas naturais",
                "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919668:1"},
    "pressao_alta": {"name": "Tratamento Natural Personalizado para Pressão Alta",
                     "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/174502432:1"},
    "airfryer": {"name": "300 receitas para AirFryer",
                 "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/176702038:1"},
}

# aliases para achar pelo texto (inclui “bibi”)
PROD_ALIASES = {
    "tabib1": ["tabib 1", "tabib i", "volume 1", "v1", "bibi 1"],
    "tabib2": ["tabib 2", "tabib ii", "volume 2", "v2", "bibi 2"],
    "tabib3": ["tabib 3", "tabib iii", "volume 3", "v3", "bibi 3"],
    "tabib4": ["tabib 4", "tabib iv", "volume 4", "v4", "bibi 4", "bibi volume 4", "tabib volume 4"],
    "tabib_full": ["tabib completo", "coleção tabib", "combo tabib"],
    "tabib_2025_combo": ["tabib 2025", "bônus 19,90", "tabib 2024", "combo 2025"],
    "antidoto": ["antidoto", "antídoto"],
    "kurima": ["kurima", "oleos essenciais", "óleos essenciais"],
    "balsamo": ["balsamo", "bálsamo", "pomadas naturais"],
    "pressao_alta": ["pressao alta", "pressão alta", "tratamento pressão"],
    "airfryer": ["airfryer", "air fryer", "receitas airfryer", "300 receitas"],
}


from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# OpenAI SDK
from openai import OpenAI

load_dotenv()

# --- Config ---
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
ASSISTANT_NAME = os.getenv("ASSISTANT_NAME", "Iara")
BRAND_NAME = os.getenv("BRAND_NAME", "Paginatto")
SITE_URL = os.getenv("SITE_URL", "https://paginattoebooks.github.io/Paginatto.site.com.br/")
SUPPORT_URL = os.getenv("SUPPORT_URL", SITE_URL)
CNPJ = os.getenv("CNPJ", "57.941.903/0001-94")

SECURITY_BLURB = os.getenv(
    "SECURITY_BLURB",
    "Checkout com HTTPS e PSP oficial. Não pedimos senhas ou códigos."
)

DELIVERY_ONE_LINER = (
  "Entrega 100% digital. Enviamos/liberamos o e-book por e-mail e WhatsApp após o pagamento. "
  "Não pedimos endereço e não existe rastreio."
)

CHECKOUT_RESUME_BASE = os.getenv("CHECKOUT_RESUME_BASE", "")

ZAPI_INSTANCE = os.getenv("ZAPI_INSTANCE", "")
ZAPI_TOKEN = os.getenv("ZAPI_TOKEN", "")
ZAPI_CLIENT_TOKEN = os.getenv("ZAPI_CLIENT_TOKEN", "")
SEND_TEXT_PATH = os.getenv("SEND_TEXT_PATH", "/send-text")

MAX_DISCOUNT_PCT = int(os.getenv("MAX_DISCOUNT_PCT", "10"))
INSTAGRAM_HANDLE = os.getenv("INSTAGRAM_HANDLE", "@Paginatto")
INSTAGRAM_URL = os.getenv("INSTAGRAM_URL", "https://instagram.com/Paginatto")

CARTPANDA_API_BASE = os.getenv("CARTPANDA_API_BASE", "")
CARTPANDA_API_TOKEN = os.getenv("CARTPANDA_API_TOKEN", "")

PRODUCTS_JSON_PATH = os.getenv("PRODUCTS_JSON_PATH", "produtos_paginatto.json")

if not (OPENAI_API_KEY and ZAPI_INSTANCE and ZAPI_TOKEN and ZAPI_CLIENT_TOKEN):
    raise RuntimeError("Defina OPENAI_API_KEY, ZAPI_INSTANCE, ZAPI_TOKEN, ZAPI_CLIENT_TOKEN no .env")

OPENAI = OpenAI(api_key=OPENAI_API_KEY)

app = FastAPI(title="Paginatto — Iara Bot")

# --- Stores (trocar por Redis em produção) ---
SESSIONS: Dict[str, List[Dict[str, str]]] = {}
SEEN_IDS: set[str] = set()

ORDERS_BY_NO: Dict[str, Dict[str, Any]] = {}
ORDERS_BY_CPF: Dict[str, List[str]] = {}
ORDERS_BY_EMAIL: Dict[str, List[str]] = {}
LAST_ORDER_BY_PHONE: Dict[str, str] = {}

ZAPI_BASE = f"https://api.z-api.io/instances/{ZAPI_INSTANCE}/token/{ZAPI_TOKEN}"
ZAPI_HEADERS = {"Client-Token": ZAPI_CLIENT_TOKEN, "Content-Type": "application/json"}

import re

def _norm(t: str) -> str:
    t = (t or "").lower()
    t = t.replace("ó", "o").replace("ô","o").replace("õ","o").replace("á","a").replace("à","a").replace("ã","a") \
         .replace("é","e").replace("ê","e").replace("í","i").replace("ú","u").replace("ç","c")
    return re.sub(r"[^a-z0-9 ]+", " ", t)

def find_product_in_text(text: str):
    t = _norm(text)
    # prioridade “tabib” + número
    if "tabib" in t or "bibi" in t:
        if " 4" in f" {t} " or " iv" in f" {t} " or " volume 4" in t or " v4" in t:
            return PRODUCTS["tabib4"]
        if " 3" in f" {t} " or " iii" in f" {t} " or " volume 3" in t or " v3" in t:
            return PRODUCTS["tabib3"]
        if " 2" in f" {t} " or " ii" in f" {t} " or " volume 2" in t or " v2" in t:
            return PRODUCTS["tabib2"]
        if " 1" in f" {t} " or " i" in f" {t} " or " volume 1" in t or " v1" in t:
            return PRODUCTS["tabib1"]
        if "completo" in t or "colecao" in t or "combo" in t:
            return PRODUCTS["tabib_full"]

    # varre aliases gerais
    for pid, aliases in PROD_ALIASES.items():
        for a in aliases:
            a_norm = _norm(a)
            if a_norm and a_norm in t:
                return PRODUCTS[pid]
    return None


def digits_only(v: str) -> str:
    return re.sub(r"\D+", "", v or "")


def normalize_phone(v: str) -> str:
    d = digits_only(v)
    if d and not d.startswith("55"):
        d = "55" + d
    return d


def parse_bool(x: Any) -> bool:
    return str(x).lower() in {"1", "true", "yes"}


def _normalize(s: str) -> str:
    s = (s or "").lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def load_products(path: str) -> Dict[str, Dict[str, str]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}
    out: Dict[str, Dict[str, str]] = {}
    for item in data:
        name = (item.get("name") or "").strip()
        if not name:
            continue
        key = _normalize(name)
        p = {
            "name": name,
            "checkout": item.get("checkout") or "",
            "image": item.get("image") or "",
        }
        aliases = {_normalize(name)}
        m = re.search(r"(tabib).*(\d+)", name.lower())
        if m:
            vol = m.group(2)
            for pat in [f"tabib volume {vol}", f"tabib v {vol}", f"tabib {vol}"]:
                aliases.add(_normalize(pat))
        p["aliases"] = list(aliases)
        out[key] = p
    return out

def _normalize(s: str) -> str:
    s = (s or "").lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", " ", s).strip()

def match_product(text: str) -> Optional[Dict[str, str]]:
    q = _normalize(text)
    # 1) prioridade: "tabib" + número
    if "tabib" in q or "bibi" in q:
        for n, key in ((" 4", "tabib4"), (" 3", "tabib3"), (" 2", "tabib2"), (" 1", "tabib1")):
            if n in f" {q} ":
                return PRODUCTS.get(key)
    # 2) varre aliases
    for pid, aliases in PROD_ALIASES.items():
        for a in aliases:
            if _normalize(a) in q:
                return PRODUCTS.get(pid)
    # 3) fallback: trecho do nome
    for _, p in PRODUCTS.items():
        if _normalize(p.get("name","")) and any(w for w in q.split() if w and w in _normalize(p["name"])):
            return p
    return None


PRODUCTS = load_products(PRODUCTS_JSON_PATH)


def match_product(text: str) -> Optional[Dict[str, str]]:
    q = _normalize(text)
    for _, p in PRODUCTS.items():
        for a in p.get("aliases", []):
            if a and a in q:
                return p
    return None


def wants_site(text: str) -> bool:
    t = (text or "").lower()
    return ("site" in t) or ("paginatto" in t and "site" in t)


def br_greeting() -> str:
    try:
        h = datetime.now(ZoneInfo("America/Sao_Paulo")).hour if ZoneInfo else datetime.utcnow().hour
    except Exception:
        h = datetime.utcnow().hour
    if 5 <= h < 12:
        return "Bom dia"
    if 12 <= h < 18:
        return "Boa tarde"
    return "Boa noite"

def first_name(v: Optional[str]) -> str:
    n = (v or "").strip()
    return n.split()[0].title() if n else ""


def analyze_intent(text: str) -> dict:
    t = (text or "").lower()
    has = lambda *xs: any(x in t for x in xs)
    return {
        "ask_why_desist": has("desisti", "desist"),
        "low_balance": has("sem saldo", "falta de saldo", "sem limite", "cartao sem limite", "saldo"),
        "security": has("seguran", "golpe", "fraude", "medo"),
        "not_received_email": has("nao chegou", "não chegou", "nao recebi", "não recebi", "email", "e-mail"),
        "thinks_physical": has("livro fisico", "livro físico", "fisico"),
        "payment_problem": has("nao consegui pagar", "não consegui pagar", "pagamento", "pix", "boleto", "cartao", "cartão", "checkout"),
        "instagram_bonus": has("instagram", "comentar", "seguir", "post"),
        "support": has("suporte", "ajuda", "atendimento"),
        "delivery_question": has("entrega", "como recebo", "como chega", "onde chega", "forma de entrega", "prazo de entrega"),
        "tracking_request": has("codigo de rastreio", "código de rastreio", "rastreamento", "rastreio"),
        "delivery": has("entrega","prazo","quando chega","chega quando","rastreio","rastreamento",
        "código de rastreio","frete","transportadora","correios","cep","endereço","endereco"),
    

    }


def cpf_from_text(text: str) -> Optional[str]:
    m = CPF_RX.search(text or "")
    return digits_only(m.group(1)) if m else None


def orderno_from_text(text: str) -> Optional[str]:
    m = ORDER_RX.search(text or "")
    return m.group(1) if m else None


def order_context_by_keys(phone: str, text: str) -> Optional[Dict[str, Any]]:
    order_no = LAST_ORDER_BY_PHONE.get(phone)
    if order_no and order_no in ORDERS_BY_NO:
        ctx = ORDERS_BY_NO[order_no]
        if ctx.get("cart_token") and CHECKOUT_RESUME_BASE:
            ctx["resume_link"] = f"{CHECKOUT_RESUME_BASE}{ctx['cart_token']}"
        return ctx

    order_no = orderno_from_text(text or "")
    if order_no and order_no in ORDERS_BY_NO:
        ctx = ORDERS_BY_NO[order_no]
        if ctx.get("cart_token") and CHECKOUT_RESUME_BASE:
            ctx["resume_link"] = f"{CHECKOUT_RESUME_BASE}{ctx['cart_token']}"
        return ctx

    cpf = cpf_from_text(text or "")
    if cpf and cpf in ORDERS_BY_CPF:
        nos = ORDERS_BY_CPF[cpf]
        if nos:
            ctx = ORDERS_BY_NO.get(nos[-1])
            if ctx and ctx.get("cart_token") and CHECKOUT_RESUME_BASE:
                ctx["resume_link"] = f"{CHECKOUT_RESUME_BASE}{ctx['cart_token']}"
            return ctx

    return None


async def cartpanda_lookup(order_no: Optional[str] = None, cpf: Optional[str] = None, email: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if not (CARTPANDA_API_BASE and CARTPANDA_API_TOKEN):
        return None
    headers = {"Authorization": f"Bearer {CARTPANDA_API_TOKEN}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=20) as http:
        try:
            if order_no:
                url = f"{CARTPANDA_API_BASE}/orders/{order_no}"
                r = await http.get(url, headers=headers)
                if r.status_code < 300:
                    return r.json()
            elif cpf:
                url = f"{CARTPANDA_API_BASE}/orders?cpf={cpf}"
                r = await http.get(url, headers=headers)
                if r.status_code < 300:
                    data = r.json()
                    if isinstance(data, list) and data:
                        return data[-1]
            elif email:
                url = f"{CARTPANDA_API_BASE}/orders?email={email}"
                r = await http.get(url, headers=headers)
                if r.status_code < 300:
                    data = r.json()
                    if isinstance(data, list) and data:
                        return data[-1]
        except Exception:
            return None
    return None


def compact_order_view(o: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "order_no": o.get("order_no") or o.get("number") or o.get("id"),
        "name": (o.get("customer") or {}).get("name") or o.get("name"),
        "email": (o.get("customer") or {}).get("email") or o.get("email"),
        "phone": (o.get("customer") or {}).get("phone") or o.get("phone"),
        "cpf": digits_only(((o.get("customer") or {}).get("document")) or o.get("cpf") or ""),
        "payment_status": o.get("payment_status") or o.get("status"),
        "checkout_url": o.get("checkout_url"),
        "cart_token": o.get("cart_token"),
        "total": o.get("total") or o.get("amount")
    }


def order_summary(ctx: Optional[Dict[str, Any]]) -> str:
    if not ctx:
        return ""
    parts = []
    if ctx.get("order_no"): parts.append(f"Pedido: {ctx['order_no']}")
    if ctx.get("payment_status"): parts.append(f"Pagamento: {ctx['payment_status']}")
    if ctx.get("customer", {}).get("name"): parts.append(f"Cliente: {ctx['customer']['name']}")
    if ctx.get("customer", {}).get("email"): parts.append(f"E-mail: {ctx['customer']['email']}")
    if ctx.get("customer", {}).get("document"): parts.append(f"CPF: {ctx['customer']['document']}")
    if ctx.get("checkout_url"): parts.append(f"Checkout: {ctx['checkout_url']}")
    if ctx.get("resume_link"): parts.append(f"Retomar: {ctx['resume_link']}")
    return " | ".join(parts)


SYSTEM_TEMPLATE = (
  # estilo
  "Saudação curta: '{greeting}, {name}! Como posso ajudar?' (sem nome: '{greeting}! Como posso ajudar?'). "
  "Respostas curtas: 1–2 frases. Sem textão. "
  "Se pedirem produto específico → responda só com checkout direto."
  "Se não pedirem link/site, não envie link algum."
  "Entrega 100% digital. Nunca fale de endereço/frete/correios/rastreio."


  # política de entrega (regra dura)
  "Produto e entrega: 100% DIGITAL (e-book). Nunca fale de endereço, frete, correios, transportadora ou rastreio. "
  "Se perguntarem por entrega, endereço, prazo, frete ou rastreio → responda apenas que é digital e enviada/ liberada "
  "por e-mail/WhatsApp após pagamento, e ofereça checar status. "
  "ENTREGA: 100% digital (ebook). NUNCA fale de frete, endereço, rastreio, Correios, transportadora.",
  "Se perguntarem sobre entrega: responda curto → 'É digital. Você recebe por e-mail/área do pedido. Posso checar pelo nº do pedido ou CPF?'.",
  "Cumprimento curto: 'bom dia/boa tarde, como posso ajudar?'. Nada de textão.",
  "Se não pedirem link/site, não envie link algum.",
  "Se perguntarem se chega na casa: diga que NÃO, pois é um ebook virtual.",


  # desistência/segurança/e-mail
  "Se desisti → pergunte o motivo. "
  "Se segurança → diga que o checkout é HTTPS/PSP oficial; se e somente se pedirem site, ofereça {insta}."
  "Se não recebeu por e-mail → peça nº do pedido ou CPF/CNPJ para verificar; ofereça reenvio pelo e-mail, pergunte o e-mail cadastrado. "

  # físico
  "Se achou que era físico → avise que é e-book digital e cite benefícios. "

  # pagamento travou
  "Se pagamento travou → pergunte em que etapa e ofereça ajuda para finalizar o pagamento. "

  # bônus instagram
  "Se citar Instagram/engajamento → ofereça bônus após seguir e comentar 3 posts; peça @ para validar. "

  # nunca
  "Nunca peça senhas/códigos. Nunca prometa alterar preço automaticamente. "

  NAO_CHEGOU = "Consigo verificar já. Me envia o nº do pedido ou CPF/CNPJ para eu checar aqui em nosso sistema?"

)



def system_prompt(extra_context: Optional[Dict[str, Any]], hints: Optional[Dict[str, bool]] = None) -> str:
    greeting = br_greeting()
    name = first_name(((extra_context or {}).get("customer") or {}).get("name") or (extra_context or {}).get("name"))
    base = SYSTEM_TEMPLATE.format(
        greeting=greeting,
        name=name or "",
        maxdisc=MAX_DISCOUNT_PCT,
        site=SITE_URL,
    )
    # Dados do produto foco
    if hints and hints.get("product_id") and hints.get("product"):
        p = hints["product"]
        base += f" Produto foco: {p.get('name')}. Entrega digital imediata."
    # Link de retomada
    if extra_context and extra_context.get("resume_link"):
        base += f" Use este resume_link quando apropriado: {extra_context['resume_link']}"
    # Pistas explícitas
    if hints:
        focos = [k for k, v in hints.items() if isinstance(v, bool) and v]
        if focos:
            base += " | FOCO: " + ",".join(focos)
    return base

from typing import Optional

def quick_routes(t: str) -> Optional[str]:
    tl = (t or "").lower()
    entrega_kw = [
        "entrega", "entregam", "envio", "enviam", "frete", "chega", "chegar",
        "chegou", "prazo", "rastreio", "rastreamento", "código de rastreio",
        "endereco", "endereço"
    ]
    if any(k in tl for k in entrega_kw):
        return (
            "Nosso produto é 100% digital. Você recebe o acesso por e-mail e WhatsApp logo após o pagamento. "
            "Não há entrega física nem código de rastreio. Quer ajuda para finalizar ou recuperar seu pedido?"
        )
    return None


async def llm_reply(history: List[Dict[str, str]], ctx: Optional[Dict[str, Any]], hints: Optional[Dict[str, bool]]) -> str:
    msgs = [{"role": "system", "content": system_prompt(ctx, hints)}]
    if ctx:
        msgs.append({"role": "assistant", "content": f"DADOS_DO_PEDIDO: {order_summary(ctx)}"})
    msgs += history[-30:]

    resp = OPENAI.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.3,
        messages=msgs,
    )
    txt = resp.choices[0].message.content.strip()

    # Fallback curto para falta de contexto em "não chegou"
    if not ctx and any(x in txt.lower() for x in ["nao chegou", "não chegou", "nao recebi", "não recebi", "acesso", "nao consigo", "não consigo"]):
        return "Me passa o nº do pedido? Se não tiver, pode ser CPF/CNPJ. Vou verificar no sistema."
    # Fallback geral para 'não tenho acesso'
    if not ctx and ("acesso" in txt.lower() or "nao consigo" in txt.lower() or "não consigo" in txt.lower()):
        return "Me passa o nº do pedido ou CPF/CNPJ para eu localizar. Posso falar com o time humano se preferir."
    return txt

    SITE_DOMS = ("paginattoebooks.github.io", "paginatto.site", "paginatto.site.com.br")

def scrub_links_if_not_requested(user_text: str, reply: str) -> str:
    if "site" in user_text.lower():
        return reply
    for d in SITE_DOMS:
        reply = re.sub(rf"https?://[^\s]*{re.escape(d)}[^\s]*", "", reply)
    return reply.strip()



# --- Webhooks ---
@app.get("/health")
async def health():
    return {"ok": True}


async def zapi_send_text(phone: str, message: str) -> dict:
    url = f"{ZAPI_BASE}{SEND_TEXT_PATH}"
    payload = {"phone": phone, "message": message}
    async with httpx.AsyncClient(timeout=20) as http:
        r = await http.post(url, headers=ZAPI_HEADERS, json=payload)
        data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {"text": r.text}
        if r.status_code >= 300:
            raise HTTPException(status_code=502, detail={"zapi_error": data})
        return data


@app.post("/webhook/zapi/receive")
async def zapi_receive(request: Request):
    data = await request.json()

    # Extrair telefone e texto da mensagem
    phone = (
        data.get("phone") or data.get("from") or data.get("chatId")
        or (data.get("contact", {}) or {}).get("phone")
        or (data.get("message", {}) or {}).get("from")
        or (data.get("messages", [{}])[0] or {}).get("from")
        or ""
    )
    text = (
        data.get("text") or data.get("body")
        or (data.get("message") if isinstance(data.get("message"), str) else None)
        or (data.get("message", {}) or {}).get("text")
        or (data.get("messages", [{}])[0] or {}).get("text")
        or (data.get("messages", [{}])[0] or {}).get("body")
        or ""
    )

    phone = normalize_phone(str(phone))
    text = str(text).strip()
    if not phone or not text:
        return JSONResponse({"status": "ignored", "reason": "missing phone or text"})

    # Verifica se a mensagem já foi processada (evita duplicados)
    msg_id = (
        data.get("messageId") or data.get("id") or data.get("message", {}).get("id") or (data.get("messages", [{}])[0] or {}).get("id")
    )
    if msg_id and msg_id in SEEN_IDS:
        return JSONResponse({"status": "duplicate_ignored"})
    if msg_id:
        SEEN_IDS.add(msg_id)

    # Busca produto no texto
   ```
prod = find_product_in_text(text)
if prod:
    price = f" – {prod['price']}" if prod.get("price") else ""
    blurb = f"\n{prod['blurb']}" if prod.get("blurb") else ""
    reply = f"{prod["name"]}{price}\nCheckout: {prod["checkout"]}{blurb}"
    await zapi_send_text(phone, reply)
    return JSONResponse({"status":"ok","routed":"product_checkout"})
```
    # ... resto do código do webhook ...


@app.post("/webhook/zapi/status")
async def zapi_status(request: Request):
    _ = await request.json()
    return {"ok": True}


@app.post("/webhook/cartpanda/order")
async def cartpanda_order(request: Request):
    data = await request.json()
    d = data.get("data") or data

    order_no = str(d.get("order_no") or d.get("number") or d.get("id") or d.get("orderNumber") or "").strip()
    customer = d.get("customer") or {}
    email = (customer.get("email") or d.get("email") or "").strip().lower()
    phone = normalize_phone(customer.get("phone") or d.get("phone") or "")
    cpf = digits_only(customer.get("document") or d.get("document") or d.get("cpf") or "")
    payment_status = (d.get("payment_status") or d.get("status") or "").strip().lower()
    cart_token = d.get("cart_token") or d.get("cartToken") or ""
    checkout_url = d.get("checkout_url") or d.get("checkoutUrl") or ""

    if not order_no:
        return JSONResponse({"indexed": False, "reason": "missing order_no"})

    ctx = {
        "order_no": order_no,
        "payment_status": payment_status,
        "checkout_url": checkout_url,
        "cart_token": cart_token,
        "customer": {
            "name": customer.get("name") or d.get("name") or "",
            "email": email,
            "phone": phone,
            "document": cpf,
        },
    }
    if cart_token and CHECKOUT_RESUME_BASE:
        ctx["resume_link"] = f"{CHECKOUT_RESUME_BASE}{cart_token}"

    ORDERS_BY_NO[order_no] = ctx
    if cpf:
        ORDERS_BY_CPF.setdefault(cpf, []).append(order_no)
    if email:
        ORDERS_BY_EMAIL.setdefault(email, []).append(order_no)
    if phone:
        LAST_ORDER_BY_PHONE[phone] = order_no

    return JSONResponse({"indexed": True, "order_no": order_no})


@app.post("/webhook/cartpanda/support")
async def cartpanda_support(request: Request):
    data = await request.json()
    d = data.get("data") or data
    order_no = str(d.get("order_no") or d.get("number") or d.get("id") or "").strip()
    phone = normalize_phone((d.get("customer") or {}).get("phone") or d.get("phone") or "")

    if phone and order_no:
        LAST_ORDER_BY_PHONE[phone] = order_no
    if order_no and order_no not in ORDERS_BY_NO:
        ORDERS_BY_NO[order_no] = compact_order_view({"order_no": order_no, "customer": d.get("customer") or {}})

    return {"ok": True, "linked_phone": phone, "order_no": order_no}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


