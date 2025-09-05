"""
Paginatto — Iara WhatsApp Bot
Z-API + ChatGPT + CartPanda + Catálogo JSON
Python 3.11+
"""

import os
import re
import json
import unicodedata
import logging
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import httpx
import redis
from openai import OpenAI
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

load_dotenv()

# --- Config ---
OPENAI_API_KEY = 
ASSISTANT_NAME = os.getenv("ASSISTANT_NAME", "Iara")
BRAND_NAME = os.getenv("BRAND_NAME", "Paginatto")
SITE_URL = os.getenv("SITE_URL", "https://paginattoebooks.github.io/Paginatto.site.com.br/")
SUPPORT_URL = os.getenv("SUPPORT_URL", SITE_URL)
CNPJ = os.getenv("CNPJ", "57.941.903/0001-94")
SECURITY_BLURB = os.getenv("SECURITY_BLURB", "Checkout com HTTPS e PSP oficial. Não pedimos senhas ou códigos.")
DELIVERY_ONE_LINER = (
    "Entrega 100% digital. Enviamos/liberamos o e-book por e-mail e WhatsApp após o pagamento. "
    "Não pedimos endereço e não existe rastreio."
)
ZAPI_INSTANCE = os.getenv("ZAPI_INSTANCE", "3E2D08AA912D5063906206E9A5181015")
ZAPI_TOKEN = os.getenv("ZAPI_TOKEN", "F8d6942e55c57407e95c2ceae481f6a92S"
SEND_TEXT_PATH = os.getenv("SEND_TEXT_PATH", "/send-text")
MAX_DISCOUNT_PCT = int(os.getenv("MAX_DISCOUNT_PCT", "10"))
INSTAGRAM_HANDLE = os.getenv("INSTAGRAM_HANDLE", "@Paginatto")
INSTAGRAM_URL = os.getenv("INSTAGRAM_URL", "https://instagram.com/Paginatto")
PRODUCTS_JSON_PATH = os.getenv("PRODUCTS_JSON_PATH", "produtos_paginatto.json")

if not (OPENAI_API_KEY and ZAPI_INSTANCE and ZAPI_TOKEN and ZAPI_CLIENT_TOKEN):
    raise RuntimeError("Defina OPENAI_API_KEY, ZAPI_INSTANCE, ZAPI_TOKEN, ZAPI_CLIENT_TOKEN no .env")

OPENAI = OpenAI(api_key=OPENAI_API_KEY)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS = redis.Redis.from_url(REDIS_URL, decode_responses=True)

app = FastAPI(title="Paginatto — Iara Bot")

CONTINUE_KW = {"sim", "quero continuar", "continuar", "retomar", "seguir", "finalizar", "pagar", "quero pagar", "voltar ao carrinho"}

def _normalize(s: str) -> str:
    s = (s or "").lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.replace("ó", "o").replace("ô", "o").replace("õ", "o").replace("á", "a").replace("à", "a").replace("ã", "a") \
         .replace("é", "e").replace("ê", "e").replace("í", "i").replace("ú", "u").replace("ç", "c")
    return re.sub(r"[^a-z0-9 ]+", " ", s).strip()

def load_products(path: str) -> Dict[str, Dict[str, str]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = [
            {
                "name": "Tabib Volume 1: Tratamento de Dores e Inflamações",
                "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919679:1",
                "image": "",
                "description": "Guia para combater dores de cabeça, musculares e articulares, além de inflamações crônicas. Traz receitas naturais e eficazes para aliviar desconfortos do dia a dia sem efeitos colaterais."
            },
            {
                "name": "Tabib Volume 2: Saúde Respiratória e Imunidade",
                "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919682:1",
                "image": "",
                "description": "Focado na saúde respiratória e fortalecimento da imunidade. Inclui receitas para tratar sinusite, rinite, gripes e resfriados."
            },
            {
                "name": "Tabib Volume 3: Saúde Digestiva e Metabólica",
                "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919686:1",
                "image": "",
                "description": "Reúne receitas que promovem equilíbrio digestivo e regulam o metabolismo. Indicado para quem busca aliviar desconfortos gástricos e desintoxicar o organismo."
            },
            {
                "name": "Tabib Volume 4: Saúde Mental e Energética",
                "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919707:1",
                "image": "",
                "description": "Traz receitas para bem-estar emocional, redução do estresse e aumento da energia. Ajuda a melhorar o humor, o sono e o equilíbrio da mente."
            },
            {
                "name": "Tabib completo",
                "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/184229277:1",
                "image": "",
                "description": "Coletânea que une todos os volumes Tabib. Oferece soluções naturais para dores, imunidade, digestão e saúde mental, resgatando a sabedoria tradicional."
            },
            {
                "name": "Tabib 2025 + Bônus 19,90 + Tabib 2024",
                "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/184229263:1",
                "image": "",
                "description": "Pacote que reúne duas edições completas do Tabib. Organizado por áreas de tratamento, é uma ferramenta prática para cuidados naturais e acessíveis."
            },
            {
                "name": "Antídoto - Antídotos indígenas",
                "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919637:1",
                "image": "",
                "description": "Inspirado em saberes indígenas, traz receitas naturais para tratar picadas de insetos e animais peçonhentos. Cada antídoto é explicado com detalhes sobre os ingredientes e seu efeito."
            },
            {
                "name": "Kurimã - Óleos essenciais",
                "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919661:1",
                "image": "",
                "description": "Guia prático sobre óleos essenciais, com receitas e dicas para relaxamento, alívio de dores e cuidados com a pele. Ensina usos seguros e terapêuticos no dia a dia."
            },
            {
                "name": "Bálsamo - Pomadas naturais",
                "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919668:1",
                "image": "",
                "description": "Manual para criar pomadas com plantas e ingredientes naturais. Inclui fórmulas para dores musculares, feridas, inflamações e mais, com explicações sobre cada componente."
            },
            {
                "name": "Tratamento Natural Personalizado para Pressão Alta",
                "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/174502432:1",
                "image": "",
                "description": "Plano individualizado que combina alimentação, ervas naturais e exercícios para controlar a pressão arterial de forma segura e eficaz. Enviado por e-mail após o quiz."
            },
            {
                "name": "300 receitas para AirFryer",
                "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/176702038:1",
                "image": "",
                "description": "Coletânea de receitas práticas e saborosas feitas especialmente para airfryer."
            }
        ]
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
            "description": item.get("description") or ""
        }
        aliases = {_normalize(name)}
        m = re.search(r"(tabib).*(\d+)", name.lower())
        if m:
            vol = m.group(2)
            for pat in [f"tabib {vol}", f"tabib volume {vol}", f"tabib v {vol}", f"tabib{vol}", f"v{vol}", f"volume {vol}", f"bibi {vol}"]:
                aliases.add(_normalize(pat))
        p["aliases"] = list(aliases)
        out[key] = p
    return out

PRODUCTS = load_products(PRODUCTS_JSON_PATH)

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

def wants_resume(t: str) -> bool:
    tl = (t or "").lower()
    return any(k in tl for k in CONTINUE_KW)

def find_product_in_text(text: str) -> dict | None:
    q = _normalize(text)
    if "tabib" in q or "bibi" in q:
        for n, key in ((" 4", "tabib4"), (" 3", "tabib3"), (" 2", "tabib2"), (" 1", "tabib1")):
            if n in f" {q} ":
                return PRODUCTS.get(key)
        if "completo" in q or "colecao" in q or "combo" in q:
            return PRODUCTS["tabib_full"]
    for pid, aliases in PROD_ALIASES.items():
        for a in aliases:
            if _normalize(a) in q:
                return PRODUCTS.get(pid)
    for _, p in PRODUCTS.items():
        if _normalize(p.get("name", "")) and any(w for w in q.split() if w and w in _normalize(p["name"])):
            return p
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
        "delivery": has("entrega", "prazo", "quando chega", "chega quando", "rastreio", "rastreamento", "código de rastreio", "frete", "transportadora", "correios", "cep", "endereço", "endereco"),
        "product_info": has("o que é", "sobre", "detalhes", "como funciona", "descrição", "explicação")
    }

def cpf_from_text(text: str) -> Optional[str]:
    m = re.compile(r"\d{3}\.?\d{3}\.?\d{3}-?\d{2}").search(text or "")
    return digits_only(m.group(0)) if m else None

def orderno_from_text(text: str) -> Optional[str]:
    m = re.compile(r"\b\d{6,}\b").search(text or "")
    return m.group(0) if m else None

def order_context_by_keys(phone: str, text: str) -> Optional[Dict[str, Any]]:
    order_no = REDIS.get(f"last_order_by_phone:{phone}")
    if order_no:
        ctx_json = REDIS.hget("orders_by_no", order_no)
        if ctx_json:
            ctx = json.loads(ctx_json)
            if ctx.get("cart_token") and CHECKOUT_RESUME_BASE:
                ctx["resume_link"] = f"{CHECKOUT_RESUME_BASE}{ctx['cart_token']}"
            return ctx

    order_no = orderno_from_text(text or "")
    if order_no:
        ctx_json = REDIS.hget("orders_by_no", order_no)
        if ctx_json:
            ctx = json.loads(ctx_json)
            if ctx.get("cart_token") and CHECKOUT_RESUME_BASE:
                ctx["resume_link"] = f"{CHECKOUT_RESUME_BASE}{ctx['cart_token']}"
            return ctx

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

async def cartpanda_lookup(order_no: Optional[str] = None, cpf: Optional[str] = None, email: Optional[str] = None) -> Optional[Dict[str, Any]]:
    CARTPANDA_API_BASE = os.getenv("CARTPANDA_API_BASE", "")
    CARTPANDA_API_TOKEN = os.getenv("CARTPANDA_API_TOKEN", "")
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
        except Exception as e:
            logging.error(f"Erro em cartpanda_lookup: {e}")
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
    "Saudação curta: '{greeting}, {name}! Como posso ajudar?' (sem nome: '{greeting}! Como posso ajudar?'). "
    "Respostas curtas: 1–2 frases. Sem textão. "
    "Se pedirem produto específico → responda com nome, descrição curta (máx. 2 frases) e checkout direto. "
    "Se pedirem detalhes do produto → forneça descrição completa, mas clipada a 2 frases. "
    "Se não pedirem link/site, não envie link algum. "
    "Entrega 100% digital. Nunca fale de endereço/frete/correios/rastreio. "
    "Produto e entrega: 100% DIGITAL (e-book). Nunca fale de endereço, frete, correios, transportadora ou rastreio. "
    "Se perguntarem por entrega, endereço, prazo, frete ou rastreio → responda apenas que é digital e enviada/liberada por e-mail/WhatsApp após pagamento, e ofereça checar status. "
    "ENTREGA: 100% digital (ebook). NUNCA fale de frete, endereço, rastreio, Correios, transportadora. "
    "Se perguntarem sobre entrega: responda curto → 'É digital. Você recebe por e-mail/área do pedido. Posso checar pelo nº do pedido ou CPF?'. "
    "Cumprimento curto: 'bom dia/boa tarde, como posso ajudar?'. "
    "Se não pedirem link/site, não envie link algum. "
    "Se perguntarem se chega na casa: diga que NÃO, pois é um ebook virtual. "
    "Se desisti → pergunte o motivo. "
    "Se segurança → diga que o checkout é HTTPS/PSP oficial; se e somente se pedirem site, ofereça {site}. "
    "Se não recebeu por e-mail → peça nº do pedido ou CPF/CNPJ para verificar; ofereça reenvio pelo e-mail, pergunte o e-mail cadastrado. "
    "Se achou que era físico → avise que é e-book digital e cite benefícios. "
    "Se pagamento travou → pergunte em que etapa e ofereça ajuda para finalizar o pagamento. "
    "Se citar Instagram/engajamento → ofereça bônus após seguir e comentar 3 posts; peça @ para validar. "
    "Nunca peça senhas/códigos. Nunca prometa alterar preço automaticamente."
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
    if hints and hints.get("product_id") and hints.get("product"):
        p = hints["product"]
        base += f" Produto foco: {p.get('name')}. Descrição: {p.get('description', '')}. Entrega digital imediata."
    if extra_context and extra_context.get("resume_link"):
        base += f" Use este resume_link quando apropriado: {extra_context['resume_link']}"
    if hints:
        focos = [k for k, v in hints.items() if isinstance(v, bool) and v]
        if focos:
            base += " | FOCO: " + ",".join(focos)
    return base

def quick_routes(t: str) -> Optional[str]:
    tl = (t or "").lower()
    if any(k in tl for k in ["entrega", "frete", "chega", "prazo", "rastreio", "rastreamento", "correios", "transportadora", "endereco", "endereço"]):
        return "É digital. Você recebe por e-mail/WhatsApp após o pagamento. Quer ajuda para finalizar?"
    if "fisico" in tl or "físico" in tl:
        return "Não é físico. É e-book digital com acesso imediato após o pagamento."
    return None

def _clip(txt: str) -> str:
    sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?|\!)\s', txt.strip())
    clipped = ' '.join(sentences[:2]).strip()
    if clipped and not clipped.endswith(('.', '?', '!')):
        clipped += '.'
    return clipped if clipped else txt

async def llm_reply(history, ctx, hints):
    msgs = [{"role": "system", "content": system_prompt(ctx, hints)}]
    if ctx:
        msgs.append({"role": "assistant", "content": f"DADOS_DO_PEDIDO: {order_summary(ctx)}"})
    msgs += history[-20:]

    resp = OPENAI.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.2,
        max_tokens=120,
        messages=msgs,
    )
    txt = (resp.choices[0].message.content or "").strip()

    if not ctx and any(x in txt.lower() for x in ["nao chegou", "não chegou", "nao recebi", "não recebi", "acesso", "nao consigo", "não consigo"]):
        return _clip("Me passa o nº do pedido? Se não tiver, pode ser CPF/CNPJ. Vou verificar no sistema.")
    if not ctx and ("acesso" in txt.lower() or "nao consigo" in txt.lower() or "não consigo" in txt.lower()):
        return _clip("Me passa o nº do pedido ou CPF/CNPJ para eu localizar. Posso falar com o time humano se preferir.")
    
    return _clip(txt)

def scrub_links_if_not_requested(user_text: str, reply: str) -> str:
    if "site" in (user_text or "").lower() or "link" in (user_text or "").lower():
        return reply
    return re.sub(r"https?://\S+", "", reply).strip()

# --- Webhooks ---
@app.get("/health")
async def health():
    return {"ok": True}

async def zapi_send_text(phone: str, message: str) -> dict:
    url = f"https://api.z-api.io/instances/{ZAPI_INSTANCE}/token/{ZAPI_TOKEN}{SEND_TEXT_PATH}"
    payload = {"phone": phone, "message": message}
    async with httpx.AsyncClient(timeout=20) as http:
        r = await http.post(url, headers={"Client-Token": ZAPI_CLIENT_TOKEN, "Content-Type": "application/json"}, json=payload)
        data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {"text": r.text}
        if r.status_code >= 300:
            raise HTTPException(status_code=502, detail={"zapi_error": data})
        return data

@app.post("/webhook/zapi/receive")
async def zapi_receive(request: Request):
    data = await request.json()
    logging.info(f"Recebido webhook Z-API: {data}")
    msg = (data.get("message") or data.get("body") or data.get("text") or "")
    phone = normalize_phone(data.get("phone") or (data.get("sender") or {}).get("phone") or "")
    if not phone or not msg:
        return JSONResponse({"status": "ignored", "reason": "missing phone or text"})

    if wants_resume(msg):
        ctx = order_context_by_keys(phone, msg)
        if ctx and (ctx.get("resume_link") or ctx.get("checkout_url")):
            link = ctx.get("resume_link") or ctx.get("checkout_url")
            await zapi_send_text(phone, f"Perfeito. Seu checkout: {link}")
            return JSONResponse({"status": "sent", "route": "resume", "order_no": ctx.get("order_no")})
        await zapi_send_text(phone, "Me envia nº do pedido ou CPF para puxar seu checkout.")
        return JSONResponse({"status": "need_id"})

    msg_id = data.get("messageId") or data.get("id") or (data.get("message") or {}).get("id")
    if msg_id:
        if REDIS.sismember("seen_ids", msg_id):
            return JSONResponse({"status": "duplicate"})
        REDIS.sadd("seen_ids", msg_id)

    intents = analyze_intent(msg)
    qr = quick_routes(msg)
    if not qr:
        if intents.get("not_received_email"):
            qr = "Me envia o nº do pedido ou CPF para eu checar."
        elif intents.get("security"):
            qr = "Checkout seguro com HTTPS e PSP oficial. Não pedimos senhas."
        elif intents.get("payment_problem"):
            qr = "Em que etapa o pagamento travou? Posso ajudar a finalizar."
        elif intents.get("instagram_bonus"):
            qr = "Siga @Paginatto e comente em 3 posts para o bônus! Qual seu @?"
        elif intents.get("support"):
            qr = f"Entre em contato pelo suporte: {SUPPORT_URL}"
        elif intents.get("product_info"):
            prod = find_product_in_text(msg)
            if prod:
                qr = f"{prod['name']}. {_clip(prod['description'])}"

    if qr:
        await zapi_send_text(phone, qr)
        return JSONResponse({"status": "sent", "route": "quick"})

    history_json = REDIS.get(f"sessions:{phone}") or "[]"
    history = json.loads(history_json)
    history.append({"role": "user", "content": msg})

    prod = find_product_in_text(msg)
    if prod:
        reply = f"{prod['name']}\n{_clip(prod['description'])}\nCheckout: {prod['checkout']}\nEntrega 100% digital."
        await zapi_send_text(phone, reply)
        history.append({"role": "assistant", "content": reply})
        REDIS.set(f"sessions:{phone}", json.dumps(history))
        return JSONResponse({"status": "sent", "route": "product", "product": prod["name"]})

    ctx = order_context_by_keys(phone, msg)
    hints = {"product_info": intents.get("product_info")}
    if prod:
        hints["product_id"] = _normalize(prod["name"])
        hints["product"] = prod
    ai = await llm_reply(history, ctx, hints)
    ai = scrub_links_if_not_requested(msg, ai)

    if len(history) <= 1 and len(ai) > 0 and not prod and not qr:
        g = br_greeting()
        if _normalize(msg) in {"oi", "ola", "olá", "bom dia", "boa tarde", "boa noite", "oii", "oie"}:
            ai = f"{g}! Como posso ajudar?"

    await zapi_send_text(phone, ai)
    history.append({"role": "assistant", "content": ai})
    REDIS.set(f"sessions:{phone}", json.dumps(history))
    return JSONResponse({"status": "sent", "route": "llm"})

@app.post("/webhook/zapi/status")
async def zapi_status(request: Request):
    data = await request.json()
    logging.info(f"Recebido webhook Z-API status: {data}")
    return {"ok": True}

@app.post("/webhook/cartpanda/order")
async def cartpanda_order(request: Request):
    data = await request.json()
    logging.info(f"Recebido webhook CartPanda: {data}")
    d = data.get("data") or data

    customer = d.get("customer") or {}
    order_no = str(d.get("order_no") or d.get("number") or d.get("id") or d.get("orderNumber") or "").strip()
    email = (customer.get("email") or d.get("email") or "").strip().lower()
    phone = normalize_phone(customer.get("phone") or d.get("phone") or "")
    cpf = digits_only(customer.get("document") or d.get("document") or d.get("cpf") or "")
    payment_status = (d.get("payment_status") or d.get("status") or "").strip().lower()
    cart_token = d.get("cart_token") or d.get("cartToken") or ""
    checkout_url = d.get("checkout_url") or d.get("checkoutUrl") or ""

    if not order_no or not phone:
        logging.error(f"Dados inválidos no webhook CartPanda: order_no={order_no}, phone={phone}")
        return JSONResponse({"error": "Dados inválidos: order_no ou phone ausente"}, status=400)

    ctx = {
        "order_no": order_no or None,
        "payment_status": payment_status or None,
        "checkout_url": checkout_url or None,
        "cart_token": cart_token or None,
        "customer": {
            "name": customer.get("name") or d.get("name") or "",
            "email": email,
            "phone": phone,
            "document": cpf,
        },
    }
    if cart_token and CHECKOUT_RESUME_BASE:
        ctx["resume_link"] = f"{CHECKOUT_RESUME_BASE}{cart_token}"

    if order_no:
        REDIS.hset("orders_by_no", order_no, json.dumps(ctx))
    if cpf:
        REDIS.rpush(f"orders_by_cpf:{cpf}", order_no or "")
    if email:
        REDIS.rpush(f"orders_by_email:{email}", order_no or "")
    if phone:
        REDIS.set(f"last_order_by_phone:{phone}", order_no or (cart_token or ""))

    return JSONResponse({"indexed": True, "order_no": order_no, "has_resume": bool(ctx.get("resume_link") or checkout_url)})

@app.post("/webhook/cartpanda/support")
async def cartpanda_support(request: Request):
    data = await request.json()
    logging.info(f"Recebido webhook CartPanda suporte: {data}")
    d = data.get("data") or data
    order_no = str(d.get("order_no") or d.get("number") or d.get("id") or "").strip()
    phone = normalize_phone((d.get("customer") or {}).get("phone") or d.get("phone") or "")

    if phone and order_no:
        REDIS.set(f"last_order_by_phone:{phone}", order_no)
    if order_no:
        ctx_json = REDIS.hget("orders_by_no", order_no)
        if not ctx_json:
            REDIS.hset("orders_by_no", order_no, json.dumps(compact_order_view({"order_no": order_no, "customer": d.get("customer") or {}})))

    return {"ok": True, "linked_phone": phone, "order_no": order_no}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
