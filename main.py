# agent.py
#
# Bot de atendimento para Cartpanda + Z-API via webhooks.
# Funções:
# - Recebe webhooks Cartpanda (order.created PIX pendente, order.paid entrega, abandoned.created, lista abandoned_carts)
# - Recebe mensagens WhatsApp (Z-API inbound) + status webhook
# - Envia texto/imagem/arquivo via Z-API (com retries)
#
# Execução local:
#   pip install -r requirements.txt
#   export FLASK_ENV=production
#   python agent.py
#
# Produção (Render):
#   web: gunicorn agent:app --bind 0.0.0.0:$PORT --timeout 120
#
import os
import hmac
import hashlib
import json
import re
import time
import random
from datetime import datetime, timedelta, timezone

import requests
from flask import Flask, request, jsonify, abort
app = Flask(__name__)

# -------------------------
# Config
# -------------------------
PORT = int(os.getenv("PORT", "8000"))

# PROMPT (corrigido sem aspas duplicadas)
SYSTEM_PROMPT_TEMPLATE = (
    "Você é um assistente comercial curto e objetivo. "
    "Saudação curta: '{greeting}, {name}, tudo bem? Como posso ajudar?' (sem nome: '{greeting}, tudo bem? Como posso ajudar?'). "
    "Responda em 1–2 frases. Sem textão. "
    "Se pedirem produto específico → responda com nome, descrição curta (máx. 2 frases) e checkout direto. "
    "Se pedirem detalhes → até 2 frases. "
    "Se não pedirem link/site, não envie link algum. "
    "Entrega 100% digital. Nunca fale de endereço/frete/correios/rastreio. "
    "Se perguntarem por entrega/prazo/frete/rastreio → diga que é digital e enviada/liberada por e-mail/WhatsApp após pagamento, e ofereça checar status pelo nº do pedido ou CPF. "
    "Se perguntarem se chega na casa: diga que NÃO, pois é e-book digital. "
    "Se segurança → cite checkout HTTPS/PSP oficial. "
    "Se não recebeu por e-mail → peça nº do pedido ou CPF/CNPJ e ofereça reenvio. "
    "Se pagamento travou → pergunte em que etapa e ofereça ajuda. "
    "Se citar Instagram/engajamento → ofereça bônus após seguir e comentar 3 posts; peça @ para validar. "
    "Nunca peça senhas/códigos. Nunca prometa alterar preço automaticamente."
)

# LLM (opcional, só se OPENAI_API_KEY estiver definido)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
try:
    if OPENAI_API_KEY:
        from openai import OpenAI
        _oai_client = OpenAI(api_key=OPENAI_API_KEY)
    else:
        _oai_client = None
except Exception:
    _oai_client = None

TZ_OFFSET = int(os.getenv("TZ_OFFSET_MINUTES", "-180"))  # Brazil default -03:00
TZ = timezone(timedelta(minutes=TZ_OFFSET))

# Z-API (obrigatório)
ZAPI_INSTANCE = os.getenv("ZAPI_INSTANCE", "").strip()
ZAPI_TOKEN = os.getenv("ZAPI_TOKEN", "").strip()
ZAPI_CLIENT_TOKEN = os.getenv("ZAPI_CLIENT_TOKEN", "").strip()
ZAPI_BASE = f"https://api.z-api.io/instances/{ZAPI_INSTANCE}/token/{ZAPI_TOKEN}".rstrip("/")

# Verificação simples do webhook inbound da Z-API
WEBHOOK_VERIFY_TOKEN = os.getenv("WEBHOOK_VERIFY_TOKEN", "changeme")

# Cartpanda HMAC (opcional, recomendado)
CARTPANDA_SIG_HEADER = os.getenv("CARTPANDA_SIG_HEADER", "X-Cartpanda-Signature")
CARTPANDA_HMAC_SECRET = os.getenv("CARTPANDA_HMAC_SECRET", "").strip()

# Redis (obrigatório)
from redis import Redis

REDIS_URL = os.getenv("REDIS_URL", "").strip()
if not REDIS_URL:
    raise RuntimeError("REDIS_URL não definido")

r = Redis.from_url(REDIS_URL, decode_responses=True)

@app.route("/health", methods=["GET"])
def health():
    try:
        r.ping()
        return jsonify(ok=True), 200
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500


# Anti-repetição e upsell
UPSELL_COOLDOWN_HOURS = int(os.getenv("UPSELL_COOLDOWN_HOURS", "24"))
SENT_TTL_MIN = int(os.getenv("SENT_TTL_MIN", "90"))

# Rate limit
RL_PER_MIN = int(os.getenv("RL_PER_MIN", "40"))
RL_PER_HOUR = int(os.getenv("RL_PER_HOUR", "600"))

# Upsell simples
UPSELL_RULES = {
    "*Tabib - Volume 1": "*Tabib - Volume 2",
    "*Tabib - Volume 2": "*Tabib - Volume 3",
    "*Tabib - Volume 3": "*Tabib - Volume 4",
    "*Tabib - Volume 4": "*TABIB KIDS",
    "*TABIB KIDS": "*Balsamo - Pomadas naturais",
}

# Entrega fallback por handle → link
DRIVE_FALLBACK = json.loads(os.getenv("DRIVE_FALLBACK_JSON", "{}"))

# -------------------------
# Copys (última versão enviada)
# -------------------------
COPY_SAUDACAO = "{saud}, {nome}. Como posso te ajudar?"
COPY_RETOMAR = "{saud}, {nome}! Aqui está seu link para retomar: {link}"
COPY_PIX = "{saud}, {nome}! Seu PIX ficou pendente. Link: {link}\nCódigo PIX:\n{pix_code}"
COPY_ENTREGA = "{saud}, {nome}! Obrigado pela compra {order}. Acesso: {digital}"
COPY_NAO_RECEBI_ASK = "{saud}, {nome}. Envie o nº do pedido (ex.: #73644) ou um print do e-mail para eu localizar e reenviar."
COPY_UPSELL = "Oferta única hoje: {oferta}. Quer aproveitar?"
COPY_UPSELL_NAO_QUERO = "Tudo bem. Promo única só hoje. Se mudar de ideia, estou à disposição."
COPY_RETORNO = "{saud}, {nome}! Que bom te ver de volta. Segue o link para retomar: {link}"
COPY_FALLBACK = "{saud}, {nome}. Posso: retomar carrinho, pagar PIX ou reenviar o ebook. Diga 'retomar', 'PIX' ou mande o nº do pedido."

# Respostas diretas de política (gatilhos)
COPY_ENTREGA_DIGITAL = "É 100% digital. Você recebe por e-mail/WhatsApp após o pagamento."
COPY_SEGURANCA = "Checkout HTTPS com PSP oficial. Nunca pedimos senhas/códigos."
COPY_PAGAMENTO_TRAVOU = "Em que etapa travou? PIX, cartão ou boleto?"
COPY_INSTAGRAM = "Tem bônus após seguir e comentar 3 posts no Instagram. Qual seu @ para validar?"

# -------------------------
# Utilitários
# -------------------------
app = Flask(__name__)

def now():
    return datetime.now(TZ)

def saudacao():
    h = now().hour
    if 5 <= h <= 11:
        return "Bom dia"
    if 12 <= h <= 17:
        return "Boa tarde"
    return "Boa noite"

def normalize_phone(raw: str) -> str:
    import re
    d = re.sub(r"\D+", "", raw or "")
    if d.startswith("55") and len(d) >= 12: return d
    if len(d) in (10,11): return "55"+d
    return d

def first_nonempty(*vals):
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
        if v:
            return v
    return ""

def sent_guard(phone: str, key: str, ttl_min: int = SENT_TTL_MIN) -> bool:
    k = f"sent:{phone}:{key}"
    nx = r.set(k, "1", ex=ttl_min * 60, nx=True)
    return bool(nx)

def set_user(phone: str, **fields):
    if not phone:
        return
    key = f"user:{phone}"
    if fields:
        r.hset(key, mapping={k: v for k, v in fields.items() if v is not None})

def get_user(phone: str) -> dict:
    if not phone:
        return {}
    return r.hgetall(f"user:{phone}") or {}

def block_upsell(phone: str):
    r.setex(f"upsell_block:{phone}", UPSELL_COOLDOWN_HOURS * 3600, "1")

def upsell_allowed(phone: str) -> bool:
    return r.get(f"upsell_block:{phone}") is None

def find_upsell_for_titles(titles):
    for t in titles:
        if t in UPSELL_RULES:
            return UPSELL_RULES[t]
    return None

def rate_limit_ok(phone: str) -> bool:
    m1 = f"rl:1m:{phone}"
    m60 = f"rl:60m:{phone}"
    c1 = r.incr(m1)
    c60 = r.incr(m60)
    try:
        r.expire(m1, 60)
        r.expire(m60, 3600)
    except Exception:
        pass
    return c1 <= RL_PER_MIN and c60 <= RL_PER_HOUR

def verify_cartpanda_hmac(raw_body: bytes, signature: str) -> bool:
    if not CARTPANDA_HMAC_SECRET:
        return True
    if not signature:
        return False
    mac = hmac.new(CARTPANDA_HMAC_SECRET.encode(), msg=raw_body, digestmod=hashlib.sha256).hexdigest()
    try:
        return hmac.compare_digest(mac, signature)
    except Exception:
        return mac == signature

def idempotent_event_seen(evt_id: str) -> bool:
    if not evt_id:
        return False
    k = f"evt:{evt_id}"
    nx = r.set(k, "1", ex=24 * 3600, nx=True)
    return not bool(nx)  # True = já visto

def retry_post(url, headers=None, json_body=None, tries=3, base=0.4, cap=3.0, timeout=20):
    last = None
    for i in range(tries):
        try:
            resp = requests.post(url, headers=headers or {}, json=json_body, timeout=timeout)
            return resp
        except Exception as e:
            last = e
            time.sleep(min(cap, base * (2 ** i)) + random.random() * 0.2)
    raise last

# -------------------------
# Z-API send
# -------------------------
def _zapi_headers():
    return {
        "Client-Token": ZAPI_CLIENT_TOKEN,
        "Content-Type": "application/json",
    }

def zapi_send_text(phone: str, text: str) -> dict:
    url = f"{ZAPI_BASE}/send-text"
    payload = {"phone": phone, "message": text}
    try:
        resp = retry_post(url, headers=_zapi_headers(), json_body=payload)
        try:
            data = resp.json()
        except Exception:
            data = {"status_code": resp.status_code, "text": resp.text}
        return {"ok": resp.status_code < 300, "status": resp.status_code, "data": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def zapi_send_image(phone: str, image_url: str, caption: str = "") -> dict:
    if not (image_url or "").lower().startswith("http"):
        return {"ok": False, "error": "invalid_image_url"}
    url = f"{ZAPI_BASE}/send-image"
    payload = {"phone": phone, "image": image_url, "caption": caption}
    try:
        resp = retry_post(url, headers=_zapi_headers(), json_body=payload)
        data = resp.json() if "application/json" in resp.headers.get("content-type", "") else {"text": resp.text}
        return {"ok": resp.status_code < 300, "status": resp.status_code, "data": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def zapi_send_file(phone: str, file_url: str, caption: str = "") -> dict:
    if not (file_url or "").lower().startswith("http"):
        return {"ok": False, "error": "invalid_file_url"}
    url = f"{ZAPI_BASE}/send-file"
    payload = {"phone": phone, "file": file_url, "caption": caption}
    try:
        resp = retry_post(url, headers=_zapi_headers(), json_body=payload)
        data = resp.json() if "application/json" in resp.headers.get("content-type", "") else {"text": resp.text}
        return {"ok": resp.status_code < 300, "status": resp.status_code, "data": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# -------------------------
# App & helpers
# -------------------------
app = Flask(__name__)

def greet_name_for(phone: str) -> str:
    u = get_user(phone)
    nm = first_nonempty(u.get("name"), "cliente").split()[0]
    return nm

# -------------------------
# Cartpanda Webhook
# -------------------------
@app.post("/webhook/cartpanda")
def webhook_cartpanda():
    raw = request.get_data() or b""
    sig = request.headers.get(CARTPANDA_SIG_HEADER, "")
    if not verify_cartpanda_hmac(raw, sig):
        abort(401)

    data = request.get_json(silent=True) or {}
    event = data.get("event") or data.get("type") or ""
    # idempotência
    evt_id = str(
        data.get("id")
        or (data.get("webhook") or {}).get("id")
        or (data.get("order") or {}).get("id")
        or hashlib.sha256(raw).hexdigest()[:40]
    )
    if idempotent_event_seen(evt_id):
        return jsonify({"ok": True, "dup": True})

    # ---- Lista de abandonados (payload grande)
    if isinstance((data.get("abandoned_carts") or {}).get("data"), list):
        carts = data["abandoned_carts"]["data"]
        count = 0
        for c in carts:
            cust = c.get("customer") or {}
            phone = normalize_phone(first_nonempty(cust.get("phone"), cust.get("phone_ext")))
            cart_token = c.get("cart_token") or ""
            cart_url = c.get("cart_url") or ""
            if not phone or not cart_token:
                continue
            name = first_nonempty(cust.get("first_name"), cust.get("full_name"), "cliente").split()[0]
            set_user(phone, name=name, last_cart=cart_url)
            r.set(f"last_cart_by_phone:{phone}", cart_url)
            count += 1
        return jsonify({"ok": True, "mode": "abandoned_list", "count": count})

    # ---- Eventos principais
    if event in ("order.paid", "order.created"):
        order = data.get("order", {}) or {}
        phone = normalize_phone(first_nonempty(order.get("phone"), (order.get("customer") or {}).get("phone")))
        name = first_nonempty((order.get("customer") or {}).get("first_name"),
                              (order.get("customer") or {}).get("full_name"),
                              "cliente").split()[0]
        if phone:
            set_user(phone, name=name)

        # PIX pendente
        if event == "order.created" and str(order.get("payment_status")) in ("1", "pending"):
            link = order.get("checkout_link") or ""
            px = (order.get("payment") or {}).get("pix_code") or order.get("pix_code") or ""
            if phone:
                set_user(phone, last_pix_link=link, last_pix_code=px)
                key = f"pix:{order.get('order_number')}"
                if rate_limit_ok(phone) and sent_guard(phone, key):
                    msg = COPY_PIX.format(saud=saudacao(), nome=name, link=link, pix_code=px or "(código indisponível)")
                    zapi_send_text(phone, msg)
            return jsonify({"ok": True})

        # Pago → entregar + upsell
        if event == "order.paid" or str(order.get("payment_status")) in ("3", "paid"):
            order_no = first_nonempty(order.get("public_id"), f"#{order.get('order_number')}")
            digital = order.get("digital_attachment") or ""
            if not digital:
                # tenta por handle do produto com fallback
                items = order.get("line_items", []) or []
                for it in items:
                    prod = it.get("product_images_info") or {}
                    handle = prod.get("handle") or ""
                    if handle and handle in DRIVE_FALLBACK:
                        digital = DRIVE_FALLBACK[handle]
                        break
            if not digital:
                digital = order.get("thank_you_page") or order.get("order_status_url") or ""
            titles = []
            for it in order.get("line_items", []) or []:
                titles.append(first_nonempty(it.get("title"), (it.get("variant") or {}).get("title")))
            if phone:
                set_user(phone, last_order=order_no, last_digital=digital, last_products="|".join(titles))
                key = f"paid:{order.get('id')}"
                if rate_limit_ok(phone) and sent_guard(phone, key):
                    msg = COPY_ENTREGA.format(saud=saudacao(), nome=name, order=order_no, digital=digital or "(link indisponível)")
                    zapi_send_text(phone, msg)
                # upsell
                if upsell_allowed(phone):
                    oferta = find_upsell_for_titles(titles) if titles else None
                    if oferta:
                        block_upsell(phone)
                        zapi_send_text(phone, COPY_UPSELL.format(oferta=oferta))
            return jsonify({"ok": True})

    elif event in ("abandoned.created", "cart.abandoned", "abandoned"):
        payload = data.get("data", {}) or data
        cust = first_nonempty(payload.get("customer"), payload.get("customer_info")) or {}
        phone = normalize_phone(first_nonempty(cust.get("phone"), cust.get("phone_ext")))
        name = first_nonempty(cust.get("first_name"), (cust.get("full_name") or "cliente").split()[0])
        cart_url = payload.get("cart_url") or ""
        if phone:
            set_user(phone, name=name, last_cart=cart_url)
            r.set(f"last_cart_by_phone:{phone}", cart_url)
        # Não enviar proativamente (guardamos para "retomar")
        return jsonify({"ok": True})

    return jsonify({"ignored": True})

# -------------------------
# Z-API inbound webhook (mensagens do cliente)
# -------------------------
@app.post("/webhook/zapi/inbound")
def webhook_zapi_inbound():
    token = request.args.get("t") or request.headers.get("X-Webhook-Token", "")
    if token != WEBHOOK_VERIFY_TOKEN:
        abort(403)

    body = request.get_json(silent=True) or {}

    phone = normalize_phone(
        first_nonempty(
            body.get("phone"),
            (body.get("message") or {}).get("phone"),
            ((body.get("data") or {}).get("message") or {}).get("from"),
            (body.get("sender") or {}).get("phone"),
            (body.get("payload") or {}).get("phone"),
        )
    )
    text = first_nonempty(
        body.get("messageText"),
        (body.get("message") or {}).get("message"),
        (body.get("message") or {}).get("text"),
        (body.get("data") or {}).get("text"),
        body.get("text"),
        "",
    ).strip()

    if not phone or not text:
        return jsonify({"ok": True, "note": "sem phone/text"})

    if not rate_limit_ok(phone):
        zapi_send_text(phone, "Recebi muitas mensagens. Vou responder por partes, combinado?")
        return jsonify({"ok": True, "rate_limited": True})

    user = get_user(phone)
    name = first_nonempty(user.get("name"), "cliente").split()[0]
    low = text.lower()

    # Intenções chave
    if any(k in low for k in ["retomar", "continuar", "não desisti", "nao desisti", "seguir", "finalizar"]):
        link = first_nonempty(user.get("last_pix_link"), user.get("last_cart"))
        msg = COPY_RETOMAR.format(saud=saudacao(), nome=name, link=link or "(link não encontrado)")
        zapi_send_text(phone, msg)
        return jsonify({"ok": True})

    if any(k in low for k in ["pix", "pagar", "pendente"]):
        link = user.get("last_pix_link")
        px = user.get("last_pix_code") or ""
        if link:
            msg = COPY_PIX.format(saud=saudacao(), nome=name, link=link, pix_code=px or "(sem código)")
        else:
            msg = f"{saudacao()}, {name}. Não encontrei PIX pendente. Diga 'retomar' para recuperar o carrinho."
        zapi_send_text(phone, msg)
        return jsonify({"ok": True})

    if any(k in low for k in ["não quero", "nao quero", "agora não", "agora nao"]):
        block_upsell(phone)
        zapi_send_text(phone, COPY_UPSELL_NAO_QUERO)
        return jsonify({"ok": True})

    if any(k in low for k in ["não recebi", "nao recebi", "não chegou", "nao chegou", "ebook", "produto", "acesso"]):
        digital = user.get("last_digital")
        if digital:
            msg = COPY_ENTREGA.format(saud=saudacao(), nome=name, order=user.get("last_order", "#?"), digital=digital)
        else:
            msg = COPY_NAO_RECEBI_ASK.format(saud=saudacao(), nome=name)
        zapi_send_text(phone, msg)
        return jsonify({"ok": True})

    # Frete/entrega/rastreio
    if any(k in low for k in ["frete", "entrega", "prazo", "rastreio", "rastreamento", "correio", "correios", "endereço", "endereco"]):
        zapi_send_text(phone, COPY_ENTREGA_DIGITAL)
        return jsonify({"ok": True})

    # Segurança
    if any(k in low for k in ["segurança", "seguranca", "golpe", "fraude", "medo"]):
        zapi_send_text(phone, COPY_SEGURANCA)
        return jsonify({"ok": True})

    # Pagamento travou
    if any(k in low for k in ["travou", "não consegui pagar", "nao consegui pagar", "erro no pagamento", "cartão", "cartao", "boleto"]):
        zapi_send_text(phone, COPY_PAGAMENTO_TRAVOU)
        return jsonify({"ok": True})

    # Instagram/engajamento
    if any(k in low for k in ["instagram", "comentar", "comentário", "comentario", "seguir", "post", "@"]):
        zapi_send_text(phone, COPY_INSTAGRAM)
        return jsonify({"ok": True})

    # Número do pedido (#12345)
    m = re.search(r"#?\s*(\d{3,})", low)
    if m:
        digital = user.get("last_digital")
        if digital:
            msg = COPY_ENTREGA.format(saud=saudacao(), nome=name, order=f"#{m.group(1)}", digital=digital)
        else:
            msg = COPY_NAO_RECEBI_ASK.format(saud=saudacao(), nome=name)
        zapi_send_text(phone, msg)
        return jsonify({"ok": True})

    # Saudações
    if any(k in low for k in ["oi", "olá", "ola", "bom dia", "boa tarde", "boa noite", "oie", "oii"]):
        zapi_send_text(phone, COPY_SAUDACAO.format(saud=saudacao(), nome=name))
        return jsonify({"ok": True})

    # Fallback curto
    zapi_send_text(phone, COPY_FALLBACK.format(saud=saudacao(), nome=name))
    return jsonify({"ok": True})

# Status de mensagens da Z-API (opcional)
@app.post("/webhook/zapi/status")
def webhook_zapi_status():
    token = request.args.get("t") or request.headers.get("X-Webhook-Token", "")
    if token != WEBHOOK_VERIFY_TOKEN:
        abort(403)
    _ = request.get_json(silent=True) or {}
    # apenas logar se quiser:
    # print("ZAPI STATUS:", _)
    return jsonify({"ok": True})

# -------------------------
# Health e root
# -------------------------
@app.get("/health")
def health():
    try:
        r.ping()
        return {"ok": True, "time": now().isoformat()}
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500

@app.get("/")
def index():
    return {"service": "paginatto-agent",
            "docs": ["/health", "/webhook/cartpanda", "/webhook/zapi/inbound", "/webhook/zapi/status"]}

# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)











