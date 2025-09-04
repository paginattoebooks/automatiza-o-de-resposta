"""
Z-API + ChatGPT + CartPanda — WhatsApp Bot (FastAPI, single-file)

Pronto para subir no Render. Python 3.11+.

.env (preencha):
  OPENAI_API_KEY=sk-...
  ASSISTANT_NAME=Iara
  BRAND_NAME=Paginatto
  SITE_URL=https://paginattoebooks.github.io/Paginatto.site.com.br/
  SUPPORT_URL=https://paginattoebooks.github.io/Paginatto.site.com.br/
  CNPJ=00.000.000/0000-00
  SECURITY_BLURB=Checkout com HTTPS, PSP oficial para PIX, dados criptografados. Não pedimos senha/código.
  CHECKOUT_RESUME_BASE=https://seu-checkout.exemplo/resume/

  ZAPI_INSTANCE=xxxxxxxxxxxxxxxx
  ZAPI_TOKEN=xxxxxxxxxxxxxxxx
  ZAPI_CLIENT_TOKEN=xxxxxxxxxxxxxxxx
  SEND_TEXT_PATH=/send-text

  # Opcional: se tiver API do CartPanda (deixe vazio se não tiver)
  CARTPANDA_API_BASE=
  CARTPANDA_API_TOKEN=

Instalação local:
  pip install -r requirements.txt
  uvicorn main:app --host 0.0.0.0 --port 8000 --reload

No Render (Web Service):
  Build: pip install -r requirements.txt
  Start: uvicorn main:app --host 0.0.0.0 --port $PORT

Rotas que você vai usar:
  GET  /health                      → status
  POST /webhook/zapi/receive        → recebe mensagens do WhatsApp (Z-API)
  POST /webhook/zapi/status         → recibos (opcional)
  POST /webhook/cartpanda/order     → recebe eventos de pedido (order.created/updated)
  POST /webhook/cartpanda/support   → recebe “pedido de suporte” (você configura no CartPanda)

Como funciona:
- Z-API dispara webhook → /webhook/zapi/receive.
- O bot monta contexto com último pedido visto para o telefone, ou procura no índice por CPF/número de pedido citados na mensagem.
- O bot chama ChatGPT com prompt personalizado da marca (Iara, Paginatto, links, segurança).
- O bot responde pelo endpoint de envio da Z-API (Client-Token + instance/token).
- CartPanda envia order.created/updated/support → indexamos por order_no, CPF, e-mail, telefone, e cart_token.

Arquivo: requirements.txt
  fastapi
  uvicorn
  httpx
  python-dotenv
  pydantic
  openai

Arquivo: main.py
"""

import os
import re
import json
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import httpx

# OpenAI SDK oficial
from openai import OpenAI

load_dotenv()

# --- Config ---
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
ASSISTANT_NAME = os.getenv("ASSISTANT_NAME", "Iara")
BRAND_NAME = os.getenv("BRAND_NAME", "Paginatto")
SITE_URL = os.getenv("SITE_URL", "")
SUPPORT_URL = os.getenv("SUPPORT_URL", SITE_URL)
CNPJ = os.getenv("CNPJ", "")
SECURITY_BLURB = os.getenv(
    "SECURITY_BLURB",
    "Checkout com HTTPS e PSP oficial. Não pedimos senhas ou códigos."
)
CHECKOUT_RESUME_BASE = os.getenv("CHECKOUT_RESUME_BASE", "")

ZAPI_INSTANCE = os.getenv("ZAPI_INSTANCE", "")
ZAPI_TOKEN = os.getenv("ZAPI_TOKEN", "")
ZAPI_CLIENT_TOKEN = os.getenv("ZAPI_CLIENT_TOKEN", "")
SEND_TEXT_PATH = os.getenv("SEND_TEXT_PATH", "/send-text")

CARTPANDA_API_BASE = os.getenv("CARTPANDA_API_BASE", "")  # opcional
CARTPANDA_API_TOKEN = os.getenv("CARTPANDA_API_TOKEN", "")  # opcional

if not (OPENAI_API_KEY and ZAPI_INSTANCE and ZAPI_TOKEN and ZAPI_CLIENT_TOKEN):
    raise RuntimeError("Defina OPENAI_API_KEY, ZAPI_INSTANCE, ZAPI_TOKEN, ZAPI_CLIENT_TOKEN no .env")

OPENAI = OpenAI(api_key=OPENAI_API_KEY)

app = FastAPI(title="Paginatto WhatsApp Bot — Iara")

# --- Memória simples (troque por Redis em produção) ---
SESSIONS: Dict[str, List[Dict[str, str]]] = {}
SEEN_IDS: set[str] = set()

ORDERS_BY_NO: Dict[str, Dict[str, Any]] = {}
ORDERS_BY_CPF: Dict[str, List[str]] = {}
ORDERS_BY_EMAIL: Dict[str, List[str]] = {}
LAST_ORDER_BY_PHONE: Dict[str, str] = {}  # phone → order_no

ZAPI_BASE = f"https://api.z-api.io/instances/{ZAPI_INSTANCE}/token/{ZAPI_TOKEN}"
ZAPI_HEADERS = {"Client-Token": ZAPI_CLIENT_TOKEN, "Content-Type": "application/json"}

# --- Util ---
CPF_RX = re.compile(r"(\d{3}\.?\d{3}\.?\d{3}-?\d{2})")
ORDER_RX = re.compile(r"\b(\d{6,12})\b")  # ajuste se seu nº de pedido tiver outro formato


def digits_only(v: str) -> str:
    return re.sub(r"\D+", "", v or "")


def normalize_phone(v: str) -> str:
    d = digits_only(v)
    if d and not d.startswith("55"):
        d = "55" + d
    return d


def parse_bool(x: Any) -> bool:
    return str(x).lower() in {"1", "true", "yes"}


async def zapi_send_text(phone: str, message: str) -> dict:
    url = f"{ZAPI_BASE}{SEND_TEXT_PATH}"
    payload = {"phone": phone, "message": message}
    async with httpx.AsyncClient(timeout=20) as http:
        r = await http.post(url, headers=ZAPI_HEADERS, json=payload)
        data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {"text": r.text}
        if r.status_code >= 300:
            raise HTTPException(status_code=502, detail={"zapi_error": data})
        return data


def cpf_from_text(text: str) -> Optional[str]:
    m = CPF_RX.search(text or "")
    if m:
        return digits_only(m.group(1))
    return None


def orderno_from_text(text: str) -> Optional[str]:
    m = ORDER_RX.search(text or "")
    if m:
        return m.group(1)
    return None


def order_context_by_keys(phone: str, text: str) -> Optional[Dict[str, Any]]:
    # 1) último pedido visto para este telefone (via webhook CartPanda)
    order_no = LAST_ORDER_BY_PHONE.get(phone)
    if order_no and order_no in ORDERS_BY_NO:
        return ORDERS_BY_NO[order_no]

    # 2) mensagem contém nº de pedido
    order_no = orderno_from_text(text or "")
    if order_no and order_no in ORDERS_BY_NO:
        return ORDERS_BY_NO[order_no]

    # 3) mensagem contém CPF
    cpf = cpf_from_text(text or "")
    if cpf and cpf in ORDERS_BY_CPF:
        nos = ORDERS_BY_CPF[cpf]
        if nos:
            return ORDERS_BY_NO.get(nos[-1])  # mais recente indexado

    return None


async def cartpanda_lookup(order_no: Optional[str] = None, cpf: Optional[str] = None, email: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Opcional: consulta API do CartPanda, se você tiver acesso.
    Deixe CARTPANDA_API_BASE/CARTPANDA_API_TOKEN vazios para pular.
    Ajuste os endpoints conforme sua conta.
    """
    if not (CARTPANDA_API_BASE and CARTPANDA_API_TOKEN):
        return None
    headers = {"Authorization": f"Bearer {CARTPANDA_API_TOKEN}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=20) as http:
        try:
            if order_no:
                # Exemplo genérico (ajuste para a API real do CartPanda)
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


SYSTEM_TEMPLATE = (
    "Você é {assistant} da {brand}. Fale português do Brasil. Tom humano, direto, cordial. "
    "Nada de menus. Faça perguntas curtas quando útil. Proponha próximo passo. "
    "Se for pagamento: pergunte em que etapa travou e ofereça reenvio do link. "
    "Segurança: {security}. Cite site oficial {site} e suporte {support} quando fizer sentido. "
    "Nunca peça senhas/códigos. Não invente dados de pedido. Responda em até 3 parágrafos curtos."
)


def system_prompt(extra_context: Optional[Dict[str, Any]]) -> str:
    base = SYSTEM_TEMPLATE.format(
        assistant=ASSISTANT_NAME,
        brand=BRAND_NAME,
        security=SECURITY_BLURB,
        site=SITE_URL,
        support=SUPPORT_URL,
    )
    if not extra_context:
        return base
    parts = [base, "Contexto do pedido (se disponível):"]
    for k, v in compact_order_view(extra_context).items():
        if v:
            parts.append(f"- {k}: {v}")
    if extra_context.get("cart_token") and CHECKOUT_RESUME_BASE:
        parts.append(f"- resume_link: {CHECKOUT_RESUME_BASE}{extra_context['cart_token']}")
    return "\n".join(parts)


async def llm_reply(history: List[Dict[str, str]], ctx: Optional[Dict[str, Any]]) -> str:
    messages = [
        {"role": "system", "content": system_prompt(ctx)}
    ] + history[-12:]
    resp = OPENAI.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.3,
        messages=messages,
    )
    return resp.choices[0].message.content.strip()


# --- Webhooks ---
@app.get("/health")
async def health():
    return {"ok": True}


@app.post("/webhook/zapi/receive")
async def zapi_receive(request: Request):
    data = await request.json()

    msg_id = (
        data.get("messageId") or data.get("id") or data.get("message", {}).get("id") or (data.get("messages", [{}])[0] or {}).get("id")
    )
    if msg_id and msg_id in SEEN_IDS:
        return JSONResponse({"status": "duplicate_ignored"})
    if msg_id:
        SEEN_IDS.add(msg_id)

    phone = (
        data.get("phone")
        or data.get("from")
        or data.get("chatId")
        or (data.get("contact", {}) or {}).get("phone")
        or (data.get("message", {}) or {}).get("from")
        or (data.get("messages", [{}])[0] or {}).get("from")
        or ""
    )
    text = (
        data.get("text")
        or data.get("body")
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

    # sessão + contexto de pedido
    convo = SESSIONS.setdefault(phone, [])
    convo.append({"role": "user", "content": text})

    ctx = order_context_by_keys(phone, text)
    if ctx is None:
        # tentativa opcional de lookup direto via API
        ctx = await cartpanda_lookup(order_no=orderno_from_text(text), cpf=cpf_from_text(text))

    try:
        reply = await llm_reply(convo, ctx)
    except Exception:
        reply = (
            f"Sou {ASSISTANT_NAME}. Tive um erro para responder agora. "
            f"Pode reformular? Se preferir, fale 'suporte' que encaminho para atendimento humano."
        )

    convo.append({"role": "assistant", "content": reply})

    try:
        out = await zapi_send_text(phone, reply)
    except HTTPException as ex:
        return JSONResponse({"status": "sent_error", "detail": ex.detail})

    return JSONResponse({"status": "sent", "zapi": out})


@app.post("/webhook/zapi/status")
async def zapi_status(request: Request):
    _ = await request.json()
    return {"ok": True}


@app.post("/webhook/cartpanda/order")
async def cartpanda_order(request: Request):
    data = await request.json()
    # Aceita payload genérico do CartPanda. Ajuste os campos conforme seu webhook real.
    event = data.get("event") or data.get("type")
    d = data.get("data") or data

    order_no = str(d.get("order_no") or d.get("number") or d.get("id") or d.get("orderNumber") or "").strip()
    customer = d.get("customer") or {}
    email = (customer.get("email") or d.get("email") or "").strip().lower()
    phone = normalize_phone(customer.get("phone") or d.get("phone") or "")
    cpf = digits_only(customer.get("document") or d.get("document") or d.get("cpf") or "")
    payment_status = d.get("payment_status") or d.get("status")
    cart_token = d.get("cart_token") or d.get("cartToken")
    checkout_url = d.get("checkout_url") or d.get("checkoutUrl")
    total = d.get("total") or d.get("amount")

    order_obj = {
        "event": event,
        "order_no": order_no,
        "customer": {"name": customer.get("name") or d.get("name"), "email": email, "phone": phone, "document": cpf},
        "payment_status": payment_status,
        "cart_token": cart_token,
        "checkout_url": checkout_url,
        "total": total,
    }

    if order_no:
        ORDERS_BY_NO[order_no] = order_obj
    if cpf:
        ORDERS_BY_CPF.setdefault(cpf, []).append(order_no)
    if email:
        ORDERS_BY_EMAIL.setdefault(email, []).append(order_no)
    if phone and order_no:
        LAST_ORDER_BY_PHONE[phone] = order_no

    return {"indexed": True, "order_no": order_no}


@app.post("/webhook/cartpanda/support")
async def cartpanda_support(request: Request):
    data = await request.json()
    # Payload sugerido: {event:"support.requested", data:{order_no, message, customer:{name,email,phone,document}}}
    d = data.get("data") or data
    order_no = str(d.get("order_no") or d.get("number") or d.get("id") or "").strip()
    phone = normalize_phone((d.get("customer") or {}).get("phone") or d.get("phone") or "")

    # Atualiza último pedido vinculado ao telefone para conversas seguintes
    if phone and order_no:
        LAST_ORDER_BY_PHONE[phone] = order_no

    # Se ainda não temos este pedido, crie um mínimo
    if order_no and order_no not in ORDERS_BY_NO:
        ORDERS_BY_NO[order_no] = compact_order_view({"order_no": order_no, "customer": d.get("customer") or {}})

    return {"ok": True, "linked_phone": phone, "order_no": order_no}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

