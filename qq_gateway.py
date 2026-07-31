from __future__ import annotations

import html
import hashlib
import json
import mimetypes
import re
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

from bridge_core import Config

try:
    import websocket
except ImportError:
    websocket = None


USER_AGENT = "CodexChatBridge/0.1 macOS"


def http_json(
    method: str,
    url: str,
    payload: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 20,
) -> Dict[str, Any]:
    body = None
    request_headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
    if headers:
        request_headers.update(headers)
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request_headers["Content-Type"] = "application/json; charset=utf-8"
    request = Request(url, data=body, headers=request_headers, method=method)
    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {url} HTTP {exc.code}: {raw[:500]}") from exc
    except URLError as exc:
        raise RuntimeError(f"{method} {url} 失败: {exc}") from exc
    if not raw:
        return {}
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise RuntimeError(f"{method} {url} 返回的不是 JSON 对象")
    if value.get("code") not in {None, 0, "0"}:
        raise RuntimeError(f"QQ API 错误: {json.dumps(value, ensure_ascii=False)[:500]}")
    return value


def http_put_bytes(url: str, body: bytes, timeout: int = 60) -> None:
    request = Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/octet-stream",
            "Content-Length": str(len(body)),
            "User-Agent": USER_AGENT,
        },
        method="PUT",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            response.read()
    except HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"QQ 文件分片上传 HTTP {exc.code}: {raw[:500]}") from exc
    except URLError as exc:
        raise RuntimeError(f"QQ 文件分片上传失败: {exc}") from exc


class QQApi:
    def __init__(self, config: Config) -> None:
        self.config = config
        self._token = ""
        self._expires_at = 0.0
        self._token_lock = threading.Lock()
        self._send_lock = threading.Lock()

    def access_token(self) -> str:
        with self._token_lock:
            if self._token and self._expires_at > time.time() + 60:
                return self._token
            value = http_json(
                "POST",
                self.config.qq_auth_url,
                {
                    "appId": self.config.qq_app_id,
                    "clientSecret": self.config.qq_app_secret,
                },
                timeout=15,
            )
            token = str(value.get("access_token") or "")
            if not token:
                raise RuntimeError("QQ access token 响应缺少 access_token")
            expires_in = int(value.get("expires_in") or 7200)
            self._token = token
            self._expires_at = time.time() + max(60, expires_in - 60)
            return token

    def headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"QQBot {self.access_token()}",
            "X-Union-Appid": self.config.qq_app_id,
            "User-Agent": USER_AGENT,
        }

    def api_url(self, path: str) -> str:
        return f"{self.config.qq_api_base}/{path.lstrip('/')}"

    def gateway_url(self) -> str:
        value = http_json("GET", self.api_url("/gateway"), headers=self.headers(), timeout=15)
        url = str(value.get("url") or "")
        if not url:
            raise RuntimeError("QQ Gateway 响应缺少 url")
        return url

    def send_c2c(
        self,
        openid: str,
        content: str,
        msg_id: str = "",
        msg_seq: int = 1,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "content": content,
            "msg_type": 0,
            "msg_seq": msg_seq,
        }
        if msg_id:
            payload["msg_id"] = msg_id
        path = f"/v2/users/{quote(openid, safe='')}/messages"
        with self._send_lock:
            return http_json(
                "POST", self.api_url(path), payload=payload, headers=self.headers(), timeout=20
            )

    def send_c2c_markdown(
        self,
        openid: str,
        content: str,
        msg_id: str = "",
        msg_seq: int = 1,
        keyboard: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "content": "",
            "msg_type": 2,
            "markdown": {"content": content},
            "msg_seq": msg_seq,
        }
        if keyboard:
            payload["keyboard"] = keyboard
        if msg_id:
            payload["msg_id"] = msg_id
        path = f"/v2/users/{quote(openid, safe='')}/messages"
        with self._send_lock:
            return http_json(
                "POST", self.api_url(path), payload=payload, headers=self.headers(), timeout=20
            )

    def send_c2c_typing(
        self, openid: str, msg_id: str, msg_seq: int = 1, seconds: int = 60
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "msg_type": 6,
            "input_notify": {"input_type": 1, "input_second": max(1, min(60, seconds))},
            "msg_id": msg_id,
            "msg_seq": msg_seq,
        }
        path = f"/v2/users/{quote(openid, safe='')}/messages"
        with self._send_lock:
            return http_json(
                "POST", self.api_url(path), payload=payload, headers=self.headers(), timeout=20
            )

    def send_c2c_media(
        self,
        openid: str,
        file_info: str,
        msg_id: str = "",
        msg_seq: int = 1,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "msg_type": 7,
            "media": {"file_info": file_info},
            "msg_seq": msg_seq,
        }
        if msg_id:
            payload["msg_id"] = msg_id
        path = f"/v2/users/{quote(openid, safe='')}/messages"
        with self._send_lock:
            return http_json(
                "POST", self.api_url(path), payload=payload, headers=self.headers(), timeout=30
            )

    def upload_c2c_image(self, openid: str, image_path: Path) -> str:
        data = image_path.read_bytes()
        whole_md5 = hashlib.md5(data).hexdigest()
        whole_sha1 = hashlib.sha1(data).hexdigest()
        first_md5 = hashlib.md5(data[:10002432]).hexdigest()
        encoded_openid = quote(openid, safe="")
        prepare = http_json(
            "POST",
            self.api_url(f"/v2/users/{encoded_openid}/upload_prepare"),
            payload={
                "file_type": 1,
                "file_size": str(len(data)),
                "file_name": image_path.name,
                "md5": whole_md5,
                "sha1": whole_sha1,
                "md5_10m": first_md5,
            },
            headers=self.headers(),
            timeout=30,
        )
        upload_id = str(prepare.get("upload_id") or "")
        parts = prepare.get("parts")
        if not upload_id or not isinstance(parts, list):
            raise RuntimeError("QQ 图片预上传响应不完整")

        if not all(isinstance(part, dict) for part in parts):
            raise RuntimeError("QQ 图片分片信息无效")
        offset = 0
        for part in sorted(parts, key=lambda value: int(value.get("index", 0))):
            part_index = int(part.get("index", 0))
            part_size = int(part.get("block_size") or prepare.get("block_size") or 0)
            presigned_url = str(part.get("presigned_url") or "")
            if part_size <= 0 or not presigned_url:
                raise RuntimeError("QQ 图片分片响应缺少大小或上传地址")
            chunk = data[offset : offset + part_size]
            if not chunk:
                raise RuntimeError("QQ 图片分片数量与文件大小不匹配")
            http_put_bytes(presigned_url, chunk)
            http_json(
                "POST",
                self.api_url(f"/v2/users/{encoded_openid}/upload_part_finish"),
                payload={
                    "upload_id": upload_id,
                    "part_index": part_index,
                    "block_size": str(len(chunk)),
                    "md5": hashlib.md5(chunk).hexdigest(),
                },
                headers=self.headers(),
                timeout=30,
            )
            offset += len(chunk)
        if offset != len(data):
            raise RuntimeError("QQ 图片上传未覆盖完整文件")

        merged = http_json(
            "POST",
            self.api_url(f"/v2/users/{encoded_openid}/files"),
            payload={
                "file_type": 1,
                "srv_send_msg": False,
                "file_name": image_path.name,
                "upload_id": upload_id,
            },
            headers=self.headers(),
            timeout=60,
        )
        file_info = str(merged.get("file_info") or "")
        if not file_info:
            raise RuntimeError("QQ 图片上传响应缺少 file_info")
        return file_info


def download_c2c_images(
    attachments: List[Dict[str, Any]],
    target_dir: Path,
    max_bytes: int,
    max_images: int,
) -> List[Path]:
    target_dir.mkdir(parents=True, exist_ok=True)
    images: List[Path] = []
    for index, attachment in enumerate(attachments):
        if len(images) >= max_images:
            break
        content_type = str(attachment.get("content_type") or "").lower()
        if not content_type.startswith("image/"):
            continue
        declared_size = int(attachment.get("size") or 0)
        if declared_size > max_bytes:
            raise ValueError(f"图片 {index + 1} 超过 {max_bytes // 1024 // 1024}MB 限制")
        url = str(attachment.get("url") or "").strip()
        parsed = urlparse(url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("QQ 图片下载地址无效")
        request = Request(url, headers={"Accept": "image/*", "User-Agent": USER_AGENT})
        try:
            with urlopen(request, timeout=30) as response:
                final_url = urlparse(response.geturl())
                if final_url.scheme != "https" or not final_url.hostname:
                    raise ValueError("QQ 图片重定向地址无效")
                response_type = str(response.headers.get("Content-Type") or "").lower()
                if response_type and not response_type.startswith("image/"):
                    raise ValueError("QQ 附件不是图片")
                response_size = int(response.headers.get("Content-Length") or 0)
                if response_size > max_bytes:
                    raise ValueError(f"图片 {index + 1} 超过大小限制")
                data = response.read(max_bytes + 1)
        except HTTPError as exc:
            raise RuntimeError(f"下载 QQ 图片 HTTP {exc.code}") from exc
        except URLError as exc:
            raise RuntimeError(f"下载 QQ 图片失败: {exc}") from exc
        if len(data) > max_bytes:
            raise ValueError(f"图片 {index + 1} 超过大小限制")
        if not data:
            raise ValueError("QQ 图片内容为空")
        raw_name = Path(str(attachment.get("filename") or "")).name
        safe_name = re.sub(r"[^A-Za-z0-9._-]", "_", raw_name).strip("._")
        suffix = Path(safe_name).suffix.lower()
        if suffix not in {".png", ".jpg", ".jpeg", ".gif", ".webp"}:
            suffix = mimetypes.guess_extension(content_type.split(";", 1)[0]) or ".img"
        stem = Path(safe_name).stem[:80] or "image"
        path = target_dir / f"{index + 1:02d}-{stem}{suffix}"
        path.write_bytes(data)
        images.append(path)
    return images


def clean_message_content(value: str) -> str:
    text = html.unescape(value or "")
    text = re.sub(r"<@!?\w+>", "", text)
    return text.strip()


def extract_c2c_event(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if payload.get("op") != 0 or payload.get("t") != "C2C_MESSAGE_CREATE":
        return None
    data = payload.get("d")
    if not isinstance(data, dict):
        return None
    author = data.get("author")
    openid = str(author.get("user_openid") or "") if isinstance(author, dict) else ""
    message_id = str(data.get("id") or "")
    content = clean_message_content(str(data.get("content") or ""))
    raw_attachments = data.get("attachments")
    attachments: List[Dict[str, Any]] = []
    if isinstance(raw_attachments, list):
        for item in raw_attachments:
            if not isinstance(item, dict):
                continue
            url = str(item.get("url") or "").strip()
            content_type = str(item.get("content_type") or "").strip().lower()
            if not url:
                continue
            attachments.append(
                {
                    "url": url,
                    "filename": Path(str(item.get("filename") or "image")).name,
                    "content_type": content_type,
                    "size": int(item.get("size") or 0),
                    "width": int(item.get("width") or 0),
                    "height": int(item.get("height") or 0),
                }
            )
    if not openid or not message_id or (not content and not attachments):
        return None
    return {
        "openid": openid,
        "message_id": message_id,
        "content": content,
        "attachments": attachments,
    }


class QQGatewayClient:
    def __init__(
        self,
        config: Config,
        api: QQApi,
        on_event: Callable[[Dict[str, Any]], None],
        on_ready: Callable[[], None],
    ) -> None:
        if websocket is None:
            raise RuntimeError("缺少 websocket-client，请先运行 scripts/setup.sh")
        self.config = config
        self.api = api
        self.on_event = on_event
        self.on_ready = on_ready
        self.stop_event = threading.Event()
        self.ws = None
        self.latest_seq: Optional[int] = None
        self.session_id = ""
        self._send_lock = threading.Lock()
        self._heartbeat_stop: Optional[threading.Event] = None
        self._seen: Dict[str, float] = {}

    def stop(self) -> None:
        self.stop_event.set()
        self._stop_heartbeat()
        with self._send_lock:
            if self.ws:
                self.ws.close()

    def run_forever(self) -> None:
        while not self.stop_event.is_set():
            try:
                url = self.api.gateway_url()
                print(f"[qq-gateway] connecting {url}", flush=True)
                self.ws = websocket.WebSocketApp(
                    url,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self.ws.run_forever(ping_interval=0)
            except Exception as exc:
                print(f"[qq-gateway] {exc}", flush=True)
            if not self.stop_event.wait(5):
                print("[qq-gateway] reconnecting", flush=True)

    def _send(self, payload: Dict[str, Any]) -> None:
        with self._send_lock:
            if not self.ws:
                raise RuntimeError("QQ Gateway 尚未连接")
            self.ws.send(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))

    def _identify(self) -> None:
        self._send(
            {
                "op": 2,
                "d": {
                    "token": f"QQBot {self.api.access_token()}",
                    "intents": self.config.qq_intents,
                    "shard": [0, 1],
                    "properties": {
                        "$os": "macos",
                        "$browser": "codex-chat-bridge",
                        "$device": "codex-chat-bridge",
                    },
                },
            }
        )

    def _start_heartbeat(self, interval_ms: int) -> None:
        self._stop_heartbeat()
        stop = threading.Event()
        self._heartbeat_stop = stop
        interval = max(5.0, interval_ms / 1000.0 * 0.9)

        def loop() -> None:
            while not stop.wait(interval):
                try:
                    self._send({"op": 1, "d": self.latest_seq})
                except Exception as exc:
                    print(f"[qq-heartbeat] {exc}", flush=True)
                    return

        threading.Thread(target=loop, name="qq-heartbeat", daemon=True).start()

    def _stop_heartbeat(self) -> None:
        if self._heartbeat_stop:
            self._heartbeat_stop.set()
            self._heartbeat_stop = None

    def _dedup(self, message_id: str) -> bool:
        now = time.time()
        self._seen = {key: expires for key, expires in self._seen.items() if expires > now}
        if message_id in self._seen:
            return False
        self._seen[message_id] = now + 600
        return True

    def _on_message(self, _ws: Any, raw: str) -> None:
        try:
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                return
            if payload.get("s") is not None:
                self.latest_seq = int(payload["s"])
            op = payload.get("op")
            if op == 10:
                interval = int((payload.get("d") or {}).get("heartbeat_interval", 45000))
                self._start_heartbeat(interval)
                self._identify()
                return
            if op == 7:
                self.ws.close()
                return
            if op == 9:
                self.session_id = ""
                self.latest_seq = None
                self.ws.close()
                return
            if op != 0:
                return
            event_type = str(payload.get("t") or "")
            if event_type == "READY":
                data = payload.get("d") or {}
                self.session_id = str(data.get("session_id") or "")
                print(f"[qq-gateway] READY session={self.session_id[:8]}", flush=True)
                self.on_ready()
                return
            event = extract_c2c_event(payload)
            if event and self._dedup(event["message_id"]):
                self.on_event(event)
        except Exception as exc:
            print(f"[qq-gateway] event error: {exc}", flush=True)

    def _on_error(self, _ws: Any, error: Any) -> None:
        print(f"[qq-gateway] websocket error: {error}", flush=True)

    def _on_close(self, _ws: Any, code: Any, reason: Any) -> None:
        self._stop_heartbeat()
        print(f"[qq-gateway] closed code={code} reason={reason}", flush=True)
