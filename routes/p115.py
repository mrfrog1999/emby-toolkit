# routes/p115.py
import logging
from flask import redirect
import threading
from datetime import datetime, timedelta
import json
import os
import re
import time
import requests
from flask import Blueprint, jsonify, request, redirect
from extensions import admin_required
from database import settings_db
from handler.p115_service import P115Service, get_config
import constants
from functools import lru_cache, wraps

# 115扫码登录相关变量 (OAuth 2.0 + PKCE 模式)
_qrcode_data = {
    "qrcode": None,        # 二维码内容
    "uid": None,           # 设备码
    "time": None,         # 时间戳
    "sign": None,         # 签名
    "code_verifier": None,# PKCE verifier
    "access_token": None,  # 最终获取的 access_token
    "refresh_token": None  # 刷新token
}
p115_bp = Blueprint('115_bp', __name__, url_prefix='/api/p115')
logger = logging.getLogger(__name__)

# --- 115扫码登录相关API (OAuth 2.0 + PKCE 模式) ---

def _generate_pkce_pair():
    """生成 PKCE 的 verifier 和 challenge"""
    import base64
    import os
    import hashlib
    
    # 1. 生成 43~128 位的随机字符串 (code_verifier)
    verifier = base64.urlsafe_b64encode(os.urandom(40)).decode('utf-8').rstrip('=')
    
    # 2. 计算 SHA256 并进行 Base64Url 编码 (code_challenge)
    digest = hashlib.sha256(verifier.encode('ascii')).digest()
    challenge = base64.urlsafe_b64encode(digest).decode('utf-8').rstrip('=')
    
    return verifier, challenge

def _generate_qrcode():
    """生成115扫码登录二维码 (OAuth 2.0 + PKCE 新版API)"""
    try:
        # 1. 生成 PKCE 密钥对
        verifier, challenge = _generate_pkce_pair()
        
        # 2. 调用获取二维码接口
        url = "https://passportapi.115.com/open/authDeviceCode"
        payload = {
            "client_id": "100196261",  # 115开发者后台的AppID
            "code_challenge": challenge,
            "code_challenge_method": "sha256"
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        
        resp = requests.post(url, data=payload, headers=headers, timeout=10)
        result = resp.json()
        
        if result.get('state'):
            qr_data = result.get('data', {})
            _qrcode_data['qrcode'] = qr_data.get('qrcode')
            _qrcode_data['uid'] = qr_data.get('uid')
            _qrcode_data['time'] = qr_data.get('time')
            _qrcode_data['sign'] = qr_data.get('sign')
            _qrcode_data['code_verifier'] = verifier
            _qrcode_data['access_token'] = None
            _qrcode_data['refresh_token'] = None
            return qr_data
        else:
            logger.error(f"获取二维码失败: {result.get('message')}")
            return None
    except Exception as e:
        logger.error(f"生成二维码失败: {e}")
        return None

def _check_qrcode_status():
    """检查二维码扫码状态 (OAuth 2.0 + PKCE 新版API)"""
    if not _qrcode_data.get('uid') or not _qrcode_data.get('time'):
        return {"status": "waiting", "message": "请先获取二维码"}
    
    try:
        # 1. 先轮询二维码状态
        url = "https://qrcodeapi.115.com/get/status/"
        params = {
            "uid": _qrcode_data.get('uid'),
            "time": _qrcode_data.get('time'),
            "sign": _qrcode_data.get('sign')
        }
        
        resp = requests.get(url, params=params, timeout=30)
        result = resp.json()
        
        state = result.get('state')
        
        # state=0 表示二维码无效/过期
        if state == 0:
            return {"status": "expired", "message": "二维码已过期，请重新获取"}
        
        # state=1 需要看 status 字段
        if state == 1:
            data = result.get('data', {})
            status = data.get('status')
            
            if status == 1:
                # 已扫码，等待确认
                return {"status": "waiting", "message": "已扫码，等待手机端确认..."}
            elif status == 2:
                # 已确认，现在需要换取 token
                # 2. 用 device code 换取 access_token
                token_url = "https://passportapi.115.com/open/deviceCodeToToken"
                token_payload = {
                    "uid": _qrcode_data.get('uid'),
                    "code_verifier": _qrcode_data.get('code_verifier')
                }
                token_headers = {"Content-Type": "application/x-www-form-urlencoded"}
                
                token_resp = requests.post(token_url, data=token_payload, headers=token_headers, timeout=10)
                token_result = token_resp.json()
                
                if token_result.get('state'):
                    token_data = token_result.get('data', {})
                    access_token = token_data.get('access_token')
                    refresh_token = token_data.get('refresh_token')
                    
                    if access_token:
                        _qrcode_data['access_token'] = access_token
                        _qrcode_data['refresh_token'] = refresh_token
                        
                        # 3. 用 access_token 获取用户信息来验证
                        user_info_url = "https://proapi.115.com/open/user/info"
                        user_headers = {"Authorization": f"Bearer {access_token}"}
                        user_resp = requests.get(user_info_url, headers=user_headers, timeout=10)
                        user_result = user_resp.json()
                        
                        # 构造 cookies 格式 (UID=...; CID=...; SEID=...)
                        # 从 access_token 解析或直接使用
                        cookies = f"UID={_qrcode_data.get('uid')}; CID={_qrcode_data.get('uid')}; SEID={access_token}"
                        
                        return {
                            "status": "success", 
                            "message": "登录成功",
                            "cookies": cookies,
                            "user_info": user_result.get('data', {})
                        }
                else:
                    return {"status": "error", "message": "获取Token失败: " + token_result.get('message', '未知错误')}
            else:
                return {"status": "waiting", "message": data.get('msg', '等待扫码...')}
        
        return {"status": "waiting", "message": "等待扫码..."}
            
    except requests.exceptions.Timeout:
        return {"status": "waiting", "message": "轮询超时，继续等待..."}
    except Exception as e:
        logger.error(f"检查二维码状态失败: {e}")
        return {"status": "error", "message": str(e)}

@p115_bp.route('/qrcode', methods=['POST'])
@admin_required
def get_qrcode():
    """获取115登录二维码"""
    data = _generate_qrcode()
    if data:
        return jsonify({
            "success": True, 
            "data": {
                "qrcode": data.get('qrcode'),
                "uid": data.get('uid')
            }
        })
    return jsonify({"success": False, "message": "获取二维码失败"}), 500

@p115_bp.route('/qrcode/status', methods=['GET'])
@admin_required
def check_qrcode_status():
    """检查扫码登录状态"""
    status = _check_qrcode_status()
    
    if status.get('status') == 'success':
        # ★★★ 扫码成功后将 Token 保存到配置 ★★★
        access_token = _qrcode_data.get('access_token')
        if access_token:
            try:
                from config_manager import save_config
                config = get_config()
                config[constants.CONFIG_OPTION_115_TOKEN] = access_token
                save_config(config)
                logger.info("  ✅ [115] 扫码获取的 Token 已自动保存到配置")
            except Exception as e:
                logger.error(f"  ❌ 保存 Token 到配置失败: {e}")
        
        return jsonify({
            "success": True,
            "status": "success",
            "message": "登录成功",
            "cookies": status.get('cookies'),
            "token": access_token  # 同时返回 Token 供前端确认
        })
    elif status.get('status') == 'expired':
        return jsonify({
            "success": False,
            "status": "expired",
            "message": "二维码已过期，请重新获取"
        })
    elif status.get('status') == 'waiting':
        return jsonify({
            "success": True,
            "status": "waiting",
            "message": "等待扫码..."
        })
    else:
        return jsonify({
            "success": False,
            "status": "error",
            "message": status.get('message', '检查状态失败')
        }), 500

# --- 简单的令牌桶/计数器限流器 ---
class RateLimiter:
    def __init__(self, max_requests=3, period=2):
        self.max_requests = max_requests  # 周期内最大请求数
        self.period = period              # 周期（秒）
        self.tokens = max_requests
        self.last_sync = datetime.now()
        self.lock = threading.Lock()

    def consume(self):
        with self.lock:
            now = datetime.now()
            # 补充令牌
            elapsed = (now - self.last_sync).total_seconds()
            self.tokens = min(self.max_requests, self.tokens + elapsed * (self.max_requests / self.period))
            self.last_sync = now

            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

@p115_bp.route('/status', methods=['GET'])
@admin_required
def get_115_status():
    """检查 115 凭证状态 (分别检查 Token 和 Cookie)"""
    try:
        from handler.p115_service import P115Service, get_config
        config = get_config()
        
        token = config.get(constants.CONFIG_OPTION_115_TOKEN, "").strip()
        cookie = config.get(constants.CONFIG_OPTION_115_COOKIES, "").strip()
        
        result = {
            "has_token": bool(token),
            "has_cookie": bool(cookie),
            "valid": False,
            "msg": "",
            "user_info": None
        }
        
        # 优先检查 Token
        if token:
            openapi_client = P115Service.get_openapi_client()
            if openapi_client:
                try:
                    user_resp = openapi_client.get_user_info()
                    if user_resp and user_resp.get('state'):
                        result["valid"] = True
                        result["msg"] = "Token 有效 (OpenAPI)"
                        result["user_info"] = user_resp.get('data', {})
                        # 如果也有 Cookie，一并提示
                        if cookie:
                            result["msg"] = "Token + Cookie 均已配置"
                        return jsonify({"status": "success", "data": result})
                except Exception as e:
                    result["msg"] = f"Token 无效: {str(e)}"
            else:
                result["msg"] = "Token 初始化失败"
        
        # 如果没有 Token，检查 Cookie
        if cookie and not result.get("user_info"):
            cookie_client = P115Service.get_cookie_client()
            if cookie_client:
                result["valid"] = True
                result["msg"] = "仅配置 Cookie (播放专用)"
                return jsonify({"status": "success", "data": result})
            else:
                result["msg"] = "Cookie 无效或 p115client 未安装"
        
        if not token and not cookie:
            result["msg"] = "未配置任何凭证"
            
        return jsonify({"status": "success", "data": result})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@p115_bp.route('/dirs', methods=['GET'])
@admin_required
def list_115_directories():
    """获取 115 目录列表"""
    client = P115Service.get_client()
    if not client:
        return jsonify({"status": "error", "message": "无法初始化 115 客户端，请检查凭证"}), 500

    try:
        cid = int(request.args.get('cid', 0))
    except:
        cid = 0
    
    try:
        request_payload = {'cid': cid, 'limit': 1000}
        
        resp = client.fs_files(request_payload)
        
        if not resp.get('state'):
            return jsonify({"success": False, "message": resp.get('error_msg', '获取失败')}), 500
            
        data = resp.get('data', [])
        
        dirs = []
        
        for item in data:
            # 官方文档：fc='0' 代表文件夹
            if str(item.get('fc')) == '0':
                dirs.append({
                    "id": str(item.get('fid')),
                    "name": item.get('fn'),
                    "parent_id": item.get('pid')
                })
        
        current_name = '根目录'
        if cid != 0 and resp.get('path'):
            # path 数组中官方返回的是 file_name
            current_name = resp.get('path')[-1].get('file_name') or resp.get('path')[-1].get('fn', '未知目录')
                
        return jsonify({
            "success": True, 
            "data": dirs,
            "current": {
                "id": str(cid),
                "name": current_name
            }
        })
        
    except Exception as e:
        logger.error(f"  ❌ [115目录] 获取目录异常: {e}", exc_info=True)
        return jsonify({"success": False, "message": str(e)}), 500

@p115_bp.route('/mkdir', methods=['POST'])
@admin_required
def create_115_directory():
    """创建 115 目录"""
    data = request.json
    pid = data.get('pid') or data.get('cid')
    name = data.get('name')
    
    if not name:
        return jsonify({"status": "error", "message": "目录名称不能为空"}), 400
        
    client = P115Service.get_client()
    if not client:
        return jsonify({"status": "error", "message": "无法初始化 115 客户端"}), 500
        
    try:
        resp = client.fs_mkdir(name, pid)
        if resp.get('state'):
            return jsonify({"status": "success", "data": resp})
        else:
            return jsonify({"status": "error", "message": resp.get('error_msg', '创建失败')}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@p115_bp.route('/sorting_rules', methods=['GET', 'POST'])
@admin_required
def handle_sorting_rules():
    """管理 115 分类规则"""
    if request.method == 'GET':
        raw_rules = settings_db.get_setting(constants.DB_KEY_115_SORTING_RULES)
        rules = []
        if raw_rules:
            if isinstance(raw_rules, list):
                rules = raw_rules
            elif isinstance(raw_rules, str):
                try:
                    parsed = json.loads(raw_rules)
                    if isinstance(parsed, list):
                        rules = parsed
                except Exception as e:
                    logger.error(f"解析分类规则 JSON 失败: {e}")
        
        # 确保每个规则都有 id
        for r in rules:
            if 'id' not in r:
                r['id'] = str(int(time.time() * 1000))
                
        return jsonify(rules)
    
    if request.method == 'POST':
        rules = request.json
        if not isinstance(rules, list):
            rules = []
        
        # ★★★ 修复：精准计算基于 p115_media_root_cid 的相对层级路径 ★★★
        client = P115Service.get_client()
        if client:
            config = get_config()
            # 获取用户配置的媒体库根目录 CID
            media_root_cid = str(config.get(constants.CONFIG_OPTION_115_MEDIA_ROOT_CID, '0'))
            
            for rule in rules:
                cid = rule.get('cid')
                if cid and str(cid) != '0':
                    try:
                        time.sleep(0.5) # 防风控限流
                        
                        payload = {'cid': cid, 'limit': 1, 'record_open_time': 0, 'count_folders': 0}
                        if hasattr(client, 'fs_files_app'):
                            dir_info = client.fs_files(payload)
                            
                        path_nodes = dir_info.get('path', [])
                        
                        start_idx = 0
                        found_root = False
                        
                        # 在链路中寻找“媒体库根目录”
                        if media_root_cid == '0':
                            start_idx = 1 # 如果没配根目录，默认跳过 115 物理“根目录”
                            found_root = True
                        else:
                            for i, node in enumerate(path_nodes):
                                if str(node.get('cid')) == media_root_cid:
                                    start_idx = i + 1 # 从根目录的下一级开始取
                                    found_root = True
                                    break
                        
                        if found_root and start_idx < len(path_nodes):
                            # 官方文档：paths 数组里返回的是 file_name
                            rel_segments = [str(n.get('file_name') or n.get('fn')).strip() for n in path_nodes[start_idx:]]
                            rule['category_path'] = "/".join(rel_segments)
                        else:
                            # 兜底：如果层级异常或没找到根目录，用规则里配的名称
                            rule['category_path'] = rule.get('dir_name', '')
                            
                        logger.info(f"  📂 已为规则 '{rule.get('name')}' 自动计算并保存路径: {rule.get('category_path')}")
                        
                    except Exception as e:
                        logger.warning(f"  ⚠️ 获取规则 '{rule.get('name')}' 路径失败: {e}")
                        if not rule.get('category_path'):
                            rule['category_path'] = rule.get('dir_name', '')
        
        settings_db.save_setting(constants.DB_KEY_115_SORTING_RULES, rules)
        return jsonify({"status": "success", "message": "115 分类规则已保存"})
    

# 实例化限流器：建议 2 秒内最多允许 3 次解析请求（针对 115 比较稳妥）
api_limiter = RateLimiter(max_requests=3, period=2)
# 全局解析锁：确保同一时间只有一个线程在请求 115 API，防止并发冲突
fetch_lock = threading.Lock()

# 用于存储已解析的 URL，格式改为: { cache_key: {"url": direct_url, "expire_at": timestamp} }
_url_cache = {}

def _get_cached_115_url(pick_code, user_agent, client_ip=None):
    """
    带缓存的 115 直链获取器 (修复 TTL 和 负面缓存 问题)
    """
    cache_key = (pick_code, user_agent, client_ip)
    now = time.time()
    
    # 1. 先检查缓存及是否过期
    if cache_key in _url_cache:
        cached_data = _url_cache[cache_key]
        if now < cached_data["expire_at"]:
            cached_url = cached_data["url"]
            if cached_url:
                # 缓存命中且有效，直接返回（静默，不打印日志）
                return cached_url
            else:
                # 命中短期的“失败缓存”，防止疯狂重试打死 115 API
                return None
        else:
            # 缓存已过期，清理掉
            del _url_cache[cache_key]
    
    # 缓存未命中或已过期，需要请求 115 API
    client = P115Service.get_client()
    if not client: 
        # 客户端未初始化，防刷缓存 10 秒
        _url_cache[cache_key] = {"url": None, "name": pick_code, "expire_at": now + 10}
        return None
    
    # 使用锁：即使缓存失效，多个请求同时进来，也只有一个能去查 115 API
    with fetch_lock:
        now = time.time()
        # 二次检查缓存（可能在锁等待期间被其他线程填充）
        if cache_key in _url_cache and now < _url_cache[cache_key]["expire_at"]:
            cached_url = _url_cache[cache_key]["url"]
            if cached_url:
                # 从缓存中取出之前解析好的文件名
                display_name = _url_cache[cache_key].get("name", pick_code[:8] + "...")
                logger.info(f"  📥 [115直链] 命中缓存: {display_name}")
                return cached_url
        
        # 这里的限流逻辑：如果令牌不足，直接等待或返回
        if not api_limiter.consume():
            logger.warning(f"  ⚠️ [流控] 请求过快，已拦截 pick_code: {pick_code}")
            time.sleep(0.5) # 稍微强制延迟，缓解压力
            return None # 触发流控不写入缓存，让客户端稍后重试即可
            
        try:
            # 增加一个小随机延迟，模拟人为行为
            time.sleep(0.1) 
            
            # 使用 POST 方法获取直链
            url_obj = client.download_url(pick_code, user_agent=user_agent)
            if url_obj:
                # download_url 现在返回直链字符串
                direct_url = str(url_obj)
                
                # ★★★ 尝试从直链中提取真实文件名用于日志展示 ★★★
                display_name = pick_code[:8] + "..."
                try:
                    from urllib.parse import urlparse, parse_qs, unquote
                    parsed = urlparse(direct_url)
                    qs = parse_qs(parsed.query)
                    # 115 的直链通常把文件名放在 file 或 filename 参数里
                    if 'file' in qs:
                        display_name = unquote(qs['file'][0])
                    elif 'filename' in qs:
                        display_name = unquote(qs['filename'][0])
                    else:
                        # 兜底：尝试从 URL 路径最后一段提取
                        path_name = unquote(os.path.basename(parsed.path))
                        if path_name:
                            display_name = path_name
                except:
                    pass

                # 首次获取日志，打印真实文件名
                logger.info(f"  🎬 [115直链] 获取成功: {display_name}")
                
                # 存入缓存，把解析出的文件名也存进去，方便下次命中缓存时打印
                _url_cache[cache_key] = {"url": direct_url, "name": display_name, "expire_at": now + 7200}
                return direct_url
            else:
                # 获取失败，存入短期负面缓存 (10秒)，防止播放器疯狂重试导致 115 封号
                _url_cache[cache_key] = {"url": None, "name": pick_code, "expire_at": now + 10}
                return None
        except Exception as e:
            logger.error(f"  ❌ 获取 115 直链 API 报错: {e}")
            # 异常也存入短期负面缓存 (10秒)
            _url_cache[cache_key] = {"url": None, "name": pick_code, "expire_at": now + 10}
            return None

# 保留原来的 lru_cache 装饰器作为备用（用于 play_115_video 直接调用）
@lru_cache(maxsize=2048)
def _get_cached_115_url_legacy(pick_code, user_agent, client_ip=None):
    """
    带缓存的 115 直链获取器（旧版本，保留兼容性）
    """
    return _get_cached_115_url(pick_code, user_agent, client_ip)

@p115_bp.route('/play/<pick_code>', methods=['GET', 'HEAD']) # 允许 HEAD 请求，加速客户端嗅探
def play_115_video(pick_code):
    """
    终极极速 302 直链解析服务 (带内存缓存版)
    """
    if request.method == 'HEAD':
        # HEAD 请求通常是播放器嗅探，直接返回 200 或简单处理，不触发解析
        return '', 200

    try:
        player_ua = request.headers.get('User-Agent', 'Mozilla/5.0')
        
        # 尝试从缓存获取
        real_url = _get_cached_115_url(pick_code, player_ua)
        
        if not real_url:
            # 如果解析太快被拦截了，给播放器返回 429 告知稍后再试
            return "Too Many Requests - 115 API Protection", 429
            
        return redirect(real_url, code=302)
        
    except Exception as e:
        logger.error(f"  ❌ 直链解析发生异常: {e}")
        return str(e), 500
    
@p115_bp.route('/fix_strm', methods=['POST'])
@admin_required
def fix_strm_files():
    """扫描并修正本地所有 .strm 文件的内部链接 (支持兼容 CMS 老格式)"""
    config = get_config()
    local_root = config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT)
    etk_url = config.get(constants.CONFIG_OPTION_ETK_SERVER_URL, "").rstrip('/')
    
    if not local_root or not os.path.exists(local_root):
        return jsonify({"success": False, "message": "未配置本地 STRM 根目录，或该目录在容器中不存在！"}), 400
    if not etk_url:
        return jsonify({"success": False, "message": "未配置 ETK 内部访问地址！"}), 400
        
    fixed_count = 0
    skipped_count = 0
    
    try:
        # 递归遍历整个本地 STRM 目录
        for root_dir, _, files in os.walk(local_root):
            for file in files:
                if file.endswith('.strm'):
                    file_path = os.path.join(root_dir, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read().strip()
                        
                        pick_code = None
                        
                        # ----------------------------------------------------
                        # ★ 核心升级：多模式兼容提取 pick_code
                        # ----------------------------------------------------
                        
                        # 模式 1: ETK 现在的标准格式
                        # 例: http://192.168.31.177:5257/api/p115/play/abc1234
                        if '/api/p115/play/' in content:
                            pick_code = content.split('/api/p115/play/')[-1].split('?')[0].strip()
                            
                        # 模式 2: ETK 之前测试用的假协议格式
                        # 例: etk_direct_play://abc1234/文件名.mkv
                        elif content.startswith('etk_direct_play://'):
                            pick_code = content.split('//')[1].split('/')[0].strip()
                            
                        # 模式 3: CMS 生成的经典格式 (增强版兼容)
                        # 解析逻辑：提取 /d/ 后面，直到出现 . 或 ? 或 / 之前的字符
                        elif '/d/' in content:
                            # 这里的正则改成了匹配 /d/ 后面非特殊符号的部分
                            match = re.search(r'/d/([a-zA-Z0-9]+)[.?/]', content)
                            if not match:
                                # 如果后面没接符号，尝试匹配到字符串结尾
                                match = re.search(r'/d/([a-zA-Z0-9]+)$', content)
                                
                            if match:
                                pick_code = match.group(1)
                                
                        # ----------------------------------------------------
                            
                        if pick_code:
                            # 拼接为当前最新的 etk_url 格式
                            new_content = f"{etk_url}/api/p115/play/{pick_code}"
                            
                            # 只有当内容确实发生变化时才执行写入
                            if content != new_content:
                                with open(file_path, 'w', encoding='utf-8') as f:
                                    f.write(new_content)
                                fixed_count += 1
                            else:
                                skipped_count += 1
                        else:
                            logger.warning(f"  ⚠️ 无法识别该 strm 格式，已跳过: {file_path}")
                            
                    except Exception as e:
                        logger.error(f"  ❌ 处理文件 {file_path} 失败: {e}")
        
        msg = f"洗刷完毕！成功修正了 {fixed_count} 个文件"
        if skipped_count > 0:
            msg += f" (已跳过 {skipped_count} 个无需修改的文件)"
        logger.info(f"  🧹 [转换完毕] {msg}")
        return jsonify({"success": True, "message": msg})
        
    except Exception as e:
        logger.error(f"  ❌ 批量修正异常: {e}", exc_info=True)
        return jsonify({"success": False, "message": str(e)}), 500
