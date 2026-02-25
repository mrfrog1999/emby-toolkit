# handler/p115_service.py
import logging
import requests
import random
import os
import json
import re
import threading
import time
import config_manager
import constants
from database import settings_db
from database.connection import get_db_connection
import handler.tmdb as tmdb
import utils
try:
    from p115client import P115Client
except ImportError:
    P115Client = None

logger = logging.getLogger(__name__)

# ======================================================================
# ★★★ 115 OpenAPI 客户端 (仅管理操作：扫描/创建目录/移动文件) ★★★
# ======================================================================
class P115OpenAPIClient:
    """使用 Access Token 进行管理操作"""
    def __init__(self, access_token):
        if not access_token:
            raise ValueError("Access Token 不能为空")
        self.access_token = access_token.strip()
        self.base_url = "https://proapi.115.com"
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "User-Agent": "Emby-toolkit/1.0 (OpenAPI)"
        }

    def get_user_info(self):
        url = f"{self.base_url}/open/user/info"
        try:
            return requests.get(url, headers=self.headers, timeout=10).json()
        except Exception as e:
            return {"state": False, "message": str(e)}

    def fs_files(self, payload):
        """获取文件列表 - 纯净 OpenAPI 版 (严格返回官方原始字段)"""
        url = f"{self.base_url}/open/ufile/files"
        params = {"show_dir": 1, "limit": 1000, "offset": 0}
        if isinstance(payload, dict): params.update(payload)
        
        try:
            return requests.get(url, params=params, headers=self.headers, timeout=30).json()
        except Exception as e:
            return {"state": False, "error_msg": str(e)}

    def fs_files_app(self, payload): return self.fs_files(payload)

    def fs_mkdir(self, name, pid):
        url = f"{self.base_url}/open/folder/add"
        resp = requests.post(url, data={"pid": str(pid), "file_name": str(name)}, headers=self.headers).json()
        if resp.get("state") and "data" in resp: resp["cid"] = resp["data"].get("file_id")
        return resp

    def fs_move(self, fid, to_cid):
        return requests.post(f"{self.base_url}/open/ufile/move", data={"file_ids": str(fid), "to_cid": str(to_cid)}, headers=self.headers).json()

    def fs_rename(self, fid_name_tuple):
        return requests.post(f"{self.base_url}/open/ufile/update", data={"file_id": str(fid_name_tuple[0]), "file_name": str(fid_name_tuple[1])}, headers=self.headers).json()

    def fs_delete(self, fids):
        fids_str = ",".join([str(f) for f in fids]) if isinstance(fids, list) else str(fids)
        return requests.post(f"{self.base_url}/open/ufile/delete", data={"file_ids": fids_str}, headers=self.headers).json()


# ======================================================================
# ★★★ 115 Cookie 客户端 (仅播放：获取直链) ★★★
# ======================================================================
class P115CookieClient:
    """使用 Cookie 进行播放操作"""
    def __init__(self, cookie_str):
        if not cookie_str:
            raise ValueError("Cookie 不能为空")
        self.cookie_str = cookie_str.strip()
        self.webapi = None
        if P115Client:
            try:
                self.webapi = P115Client(self.cookie_str)
            except Exception as e:
                logger.warning(f"  ⚠️ Cookie 客户端初始化失败: {e}")
                raise

    def download_url(self, pick_code, user_agent=None):
        """获取直链 (仅 Cookie 可用)"""
        if self.webapi:
            try:
                url_obj = self.webapi.download_url(pick_code, user_agent=user_agent)
                if url_obj: return str(url_obj)
            except Exception as e:
                logger.warning(f"  ⚠️ Cookie 直链获取失败: {e}")
        return None

    def get_user_info(self):
        """获取用户信息 (仅用于验证)"""
        if self.webapi:
            try:
                # Cookie 模式获取用户信息的方式有限
                return {"state": True, "data": {"user_name": "Cookie用户"}}
            except:
                pass
        return None


# ======================================================================
# ★★★ 115 服务管理器 (分离管理/播放客户端) ★★★
# ======================================================================
class P115Service:
    """统一管理 OpenAPI 和 Cookie 客户端"""
    _instance = None
    _lock = threading.Lock()
    
    # 客户端缓存
    _openapi_client = None
    _cookie_client = None
    _token_cache = None
    _cookie_cache = None
    
    _last_request_time = 0

    @classmethod
    def get_openapi_client(cls):
        """获取管理客户端 (OpenAPI)"""
        config = get_config()
        token = config.get(constants.CONFIG_OPTION_115_TOKEN, "").strip()
        
        if not token:
            return None

        with cls._lock:
            if cls._openapi_client is None or token != cls._token_cache:
                try:
                    cls._openapi_client = P115OpenAPIClient(token)
                    cls._token_cache = token
                    logger.info("  🚀 [115] OpenAPI 客户端已初始化 (Token 模式)")
                except Exception as e:
                    logger.error(f"  ❌ 115 OpenAPI 客户端初始化失败: {e}")
                    cls._openapi_client = None
            
            return cls._openapi_client

    @classmethod
    def get_cookie_client(cls):
        """获取播放客户端 (Cookie)"""
        config = get_config()
        cookie = config.get(constants.CONFIG_OPTION_115_COOKIES, "").strip()
        
        if not cookie:
            return None

        with cls._lock:
            if cls._cookie_client is None or cookie != cls._cookie_cache:
                try:
                    cls._cookie_client = P115CookieClient(cookie)
                    cls._cookie_cache = cookie
                    logger.info("  🚀 [115] Cookie 客户端已初始化 (播放模式)")
                except Exception as e:
                    logger.error(f"  ❌ 115 Cookie 客户端初始化失败: {e}")
                    cls._cookie_client = None
            
            return cls._cookie_client

    @classmethod
    def get_client(cls):
        """
        获取严格分离客户端：
        管理操作 -> 强制走 OpenAPI
        播放操作 -> 强制走 Cookie
        """
        openapi = cls.get_openapi_client()
        cookie = cls.get_cookie_client()
        
        if not openapi and not cookie:
            return None

        class StrictSplitClient:
            def __init__(self, openapi_client, cookie_client):
                self._openapi = openapi_client
                self._cookie = cookie_client

            def _check_openapi(self):
                if not self._openapi:
                    raise Exception("未配置 115 Token (OpenAPI)，无法执行管理操作")

            def get_user_info(self):
                if self._openapi: return self._openapi.get_user_info()
                if self._cookie: return self._cookie.get_user_info()
                return None

            def fs_files(self, payload):
                self._check_openapi()
                return self._openapi.fs_files(payload)

            def fs_files_app(self, payload):
                self._check_openapi()
                return self._openapi.fs_files_app(payload)

            def fs_mkdir(self, name, pid):
                self._check_openapi()
                return self._openapi.fs_mkdir(name, pid)

            def fs_move(self, fid, to_cid):
                self._check_openapi()
                return self._openapi.fs_move(fid, to_cid)

            def fs_rename(self, fid_name_tuple):
                self._check_openapi()
                return self._openapi.fs_rename(fid_name_tuple)

            def fs_delete(self, fids):
                self._check_openapi()
                return self._openapi.fs_delete(fids)

            def download_url(self, pick_code, user_agent=None):
                if not self._cookie:
                    raise Exception("未配置 115 Cookie，无法获取播放直链")
                return self._cookie.download_url(pick_code, user_agent)

        # 全局限流逻辑
        with cls._lock:
            try:
                interval = float(get_config().get(constants.CONFIG_OPTION_115_INTERVAL, 5.0))
            except (ValueError, TypeError):
                interval = 5.0
            
            current_time = time.time()
            elapsed = current_time - cls._last_request_time
            if elapsed < interval:
                time.sleep(interval - elapsed)
            cls._last_request_time = time.time()

        return StrictSplitClient(openapi, cookie)
    
    @classmethod
    def get_cookies(cls):
        """获取 Cookie (用于直链下载等)"""
        config = get_config()
        return config.get(constants.CONFIG_OPTION_115_COOKIES)
    
    @classmethod
    def get_token(cls):
        """获取 Token (用于 API 调用)"""
        config = get_config()
        return config.get(constants.CONFIG_OPTION_115_TOKEN)


# ======================================================================
# ★★★ 新增：115 目录树 DB 缓存管理器 ★★★
# ======================================================================
class P115CacheManager:
    @staticmethod
    def get_cid(parent_cid, name):
        """从本地数据库获取 CID (毫秒级)"""
        if not parent_cid or not name: return None
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT id FROM p115_filesystem_cache WHERE parent_id = %s AND name = %s", 
                        (str(parent_cid), str(name))
                    )
                    row = cursor.fetchone()
                    return row['id'] if row else None
        except Exception as e:
            logger.error(f"  ❌ 读取 115 DB 缓存失败: {e}")
            return None

    @staticmethod
    def save_cid(cid, parent_cid, name):
        """将 CID 存入本地数据库缓存"""
        if not cid or not parent_cid or not name: return
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO p115_filesystem_cache (id, parent_id, name)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (parent_id, name)
                        DO UPDATE SET id = EXCLUDED.id, updated_at = NOW()
                    """, (str(cid), str(parent_cid), str(name)))
                    conn.commit()
        except Exception as e:
            logger.error(f"  ❌ 写入 115 DB 缓存失败: {e}")

    @staticmethod
    def get_cid_by_name(name):
        """仅通过名称查找 CID (适用于带有 {tmdb=xxx} 的唯一主目录)"""
        if not name: return None
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT id FROM p115_filesystem_cache WHERE name = %s LIMIT 1", (str(name),))
                    row = cursor.fetchone()
                    return row['id'] if row else None
        except Exception as e:
            return None

    @staticmethod
    def delete_cid(cid):
        """从缓存中物理删除该目录及其子目录的记录"""
        if not cid: return
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # 删除自身以及以它为父目录的子项
                    cursor.execute("DELETE FROM p115_filesystem_cache WHERE id = %s OR parent_id = %s", (str(cid), str(cid)))
                    conn.commit()
        except Exception as e:
            logger.error(f"  ❌ 清理 115 DB 缓存失败: {e}")

def get_config():
    return config_manager.APP_CONFIG

class SmartOrganizer:
    def __init__(self, client, tmdb_id, media_type, original_title):
        self.client = client
        self.tmdb_id = tmdb_id
        self.media_type = media_type
        self.original_title = original_title
        self.api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)

        self.studio_map = settings_db.get_setting('studio_mapping') or utils.DEFAULT_STUDIO_MAPPING
        self.keyword_map = settings_db.get_setting('keyword_mapping') or utils.DEFAULT_KEYWORD_MAPPING
        self.rating_map = settings_db.get_setting('rating_mapping') or utils.DEFAULT_RATING_MAPPING
        self.rating_priority = settings_db.get_setting('rating_priority') or utils.DEFAULT_RATING_PRIORITY

        self.raw_metadata = self._fetch_raw_metadata()
        self.details = self.raw_metadata
        raw_rules = settings_db.get_setting(constants.DB_KEY_115_SORTING_RULES)
        self.rules = []
        
        if raw_rules:
            if isinstance(raw_rules, list):
                self.rules = raw_rules
            elif isinstance(raw_rules, str):
                try:
                    self.rules = json.loads(raw_rules)
                except Exception as e:
                    logger.error(f"  ❌ 解析 115 分类规则失败: {e}")
                    self.rules = []

    def _fetch_raw_metadata(self):
        """
        获取 TMDb 原始元数据 (ID/Code)，不进行任何中文转换。
        """
        if not self.api_key: return {}

        data = {
            'genre_ids': [],
            'country_codes': [],
            'lang_code': None,
            'company_ids': [],
            'network_ids': [],
            'keyword_ids': [],
            'rating_label': '未知' # 分级是特例，必须计算出标签才能匹配
        }

        try:
            raw_details = {}
            if self.media_type == 'tv':
                raw_details = tmdb.get_tv_details(
                    self.tmdb_id, self.api_key,
                    append_to_response="keywords,content_ratings,networks"
                )
            else:
                raw_details = tmdb.get_movie_details(
                    self.tmdb_id, self.api_key,
                    append_to_response="keywords,release_dates"
                )

            if not raw_details: return {}

            # 1. 基础 ID/Code 提取
            data['genre_ids'] = [g.get('id') for g in raw_details.get('genres', [])]
            data['country_codes'] = [c.get('iso_3166_1') for c in raw_details.get('production_countries', [])]
            if not data['country_codes'] and raw_details.get('origin_country'):
                data['country_codes'] = raw_details.get('origin_country')

            data['lang_code'] = raw_details.get('original_language')

            data['company_ids'] = [c.get('id') for c in raw_details.get('production_companies', [])]
            data['network_ids'] = [n.get('id') for n in raw_details.get('networks', [])] if self.media_type == 'tv' else []

            # 2. 关键词 ID 提取
            kw_container = raw_details.get('keywords', {})
            raw_kw_list = kw_container.get('keywords', []) if self.media_type == 'movie' else kw_container.get('results', [])
            data['keyword_ids'] = [k.get('id') for k in raw_kw_list]

            # 3. 分级计算 
            data['rating_label'] = utils.get_rating_label(
                raw_details,
                self.media_type,
                self.rating_map,
                self.rating_priority
            )

            # 补充标题日期供重命名
            data['title'] = raw_details.get('title') or raw_details.get('name')
            date_str = raw_details.get('release_date') or raw_details.get('first_air_date')
            data['date'] = date_str
            data['year'] = 0
            
            if date_str and len(str(date_str)) >= 4:
                try:
                    data['year'] = int(str(date_str)[:4])
                except: 
                    pass
            # 补充评分供规则匹配
            data['vote_average'] = raw_details.get('vote_average', 0)

            return data

        except Exception as e:
            logger.warning(f"  ⚠️ [整理] 获取原始元数据失败: {e}", exc_info=True)
            return {}

    def _match_rule(self, rule):
        """
        规则匹配逻辑：
        - 标准字段：直接比对 ID/Code
        - 集合字段（工作室/关键词）：通过 Label 反查 Config 中的 ID 列表，再比对 TMDb ID
        """
        if not self.raw_metadata: return False

        # 1. 媒体类型
        if rule.get('media_type') and rule['media_type'] != 'all':
            if rule['media_type'] != self.media_type: return False

        # 2. 类型 (Genres) - ID 匹配
        if rule.get('genres'):
            # rule['genres'] 存的是 ID 列表 (如 [16, 35])
            # self.raw_metadata['genre_ids'] 是 TMDb ID 列表
            # 只要有一个交集就算命中
            rule_ids = [int(x) for x in rule['genres']]
            if not any(gid in self.raw_metadata['genre_ids'] for gid in rule_ids): return False

        # 3. 国家 (Countries) - Code 匹配
        if rule.get('countries'):
            # rule['countries'] 存的是 Code (如 ['US', 'CN'])
            # 只匹配第一个主要国家，避免合拍片误判 
            current_countries = self.raw_metadata.get('country_codes', [])
            # 获取列表中的第一个国家作为主要国家
            primary_country = current_countries[0] if current_countries else None
            
            # 如果没有国家信息，或者主要国家不在规则允许的列表中，则不匹配
            if not primary_country or primary_country not in rule['countries']:
                return False

        # 4. 语言 (Languages) - Code 匹配
        if rule.get('languages'):
            if self.raw_metadata['lang_code'] not in rule['languages']: return False

        # 5. 工作室 (Studios) - Label -> ID 匹配
        if rule.get('studios'):
            # rule['studios'] 存的是 Label (如 ['漫威', 'Netflix'])
            # 我们需要遍历这些 Label，去 self.studio_map 里找对应的 ID
            target_ids = set()
            for label in rule['studios']:
                # 找到配置项
                config_item = next((item for item in self.studio_map if item['label'] == label), None)
                if config_item:
                    target_ids.update(config_item.get('company_ids', []))
                    target_ids.update(config_item.get('network_ids', []))

            # 检查 TMDb 的 company/network ID 是否在 target_ids 中
            has_company = any(cid in target_ids for cid in self.raw_metadata['company_ids'])
            has_network = any(nid in target_ids for nid in self.raw_metadata['network_ids'])

            if not (has_company or has_network): return False

        # 6. 关键词 (Keywords) - Label -> ID 匹配
        if rule.get('keywords'):
            target_ids = set()
            for label in rule['keywords']:
                config_item = next((item for item in self.keyword_map if item['label'] == label), None)
                if config_item:
                    target_ids.update(config_item.get('ids', []))

            # 兼容字符串/数字 ID
            tmdb_kw_ids = [int(k) for k in self.raw_metadata['keyword_ids']]
            target_ids_int = [int(k) for k in target_ids]

            if not any(kid in target_ids_int for kid in tmdb_kw_ids): return False

        # 7. 分级 (Rating) - Label 匹配
        if rule.get('ratings'):
            if self.raw_metadata['rating_label'] not in rule['ratings']: return False

        # 8. 年份 (Year) 
        year_min = rule.get('year_min')
        year_max = rule.get('year_max')
        
        if year_min or year_max:
            current_year = self.raw_metadata.get('year', 0)
            
            # 如果获取不到年份，且设置了年份限制，则视为不匹配
            if current_year == 0: return False
            
            if year_min and current_year < int(year_min): return False
            if year_max and current_year > int(year_max): return False

        # 9. 时长 (Runtime) 
        # 逻辑：电影取 runtime，剧集取 episode_run_time (列表取平均或第一个)
        run_min = rule.get('runtime_min')
        run_max = rule.get('runtime_max')

        if run_min or run_max:
            current_runtime = 0
            if self.media_type == 'movie':
                current_runtime = self.details.get('runtime') or 0
            else:
                # 剧集时长通常是一个列表 [45, 60]，取第一个作为参考
                runtimes = self.details.get('episode_run_time', [])
                if runtimes and len(runtimes) > 0:
                    current_runtime = runtimes[0]

            # 如果获取不到时长，且设置了限制，视为不匹配
            if current_runtime == 0: return False

            if run_min and current_runtime < int(run_min): return False
            if run_max and current_runtime > int(run_max): return False

        # 10. 评分 (Min Rating) - 数值比较
        if rule.get('min_rating') and float(rule['min_rating']) > 0:
            vote_avg = self.details.get('vote_average', 0)
            if vote_avg < float(rule['min_rating']):
                return False

        return True

    def get_target_cid(self):
        """遍历规则，返回命中的 CID。未命中返回 None"""
        for rule in self.rules:
            if not rule.get('enabled', True): continue
            if self._match_rule(rule):
                logger.info(f"  🎯 [115] 命中规则: {rule.get('name')} -> 目录: {rule.get('dir_name')}")
                return rule.get('cid')
        return None

    def _extract_video_info(self, filename):
        """
        从文件名提取视频信息 (来源 · 分辨率 · 编码 · 音频 · 制作组)
        参考格式: BluRay · 1080p · X264 · DDP 7.1 · CMCT
        """
        info_tags = []
        name_upper = filename.upper()

        # 1. 来源/质量 (Source)
        source = ""
        if re.search(r'REMUX', name_upper): source = 'Remux'
        elif re.search(r'BLU-?RAY|BD', name_upper): source = 'BluRay'
        elif re.search(r'WEB-?DL', name_upper): source = 'WEB-DL'
        elif re.search(r'WEB-?RIP', name_upper): source = 'WEBRip'
        elif re.search(r'HDTV', name_upper): source = 'HDTV'
        elif re.search(r'DVD', name_upper): source = 'DVD'

        # ★★★ 修复：UHD 识别 ★★★
        if 'UHD' in name_upper:
            if source == 'BluRay': source = 'UHD BluRay'
            elif not source: source = 'UHD'

        # 2. 特效 (Effect: HDR/DV)
        effect = ""
        is_dv = re.search(r'(?:^|[\.\s\-\_])(DV|DOVI|DOLBY\s?VISION)(?:$|[\.\s\-\_])', name_upper)
        is_hdr = re.search(r'(?:^|[\.\s\-\_])(HDR|HDR10\+?)(?:$|[\.\s\-\_])', name_upper)

        if is_dv and is_hdr: effect = "HDR DV"
        elif is_dv: effect = "DV"
        elif is_hdr: effect = "HDR"

        if source:
            info_tags.append(f"{source} {effect}".strip())
        elif effect:
            info_tags.append(effect)

        # 3. 分辨率 (Resolution)
        res_match = re.search(r'(2160|1080|720|480)[pP]', filename)
        if res_match:
            info_tags.append(res_match.group(0).lower())
        elif '4K' in name_upper:
            info_tags.append('2160p')

        # 4. 编码 (Codec)
        codec = ""
        if re.search(r'[HX]265|HEVC', name_upper): info_tags.append('H265')
        elif re.search(r'[HX]264|AVC', name_upper): info_tags.append('H264')
        elif re.search(r'AV1', name_upper): info_tags.append('AV1')
        elif re.search(r'MPEG-?2', name_upper): info_tags.append('MPEG2')
        # 比特率提取 (Bit Depth) 
        bit_depth = ""
        bit_match = re.search(r'(\d{1,2})BIT', name_upper)
        if bit_match:
            bit_depth = f"{bit_match.group(1)}bit" # 统一格式为小写 bit

        # 将编码和比特率组合，比如 "H265 10bit" 或单独 "H265"
        if codec:
            full_codec = f"{codec} {bit_depth}".strip()
            info_tags.append(full_codec)
        elif bit_depth:
            info_tags.append(bit_depth)

        # 5. 音频 (Audio) - ★★★ 修复重点 ★★★
        audio_info = []
        
        # (1) 优先匹配带数字的音轨 (2Audio, 3Audios) 并统一格式为 "xAudios"
        # 正则说明: 匹配边界 + 数字 + 空格(可选) + Audio + s(可选) + 边界
        num_audio_match = re.search(r'\b(\d+)\s?Audios?\b', name_upper, re.IGNORECASE)
        if num_audio_match:
            # 统一格式化为: 数字 + Audios (例如: 2Audios)
            audio_info.append(f"{num_audio_match.group(1)}Audios")
        else:
            # (2) 如果没有数字音轨，再匹配 Multi/Dual 等通用标签
            if re.search(r'\b(Multi|双语|多音轨|Dual-Audio)\b', name_upper, re.IGNORECASE):
                audio_info.append('Multi')

        # (3) 其他具体音频编码
        if re.search(r'ATMOS', name_upper): audio_info.append('Atmos')
        elif re.search(r'TRUEHD', name_upper): audio_info.append('TrueHD')
        elif re.search(r'DTS-?HD(\s?MA)?', name_upper): audio_info.append('DTS-HD')
        elif re.search(r'DTS', name_upper): audio_info.append('DTS')
        elif re.search(r'DDP|EAC3|DOLBY\s?DIGITAL\+', name_upper): audio_info.append('DDP')
        elif re.search(r'AC3|DD', name_upper): audio_info.append('AC3')
        elif re.search(r'AAC', name_upper): audio_info.append('AAC')
        elif re.search(r'FLAC', name_upper): audio_info.append('FLAC')
        elif re.search(r'OPUS', name_upper): audio_info.append('Opus')
        
        chan_match = re.search(r'\b(7\.1|5\.1|2\.0)\b', filename)
        if chan_match:
            audio_info.append(chan_match.group(1))
            
        if audio_info:
            info_tags.append(" ".join(audio_info))

        # 流媒体平台识别
        # 匹配 NF, AMZN, DSNP, HMAX, HULU, NETFLIX, DISNEY+, APPLETV+
        stream_match = re.search(r'\b(NF|AMZN|DSNP|HMAX|HULU|NETFLIX|DISNEY\+|APPLETV\+|B-GLOBAL)\b', name_upper)
        if stream_match:
            info_tags.append(stream_match.group(1))

        # 6. 发布组 (Release Group)
        group_found = False
        try:
            from tasks import helpers
            for group_name, patterns in helpers.RELEASE_GROUPS.items():
                for pattern in patterns:
                    try:
                        match = re.search(pattern, filename, re.IGNORECASE)
                        if match:
                            info_tags.append(match.group(0))
                            group_found = True
                            break
                    except: pass
                if group_found: break

            if not group_found:
                name_no_ext = os.path.splitext(filename)[0]
                match_suffix = re.search(r'-([a-zA-Z0-9]+)$', name_no_ext)
                if match_suffix:
                    possible_group = match_suffix.group(1)
                    if len(possible_group) > 2 and possible_group.upper() not in ['1080P', '2160P', '4K', 'HDR', 'H265', 'H264']:
                        info_tags.append(possible_group)
        except ImportError:
            pass

        return " · ".join(info_tags) if info_tags else ""

    def _rename_file_node(self, file_node, new_base_name, year=None, is_tv=False):
        # 兼容 OpenAPI 键名
        original_name = file_node.get('fn') or file_node.get('n') or file_node.get('file_name', '')
        if '.' not in original_name: return original_name, None

        parts = original_name.rsplit('.', 1)
        name_body = parts[0]
        ext = parts[1].lower()

        is_sub = ext in ['srt', 'ass', 'ssa', 'sub', 'vtt', 'sup']
        lang_suffix = ""
        if is_sub:
            lang_keywords = [
                'zh', 'cn', 'tw', 'hk', 'en', 'jp', 'kr',
                'chs', 'cht', 'eng', 'jpn', 'kor', 'fre', 'spa',
                'default', 'forced', 'tc', 'sc'
            ]
            sub_parts = name_body.split('.')
            if len(sub_parts) > 1:
                last_part = sub_parts[-1].lower()
                if last_part in lang_keywords or '-' in last_part:
                    lang_suffix = f".{sub_parts[-1]}"

            if not lang_suffix:
                match = re.search(r'(?:\.|-|_|\s)(chs|cht|zh-cn|zh-tw|eng|jpn|kor|tc|sc)(?:\.|-|_|$)', name_body, re.IGNORECASE)
                if match:
                    lang_suffix = f".{match.group(1)}"

        tag_suffix = ""
        try:
            search_name = original_name
            if is_sub:
                if lang_suffix and name_body.endswith(lang_suffix):
                    clean_body = name_body[:-len(lang_suffix)]
                    search_name = f"{clean_body}.mkv"
                else:
                    search_name = f"{name_body}.mkv"

            video_info = self._extract_video_info(search_name)
            if video_info:
                tag_suffix = f" · {video_info}"
        except Exception as e:
            pass

        if is_tv:
            pattern = r'(?:s|S)(\d{1,2})(?:e|E)(\d{1,2})|Ep?(\d{1,2})|第(\d{1,3})[集话]'
            match = re.search(pattern, original_name)
            if match:
                s, e, ep_only, zh_ep = match.groups()
                season_num = int(s) if s else 1
                episode_num = int(e) if e else (int(ep_only) if ep_only else int(zh_ep))

                s_str = f"S{season_num:02d}"
                e_str = f"E{episode_num:02d}"

                new_name = f"{new_base_name} - {s_str}{e_str}{tag_suffix}{lang_suffix}.{ext}"
                return new_name, season_num
            else:
                return original_name, None
        else:
            movie_base = f"{new_base_name} ({year})" if year else new_base_name
            new_name = f"{movie_base}{tag_suffix}{lang_suffix}.{ext}"
            return new_name, None

    def _scan_files_recursively(self, cid, depth=0, max_depth=3):
        all_files = []
        if depth > max_depth: return []
        try:
            time.sleep(1.5) 
            res = self.client.fs_files({'cid': cid, 'limit': 1000, 'record_open_time': 0, 'count_folders': 0})
            if res.get('data'):
                for item in res['data']:
                    # 兼容 OpenAPI 键名
                    fc_val = item.get('fc') if item.get('fc') is not None else item.get('type')
                    if str(fc_val) == '1':
                        all_files.append(item)
                    elif str(fc_val) == '0':
                        sub_id = item.get('fid') or item.get('file_id')
                        sub_files = self._scan_files_recursively(sub_id, depth + 1, max_depth)
                        all_files.extend(sub_files)
        except Exception as e:
            logger.warning(f"  ⚠️ 扫描目录出错 (CID: {cid}): {e}")
        return all_files

    def _is_junk_file(self, filename):
        """
        检查是否为垃圾文件/样本/花絮 (基于 MP 规则)
        """
        # 垃圾文件正则列表 (合并了通用规则和你提供的 MP 规则)
        junk_patterns = [
            # 基础关键词
            r'(?i)\b(sample|trailer|featurette|bonus)\b',

            # MP 规则集
            r'(?i)Special Ending Movie',
            r'(?i)\[((TV|BD|\bBlu-ray\b)?\s*CM\s*\d{2,3})\]',
            r'(?i)\[Teaser.*?\]',
            r'(?i)\[PV.*?\]',
            r'(?i)\[NC[OPED]+.*?\]',
            r'(?i)\[S\d+\s+Recap(\s+\d+)?\]',
            r'(?i)Menu',
            r'(?i)Preview',
            r'(?i)\b(CDs|SPs|Scans|Bonus|映像特典|映像|specials|特典CD|Menu|Logo|Preview|/mv)\b',
            r'(?i)\b(NC)?(Disc|片头|OP|SP|ED|Advice|Trailer|BDMenu|片尾|PV|CM|Preview|MENU|Info|EDPV|SongSpot|BDSpot)(\d{0,2}|_ALL)\b',
            r'(?i)WiKi\.sample'
        ]

        for pattern in junk_patterns:
            if re.search(pattern, filename):
                return True
        return False

    def execute(self, root_item, target_cid, delete_source=True):
        title = self.details.get('title') or self.original_title
        date_str = self.details.get('date') or ''
        year = date_str[:4] if date_str else ''

        safe_title = re.sub(r'[\\/:*?"<>|]', '', title).strip()
        std_root_name = f"{safe_title} ({year}) {{tmdb={self.tmdb_id}}}" if year else f"{safe_title} {{tmdb={self.tmdb_id}}}"

        # 兼容 OpenAPI 键名
        root_name = root_item.get('fn') or root_item.get('n') or root_item.get('file_name', '未知')
        source_root_id = root_item.get('fid') or root_item.get('file_id')
        fc_val = root_item.get('fc') if root_item.get('fc') is not None else root_item.get('type')
        is_source_file = str(fc_val) == '1'
        dest_parent_cid = target_cid if (target_cid and str(target_cid) != '0') else (root_item.get('pid') or root_item.get('parent_id') or root_item.get('cid'))

        config = get_config()
        configured_exts = config.get(constants.CONFIG_OPTION_115_EXTENSIONS, [])
        allowed_exts = set(e.lower() for e in configured_exts)
        known_video_exts = {'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg'}
        MIN_VIDEO_SIZE = 10 * 1024 * 1024

        logger.info(f"  🚀 [115] 开始整理: {root_name} -> {std_root_name}")

        final_home_cid = P115CacheManager.get_cid(dest_parent_cid, std_root_name)

        if final_home_cid:
            logger.info(f"  ⚡ [缓存命中] 主目录: {std_root_name}")
        else:
            mk_res = self.client.fs_mkdir(std_root_name, dest_parent_cid)
            if mk_res.get('state'):
                final_home_cid = mk_res.get('cid')
                P115CacheManager.save_cid(final_home_cid, dest_parent_cid, std_root_name)
                logger.info(f"  🆕 创建新主目录并缓存: {std_root_name}")
            else:
                try:
                    search_res = self.client.fs_files({'cid': dest_parent_cid, 'search_value': std_root_name, 'limit': 1150, 'record_open_time': 0, 'count_folders': 0})
                    if search_res.get('data'):
                        for item in search_res['data']:
                            item_name = item.get('fn') or item.get('n') or item.get('file_name')
                            item_fc = item.get('fc') if item.get('fc') is not None else item.get('type')
                            if item_name == std_root_name and str(item_fc) == '0':
                                final_home_cid = item.get('fid') or item.get('file_id')
                                P115CacheManager.save_cid(final_home_cid, dest_parent_cid, std_root_name)
                                logger.info(f"  📂 成功查找到已存在主目录并永久缓存: {std_root_name}")
                                break
                except Exception as e:
                    logger.warning(f"  ⚠️ 115模糊查找异常: {e}")

                if not final_home_cid:
                    logger.warning(f"  ⚠️ 115搜索失效，启动全量遍历查找老目录: '{std_root_name}' ...")
                    offset = 0
                    limit = 1000
                    while True:
                        try:
                            res = self.client.fs_files({'cid': dest_parent_cid, 'limit': limit, 'offset': offset, 'type': 0, 'record_open_time': 0, 'count_folders': 0})
                            data = res.get('data', [])
                            if not data: break 
                            
                            for item in data:
                                item_name = item.get('fn') or item.get('n') or item.get('file_name')
                                item_fc = item.get('fc') if item.get('fc') is not None else item.get('type')
                                if item_name == std_root_name and str(item_fc) == '0':
                                    final_home_cid = item.get('fid') or item.get('file_id')
                                    P115CacheManager.save_cid(final_home_cid, dest_parent_cid, std_root_name)
                                    logger.info(f"  📂 成功查找到已存在主目录并永久缓存: {std_root_name}")
                                    break
                                    
                            if final_home_cid: break 
                            offset += limit 
                        except Exception as e:
                            logger.error(f"遍历查找失败: {e}")
                            break

        if not final_home_cid:
            logger.error(f"  ❌ 无法获取或创建目标目录 (已尝试所有手段)")
            return False

        candidates = []
        if is_source_file:
            candidates.append(root_item)
        else:
            candidates = self._scan_files_recursively(source_root_id, max_depth=3)

        if not candidates: return True

        moved_count = 0
        for file_item in candidates:
            # 兼容 OpenAPI 键名
            fid = file_item.get('fid') or file_item.get('file_id')
            file_name = file_item.get('fn') or file_item.get('n') or file_item.get('file_name', '')
            ext = file_name.split('.')[-1].lower() if '.' in file_name else ''
            if self._is_junk_file(file_name): continue
            if ext not in allowed_exts: continue
            
            file_size = _parse_115_size(file_item.get('fs') or file_item.get('size'))
            if ext in known_video_exts and 0 < file_size < MIN_VIDEO_SIZE: continue

            new_filename, season_num = self._rename_file_node(
                file_item, safe_title, year=year, is_tv=(self.media_type=='tv')
            )

            real_target_cid = final_home_cid
            if self.media_type == 'tv' and season_num is not None:
                s_name = f"Season {season_num:02d}"
                s_cid = P115CacheManager.get_cid(final_home_cid, s_name)
                
                if s_cid:
                    logger.info(f"  ⚡ [缓存命中] 季目录: {std_root_name} - {s_name}")
                    real_target_cid = s_cid
                else:
                    s_mk = self.client.fs_mkdir(s_name, final_home_cid)
                    s_cid = s_mk.get('cid') if s_mk.get('state') else None
                    
                    if not s_cid: 
                        try:
                            s_search = self.client.fs_files({'cid': final_home_cid, 'search_value': s_name, 'limit': 1150, 'record_open_time': 0, 'count_folders': 0})
                            for item in s_search.get('data', []):
                                item_name = item.get('fn') or item.get('n') or item.get('file_name')
                                item_fc = item.get('fc') if item.get('fc') is not None else item.get('type')
                                if item_name == s_name and str(item_fc) == '0':
                                    s_cid = item.get('fid') or item.get('file_id')
                                    break
                        except: pass
                    
                    if s_cid:
                        P115CacheManager.save_cid(s_cid, final_home_cid, s_name)
                        logger.info(f"  🆕 创建季目录并缓存: {std_root_name} - {s_name}")
                        real_target_cid = s_cid

            if new_filename != file_name:
                if self.client.fs_rename((fid, new_filename)).get('state'):
                    logger.info(f"  ✏️ [重命名] {file_name} -> {new_filename}")

            if self.client.fs_move(fid, real_target_cid).get('state'):
                if self.media_type == 'tv' and season_num is not None:
                    logger.info(f"  📁 [移动] {file_name} -> {std_root_name} - {s_name}")
                else:
                    logger.info(f"  📁 [移动] {file_name} -> {std_root_name}")
                moved_count += 1

                # 兼容 OpenAPI 键名
                pick_code = file_item.get('pc') or file_item.get('pick_code')
                local_root = config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT)
                etk_url = config.get(constants.CONFIG_OPTION_ETK_SERVER_URL, "http://127.0.0.1:5257").rstrip('/')
                
                if pick_code and local_root and os.path.exists(local_root):
                    try:
                        category_name = None
                        for rule in self.rules:
                            if rule.get('cid') == str(target_cid):
                                category_name = rule.get('dir_name', '未识别')
                                break
                        if not category_name: category_name = "未识别"

                        category_rule = next((r for r in self.rules if str(r.get('cid')) == str(target_cid)), None)
                        
                        if category_rule and 'category_path' in category_rule:
                            relative_category_path = category_rule['category_path']
                            logger.debug(f"  ⚡ [规则缓存] 分类路径: '{relative_category_path}'")
                        else:
                            relative_category_path = category_rule.get('dir_name', '未识别') if category_rule else "未识别"

                        if self.media_type == 'tv' and season_num is not None:
                            local_dir = os.path.join(local_root, relative_category_path, std_root_name, s_name)
                        else:
                            local_dir = os.path.join(local_root, relative_category_path, std_root_name)
                        
                        os.makedirs(local_dir, exist_ok=True) 

                        ext = new_filename.split('.')[-1].lower() if '.' in new_filename else ''
                        is_video = ext in known_video_exts
                        is_sub = ext in ['srt', 'ass', 'ssa', 'sub', 'vtt', 'sup']

                        if is_video:
                            strm_filename = os.path.splitext(new_filename)[0] + ".strm"
                            strm_filepath = os.path.join(local_dir, strm_filename)
                            strm_content = f"{etk_url}/api/p115/play/{pick_code}"
                            
                            with open(strm_filepath, 'w', encoding='utf-8') as f:
                                f.write(strm_content)
                            logger.info(f"  📝 STRM 已生成 -> {strm_filename}")
                            
                        elif is_sub:
                            if config.get(constants.CONFIG_OPTION_115_DOWNLOAD_SUBS, True):
                                sub_filepath = os.path.join(local_dir, new_filename)
                                if not os.path.exists(sub_filepath):
                                    try:
                                        logger.info(f"  ⬇️ [字幕下载] 正在向 115 拉取外挂字幕: {new_filename} ...")
                                        url_obj = self.client.download_url(pick_code, user_agent="Mozilla/5.0")
                                        dl_url = str(url_obj)
                                        if dl_url:
                                            import requests
                                            headers = {
                                                "User-Agent": "Mozilla/5.0",
                                                "Cookie": self.get_cookies()
                                            }
                                            resp = requests.get(dl_url, stream=True, timeout=30, headers=headers)
                                            resp.raise_for_status()
                                            with open(sub_filepath, 'wb') as f:
                                                for chunk in resp.iter_content(chunk_size=8192):
                                                    f.write(chunk)
                                            logger.info(f"  ✅ [字幕下载] 下载完成！")
                                    except Exception as e:
                                        logger.error(f"  ❌ 下载字幕失败: {e}")
                        
                    except Exception as e:
                        logger.error(f"  ❌ 生成 STRM 文件失败: {e}", exc_info=True)

        if delete_source and not is_source_file and moved_count > 0:
            self.client.fs_delete([source_root_id])
            logger.info(f"  🧹 已清理空目录")

        return True

def _parse_115_size(size_val):
    """
    统一解析 115 返回的文件大小为字节(Int)
    支持: 12345(int), "12345"(str), "1.2GB", "500KB"
    """
    try:
        if size_val is None: return 0

        # 1. 如果已经是数值 (115 API 's' 字段通常是 int)
        if isinstance(size_val, (int, float)):
            return int(size_val)

        # 2. 如果是字符串
        if isinstance(size_val, str):
            s = size_val.strip()
            if not s: return 0
            # 纯数字字符串
            if s.isdigit():
                return int(s)

            s_upper = s.upper().replace(',', '')
            mult = 1
            if 'TB' in s_upper: mult = 1024**4
            elif 'GB' in s_upper: mult = 1024**3
            elif 'MB' in s_upper: mult = 1024**2
            elif 'KB' in s_upper: mult = 1024

            match = re.search(r'([\d\.]+)', s_upper)
            if match:
                return int(float(match.group(1)) * mult)
    except Exception:
        pass
    return 0

def get_115_account_info():
    """
    获取 115 账号状态及详细信息
    """
    client = P115Service.get_client()
    if not client: raise Exception("无法初始化 115 客户端")

    config = get_config()
    auth_str = config.get(constants.CONFIG_OPTION_115_COOKIES, "")

    if not auth_str:
        raise Exception("未配置 115 凭证")

    try:
        # 尝试获取详细用户信息 (仅 OpenAPI 支持)
        if hasattr(client, 'get_user_info'):
            user_resp = client.get_user_info()
            if user_resp and user_resp.get('state'):
                return {
                    "valid": True,
                    "msg": "混合模式正常 (OpenAPI+Cookie)" if "|||" in auth_str else "OpenAPI 模式正常",
                    "user_info": user_resp.get('data', {})
                }

        # 如果没有 OpenAPI，回退到基础检查
        resp = client.fs_files_app({'limit': 1})
        if not resp.get('state'):
            raise Exception("凭证已失效")

        return {
            "valid": True,
            "msg": "Cookie 模式正常",
            "user_info": None
        }
    except Exception as e:
        raise Exception(f"凭证无效或网络不通: {e}")


def _identify_media_enhanced(filename, forced_media_type=None):
    """
    增强识别逻辑：
    1. 支持多种 TMDb ID 标签格式: {tmdb=xxx}
    2. 支持标准命名格式: Title (Year)
    3. 接收外部强制指定的类型 (forced_media_type)，不再轮询猜测
    
    返回: (tmdb_id, media_type, title) 或 (None, None, None)
    """
    tmdb_id = None
    media_type = 'movie' # 默认
    title = filename
    
    # 1. 优先提取 TMDb ID 标签 (最稳)
    match_tag = re.search(r'\{?tmdb(?:id)?[=\-](\d+)\}?', filename, re.IGNORECASE)
    
    if match_tag:
        tmdb_id = match_tag.group(1)
        
        # 如果外部指定了类型，直接用；否则看文件名特征
        if forced_media_type:
            media_type = forced_media_type
        elif re.search(r'(?:S\d{1,2}|E\d{1,2}|第\d+季|Season)', filename, re.IGNORECASE):
            media_type = 'tv'
        
        # 提取标题
        clean_name = re.sub(r'\{?tmdb(?:id)?[=\-]\d+\}?', '', filename, flags=re.IGNORECASE).strip()
        match_title = re.match(r'^(.+?)\s*[\(\[]\d{4}[\)\]]', clean_name)
        if match_title:
            title = match_title.group(1).strip()
        else:
            title = clean_name
            
        return tmdb_id, media_type, title

    # 2. 其次提取标准格式 Title (Year)
    match_std = re.match(r'^(.+?)\s+[\(\[](\d{4})[\)\]]', filename)
    if match_std:
        name_part = match_std.group(1).strip()
        year_part = match_std.group(2)
        
        # === 关键修正：类型判断逻辑 ===
        if forced_media_type:
            # 如果外部透视过目录，确定是 TV，直接信赖
            media_type = forced_media_type
        else:
            # 否则才根据文件名特征判断
            if re.search(r'(?:S\d{1,2}|E\d{1,2}|第\d+季|Season)', filename, re.IGNORECASE):
                media_type = 'tv'
            else:
                media_type = 'movie'
            
        # 尝试通过 TMDb API 确认 ID
        try:
            api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
            if api_key:
                # 精准搜索，不轮询，不瞎猜
                results = tmdb.search_media(
                    query=name_part, 
                    api_key=api_key, 
                    item_type=media_type, 
                    year=year_part
                )
                
                if results and len(results) > 0:
                    best = results[0]
                    return best['id'], media_type, (best.get('title') or best.get('name'))
                else:
                    logger.warning(f"  ⚠️ TMDb 未找到资源: {name_part} ({year_part}) 类型: {media_type}")

        except Exception as e:
            pass

    return None, None, None


def task_scan_and_organize_115(processor=None):
    """
    [任务链] 主动扫描 115 待整理目录
    - 识别成功 -> 归类到目标目录
    - 识别失败 -> 移动到 '未识别' 目录
    ★ 修复：增加子文件探测逻辑，防止剧集文件夹因命名不规范被误判为电影
    """
    logger.info("=== 开始执行 115 待整理目录扫描 ===")

    client = P115Service.get_client()
    if not client: raise Exception("无法初始化 115 客户端")

    config = get_config()
    cookies = config.get(constants.CONFIG_OPTION_115_COOKIES)
    cid_val = config.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID)
    save_val = config.get(constants.CONFIG_OPTION_115_SAVE_PATH_NAME, '待整理')
    enable_organize = config.get(constants.CONFIG_OPTION_115_ENABLE_ORGANIZE, False)

    if not cookies:
        logger.error("  ⚠️ 未配置 115 Cookies，跳过。")
        return
    if not cid_val or str(cid_val) == '0':
        logger.error("  ⚠️ 未配置待整理目录 (CID)，跳过。")
        return
    if not enable_organize:
        logger.warning("  ⚠️ 未开启智能整理开关，仅扫描不处理。")
        return
    current_time = time.time()
    try:
        save_cid = int(cid_val)
        save_name = str(save_val)

        # 1. 准备 '未识别' 目录
        unidentified_folder_name = "未识别"
        unidentified_cid = None
        try:
            time.sleep(1.5)
            # ★ 优化：纯读模式，不统计文件夹
            search_res = client.fs_files({
                'cid': save_cid, 'search_value': unidentified_folder_name, 'limit': 1,
                'record_open_time': 0, 'count_folders': 0
            })
            if search_res.get('data'):
                for item in search_res['data']:
                    if item.get('fn') == unidentified_folder_name and str(item.get('fc')) == '0':
                        unidentified_cid = item.get('fid')
                        break
        except: pass

        if not unidentified_cid:
            try:
                mk_res = client.fs_mkdir(unidentified_folder_name, save_cid)
                if mk_res.get('state'): unidentified_cid = mk_res.get('cid')
            except: pass

        logger.info(f"  🔍 正在扫描目录: {save_name} ...")
        
        # =================================================================
        # ★★★ 主目录扫描：纯读模式 + 修正排序字段 + 退避重试 ★★★
        # =================================================================
        res = {}
        for retry in range(3):
            try:
                time.sleep(2)
                res = client.fs_files({
                    'cid': save_cid, 'limit': 50, 'o': 'user_utime', 'asc': 0,
                    'record_open_time': 0, 'count_folders': 0
                })
                break 
            except Exception as e:
                if '405' in str(e) or 'Method Not Allowed' in str(e):
                    logger.warning(f"  ⚠️ 扫描主目录触发 115 风控拦截 (405)，休眠 5 秒后重试 ({retry+1}/3)...")
                    time.sleep(5)
                else:
                    raise

        if not res.get('data'):
            logger.info(f"  📂 [{save_name}] 目录为空或获取失败。")
            return

        processed_count = 0
        moved_to_unidentified = 0

        for item in res['data']:
            # 兼容 OpenAPI 键名
            name = item.get('fn') or item.get('n') or item.get('file_name')
            if not name: continue
            item_id = item.get('fid') or item.get('file_id')
            fc_val = item.get('fc') if item.get('fc') is not None else item.get('type')
            is_folder = str(fc_val) == '0'

            if str(item_id) == str(unidentified_cid) or name == unidentified_folder_name:
                continue

            forced_type = None
            peek_failed = False

            if is_folder:
                # =================================================================
                # ★★★ 子目录透视：开启 nf=1 (仅看文件夹) 极大降低负载 ★★★
                # =================================================================
                for retry in range(2):
                    try:
                        time.sleep(2)
                        sub_res = client.fs_files({
                            'cid': item.get('cid'), 'limit': 20, 
                            'nf': 1, # ★ 核心优化：只返回文件夹，不返回文件
                            'record_open_time': 0, 'count_folders': 0
                        })
                        if sub_res.get('data'):
                            for sub_item in sub_res['data']:
                                sub_name = sub_item.get('fn', '')
                                if re.search(r'(Season\s?\d+|S\d+|Ep?\d+|第\d+季)', sub_name, re.IGNORECASE):
                                    forced_type = 'tv'
                                    break
                        peek_failed = False
                        break
                    except Exception as e:
                        if '405' in str(e) or 'Method Not Allowed' in str(e):
                            logger.warning(f"  ⚠️ 透视目录 '{name}' 触发风控，休眠 3 秒后重试 ({retry+1}/2)...")
                            time.sleep(3)
                            peek_failed = True
                        else:
                            peek_failed = True
                            break

            if peek_failed:
                logger.warning(f"  ⏭️ 透视 '{name}' 连续失败，为防误判跳过本次识别。")
                continue

            tmdb_id, media_type, title = _identify_media_enhanced(name, forced_media_type=forced_type)
            
            if tmdb_id:
                logger.info(f"  ➜ 识别成功: {name} -> ID:{tmdb_id} ({media_type})")
                try:
                    organizer = SmartOrganizer(client, tmdb_id, media_type, title)
                    target_cid = organizer.get_target_cid()
                    
                    if organizer.execute(item, target_cid, delete_source=False):
                        processed_count += 1
                        
                        if is_folder:
                            update_time_str = item.get('upt') or '0'
                            try:
                                update_time = int(update_time_str)
                            except:
                                update_time = current_time
                                
                            if (current_time - update_time) > 86400:
                                logger.info(f"  🧹 [兜底清理] 清理已过期(>24h)的残留目录: {name}")
                                client.fs_delete([item_id])

                except Exception as e:
                    logger.error(f"  ❌ 整理出错: {e}")
            else:
                if unidentified_cid:
                    try:
                        client.fs_move(item_id, unidentified_cid)
                        moved_to_unidentified += 1
                    except: pass

        logger.info(f"=== 扫描结束，成功归类 {processed_count} 个，移入未识别 {moved_to_unidentified} 个 ===")

    except Exception as e:
        logger.error(f"  ⚠️ 115 扫描任务异常: {e}", exc_info=True)

def task_sync_115_directory_tree(processor=None):
    """
    主动同步 115 分类目录下的所有子目录到本地 DB 缓存。
    这能彻底解决 115 API search_value 失效导致的老目录无法识别问题。
    """
    logger.info("=== 开始全量同步 115 目录树到本地数据库 ===")
    
    # 局部导入 task_manager 用于向前端发送实时进度 (防止与 core.py 循环引用)
    try:
        import task_manager
    except ImportError:
        task_manager = None

    def update_progress(prog, msg):
        if task_manager:
            task_manager.update_status_from_thread(prog, msg)
        logger.info(msg)

    client = P115Service.get_client()
    if not client: 
        update_progress(100, "115 客户端未初始化，任务结束。")
        return

    raw_rules = settings_db.get_setting(constants.DB_KEY_115_SORTING_RULES)
    if not raw_rules: 
        update_progress(100, "未配置分类规则，无需同步。")
        return
    
    rules = json.loads(raw_rules) if isinstance(raw_rules, str) else raw_rules
    
    # 提取所有启用的规则中的目标分类目录 CID，并去重
    target_cids = set()
    for rule in rules:
        if rule.get('enabled', True) and rule.get('cid'):
            cid_str = str(rule['cid'])
            if cid_str and cid_str != '0':
                target_cids.add(cid_str)

    if not target_cids:
        update_progress(100, "未找到有效的分类目标目录 CID，任务结束。")
        return

    total_cached = 0
    total_cids = len(target_cids)
    
    for idx, cid in enumerate(target_cids):
        base_prog = int((idx / total_cids) * 100)
        update_progress(base_prog, f"  🔍 正在扫描第 {idx+1}/{total_cids} 个分类目录 (CID: {cid})...")
        
        offset = 0
        limit = 1000
        page_count = 0
        
        while True:
            # 响应前端的中止任务按钮
            if processor and getattr(processor, 'is_stop_requested', lambda: False)():
                update_progress(100, "任务已被用户手动终止。")
                return

            try:
                # 获取数据列表
                res = client.fs_files({'cid': cid, 'limit': limit, 'offset': offset, 'record_open_time': 0, 'count_folders': 0})
                data = res.get('data', [])
                
                if not data: 
                    break # 本目录全空，跳出
                
                page_count += 1
                dir_count_in_page = 0
                
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        for item in data:
                            # 兼容 OpenAPI 键名
                            fc_val = item.get('fc') if item.get('fc') is not None else item.get('type')
                            if str(fc_val) == '0':
                                sub_cid = item.get('fid') or item.get('file_id')
                                sub_name = item.get('fn') or item.get('n') or item.get('file_name')
                                if sub_cid and sub_name:
                                    cursor.execute("""
                                        INSERT INTO p115_filesystem_cache (id, parent_id, name)
                                        VALUES (%s, %s, %s)
                                        ON CONFLICT (parent_id, name)
                                        DO UPDATE SET id = EXCLUDED.id, updated_at = NOW()
                                    """, (str(sub_cid), str(cid), str(sub_name)))
                                    total_cached += 1
                                    dir_count_in_page += 1
                        conn.commit()
                
                # 实时播报当前正在翻第几页，以及入库了多少个文件夹
                update_progress(base_prog, f"  ➜ CID: {cid} | 翻阅第 {page_count} 页 | 新增/更新 {dir_count_in_page} 个目录...")
                
                # ★ 性能优化：如果获取的数据小于请求的上限，说明到底了，不用再请求下一页
                if len(data) < limit:
                    break
                    
                offset += limit
                time.sleep(1) # 稍微喘口气，防 115 踢人
                
            except Exception as e:
                logger.error(f"  ❌ 同步目录树异常 (CID: {cid}): {e}")
                break # 发生异常，跳过这个 CID 继续查下一个

    update_progress(100, f"=== 同步结束！共成功更新 {total_cached} 个目录的缓存 ===")

def task_full_sync_strm_and_subs(processor=None):
    """
    极速全量生成 STRM 与 同步字幕 (带防失败自动降级机制)
    修复版：完美对齐网盘与本地分类目录的层级路径
    """
    config = get_config()
    download_subs = config.get(constants.CONFIG_OPTION_115_DOWNLOAD_SUBS, True)
    enable_cleanup = config.get(constants.CONFIG_OPTION_115_LOCAL_CLEANUP, False)
    start_msg = "=== 🚀 开始全量生成 STRM 与 同步字幕 ===" if download_subs else "=== 🚀 开始全量生成 STRM (已跳过字幕) ==="
    if enable_cleanup: start_msg += " [已开启本地清理]"
    logger.info(start_msg)
    
    try:
        import task_manager
    except ImportError:
        task_manager = None

    def update_progress(prog, msg):
        if task_manager: task_manager.update_status_from_thread(prog, msg)
        logger.info(msg)

    local_root = config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT)
    etk_url = config.get(constants.CONFIG_OPTION_ETK_SERVER_URL, "").rstrip('/')
    media_root_cid = str(config.get(constants.CONFIG_OPTION_115_MEDIA_ROOT_CID, '0'))
    
    known_video_exts = {'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg'}
    known_sub_exts = {'srt', 'ass', 'ssa', 'sub', 'vtt', 'sup'}
    
    allowed_exts = set(e.lower() for e in config.get(constants.CONFIG_OPTION_115_EXTENSIONS, []))
    if not allowed_exts:
        allowed_exts = known_video_exts | known_sub_exts
    
    if not local_root or not etk_url:
        update_progress(100, "错误：未配置本地 STRM 根目录或 ETK 访问地址！")
        return

    client = P115Service.get_client()
    if not client: return

    raw_rules = settings_db.get_setting(constants.DB_KEY_115_SORTING_RULES)
    if not raw_rules: return
    rules = json.loads(raw_rules) if isinstance(raw_rules, str) else raw_rules
    
    # 1. 预处理：获取每个目标分类目录对应的完整相对路径 (参考 execute 逻辑)
    cid_to_rel_path = {}
    target_cids = []
    
    for r in rules:
        if r.get('enabled', True) and r.get('cid') and str(r['cid']) != '0':
            cid = str(r['cid'])
            target_cids.append(cid)
            # ★ 核心修改：直接从规则中读取 category_path
            if 'category_path' in r:
                cid_to_rel_path[cid] = r['category_path']
            else:
                # 兜底：使用规则中配置的名称
                cid_to_rel_path[cid] = r.get('dir_name', '未识别')

    valid_local_files = set() # 本地已存在的 STRM 和字幕文件绝对路径集合（仅当 enable_cleanup=True 时使用）
    successful_cids = set() # 记录成功处理过的 CID，最后用于清理本地多余文件
    # ==========================================
    # ★ 内部处理逻辑：接收 base_cid 来确定分类前缀
    # ==========================================
    def process_file_info(info, rel_path_parts, base_cid):
        nonlocal files_generated
        # 兼容 OpenAPI 键名
        name = info.get('fn') or info.get('n') or info.get('file_name', '')
        ext = name.split('.')[-1].lower() if '.' in name else ''
        if ext not in allowed_exts: return
        pc = info.get('pc') or info.get('pick_code')
        if not pc: return
        
        # 获取分类前缀路径 (例如 "纪录片/BBC")
        category_prefix = cid_to_rel_path.get(str(base_cid), "未识别")
        
        # 拼接本地路径：本地根目录 / 分类前缀 / 资源子目录 / 文件
        current_local_path = os.path.join(local_root, category_prefix, *rel_path_parts)
        os.makedirs(current_local_path, exist_ok=True)
        
        if ext in known_video_exts:
            strm_name = os.path.splitext(name)[0] + ".strm"
            strm_path = os.path.join(current_local_path, strm_name)
            content = f"{etk_url}/api/p115/play/{pc}"
            
            need_write = True
            if os.path.exists(strm_path):
                try:
                    with open(strm_path, 'r', encoding='utf-8') as f:
                        if f.read().strip() == content: need_write = False
                except: pass
                        
            if need_write:
                with open(strm_path, 'w', encoding='utf-8') as f: f.write(content)
                logger.debug(f"生成 STRM: {strm_name}")
            files_generated += 1
            valid_local_files.add(os.path.abspath(strm_path)) # 记录有效文件绝对路径
                
        elif ext in known_sub_exts:
            # 检查开关
            if download_subs:
                sub_path = os.path.join(current_local_path, name)
                if not os.path.exists(sub_path):
                    try:
                        import requests
                        url_obj = client.download_url(pc, user_agent="Mozilla/5.0")
                        if url_obj:
                            headers = {
                                "User-Agent": "Mozilla/5.0",
                                "Cookie": P115Service.get_cookies()
                            }
                            resp = requests.get(str(url_obj), stream=True, timeout=15, headers=headers)
                            resp.raise_for_status()
                            with open(sub_path, 'wb') as f:
                                for chunk in resp.iter_content(8192): f.write(chunk)
                            logger.info(f"下载字幕: {name}")
                        files_generated += 1
                        valid_local_files.add(os.path.abspath(sub_path)) # 记录有效文件绝对路径
                    except Exception as e:
                        logger.error(f"下载字幕失败 [{name}]: {e}")

    # ==========================================
    # 2. 遍历执行
    # ==========================================
    total_cids = len(target_cids)
    for idx, base_cid in enumerate(target_cids):
        base_prog = int((idx / total_cids) * 100)
        category_rel_path = cid_to_rel_path.get(base_cid)
        update_progress(base_prog, f"  ➜ 正在同步层级: {category_rel_path} (CID: {base_cid}) ...")
        
        items_yielded = 0
        files_generated = 0
        
        # A. 优先尝试极速遍历
        try:
            from p115client.tool.iterdir import iter_files_with_path_skim
            
            iterator = iter_files_with_path_skim(
                client, 
                int(base_cid), 
                with_ancestors=True, 
                max_workers=1 
            )
            
            for info in iterator:
                if processor and getattr(processor, 'is_stop_requested', lambda: False)():
                    update_progress(100, "任务已被用户手动终止。")
                    return
                
                # 只有带 fid 的才是文件，文件夹不参与 process_file_info
                fid = info.get('fid') or info.get('id')
                if not fid or info.get('ico') == 'folder':
                    continue

                items_yielded += 1
                
                ancestors = info.get('ancestors', [])
                rel_path_parts = []
                
                if isinstance(ancestors, list) and len(ancestors) > 0:
                    found_base = False
                    for node in ancestors:
                        node_id = str(node.get('id') or node.get('cid', ''))
                        
                        # 找到规则配置的根 CID
                        if node_id == str(base_cid):
                            found_base = True
                            continue
                        
                        if found_base:
                            # 修复点 1：确保这个节点不是文件本身（防止极速模式把文件当路径）
                            node_name = str(node.get('name', '')).strip()
                            if node_id != str(fid) and node_name:
                                rel_path_parts.append(node_name)
                
                # 修复点 2：双重保险。如果路径最后一位跟文件名完全一样（比如 115 里的特殊打包文件），剔除它
                file_real_name = info.get('n') or info.get('name', '')
                if rel_path_parts and rel_path_parts[-1] == file_real_name:
                    rel_path_parts.pop()

                process_file_info(info, rel_path_parts, base_cid)
                
        except Exception as e:
            logger.warning(f"  ⚠️ 极速遍历异常 CID:{base_cid} - 错误详情: {repr(e)}")

        # B. 自动降级：如果极速模式没出货，启动标准递归
        if items_yielded == 0:
            logger.warning(f"  ⚠️ 极速遍历未发现文件，正在使用标准递归扫描...")
            def reliable_recursive_scan(cid, current_parts):
                offset = 0
                limit = 1000
                while True:
                    if processor and getattr(processor, 'is_stop_requested', lambda: False)(): return
                    res = client.fs_files({'cid': cid, 'limit': limit, 'offset': offset, 'record_open_time': 0, 'count_folders': 0})
                    data = res.get('data', [])
                    if not data: break
                    for item in data:
                        if str(item.get('fc')) == '1':
                            process_file_info(item, current_parts, base_cid)
                        elif str(item.get('fc')) == '0':
                            reliable_recursive_scan(item.get('fid'), current_parts + [item.get('fn')])
                    if len(data) < limit: break
                    offset += limit
            
            try:
                reliable_recursive_scan(base_cid, [])
            except Exception as e:
                logger.error(f"标准扫描异常 CID:{base_cid}: {e}")
                
        logger.info(f"  ✅ [{category_rel_path}] 同步完成，处理文件: {files_generated}")
        if files_generated > 0:
            successful_cids.add(base_cid)
        # ==========================================
    # ★ 新增：安全的本地清理逻辑 (放在 for 循环外面，函数的末尾)
    # ==========================================
    if enable_cleanup:
        update_progress(95, "  🧹 正在执行本地多余文件清理...")
        cleaned_files = 0
        cleaned_dirs = 0
        
        for base_cid in successful_cids:
            category_rel_path = cid_to_rel_path.get(base_cid)
            target_local_dir = os.path.join(local_root, category_rel_path)
            
            if not os.path.exists(target_local_dir): continue
            
            # 1. 清理多余的文件 (只碰 strm 和 字幕)
            for root_dir, dirs, files in os.walk(target_local_dir):
                for file in files:
                    ext = file.split('.')[-1].lower()
                    if ext in known_sub_exts or ext == 'strm':
                        file_path = os.path.abspath(os.path.join(root_dir, file))
                        if file_path not in valid_local_files:
                            try:
                                os.remove(file_path)
                                cleaned_files += 1
                                logger.debug(f"  🗑️ [清理] 删除失效文件: {file}")
                            except Exception as e:
                                logger.warning(f"  ⚠️ 删除文件失败 {file}: {e}")
            
            # 2. 清理空文件夹 (自底向上)
            for root_dir, dirs, files in os.walk(target_local_dir, topdown=False):
                for d in dirs:
                    dir_path = os.path.join(root_dir, d)
                    try:
                        if not os.listdir(dir_path): # 如果文件夹为空
                            os.rmdir(dir_path)
                            cleaned_dirs += 1
                    except: pass
                    
        logger.info(f"  🧹 清理完成: 删除了 {cleaned_files} 个失效文件, {cleaned_dirs} 个空目录。")

    end_msg = "=== 全量 STRM 与字幕同步结束 ===" if download_subs else "=== 全量 STRM 生成结束 ==="
    update_progress(100, end_msg)

def delete_115_files_by_webhook(item_path, pickcodes):
    """
    接收神医 Webhook 传来的路径和提取码，精准销毁 115 网盘文件。
    ★ 增加防风控限流与熔断保护机制
    """
    if not pickcodes or not item_path: return

    client = P115Service.get_client()
    if not client: return

    try:
        # 1. 从本地路径中提取带有 TMDb ID 的主目录名称 (例如: 爱我爱我 (2026) {tmdb=1317672})
        match = re.search(r'([^/\\]+\{tmdb=\d+\})', item_path)
        if not match:
            logger.warning(f"  ⚠️ [联动删除] 无法从路径提取 TMDb 目录名: {item_path}")
            return
        tmdb_folder_name = match.group(1)

        # 2. 查找该主目录在 115 上的 CID
        base_cid = P115CacheManager.get_cid_by_name(tmdb_folder_name)
        if not base_cid:
            # 缓存没命中，尝试模糊搜索兜底
            try:
                time.sleep(1.5) # ★ 搜索接口风控极严，必须加睡眠限流
                res = client.fs_files({'search_value': tmdb_folder_name, 'limit': 1000, 'record_open_time': 0, 'count_folders': 0})
                for item in res.get('data', []):
                    if item.get('fn') == tmdb_folder_name and str(item.get('fc')) == '0':
                        base_cid = item.get('fid')
                        break
            except Exception as e:
                logger.warning(f"  ⚠️ [联动删除] 模糊搜索目录 '{tmdb_folder_name}' 时被风控或报错: {e}")

        if not base_cid:
            logger.warning(f"  ⚠️ [联动删除] 未在 115 找到对应主目录，可能已被删除: {tmdb_folder_name}")
            return

        # 3. 递归扫描该主目录，将 Pickcode 映射为 115 的文件 ID (fid)
        fids_to_delete = []
        
        def scan_and_match(cid):
            try:
                time.sleep(1.5) # ★ 强制防风控限流：每次请求间隔 1.5 秒
                res = client.fs_files({'cid': cid, 'limit': 1000, 'record_open_time': 0, 'count_folders': 0})
                for item in res.get('data', []):
                    if str(item.get('fc')) == '1':
                        if item.get('pc') in pickcodes:
                            fids_to_delete.append(item.get('fid'))
                    elif str(item.get('fc')) == '0':
                        scan_and_match(item.get('fid'))
            except Exception as e:
                logger.warning(f"  ⚠️ [联动删除] 扫描目录 {cid} 时被风控或报错: {e}")

        logger.debug(f"  🔍 [联动删除] 正在网盘目录 '{tmdb_folder_name}' 中匹配文件 (带防风控延迟)...")
        scan_and_match(base_cid)

        # 4. 执行物理销毁
        if fids_to_delete:
            resp = client.fs_delete(fids_to_delete)
            if resp.get('state'):
                logger.info(f"  💥 [联动删除] 成功在 115 网盘删除了 {len(fids_to_delete)} 个文件！")
            else:
                logger.error(f"  ❌ [联动删除] 115 删除接口调用失败: {resp}")

            # 5. 鞭尸检查：如果主目录里已经没有视频文件了，连目录一起扬了
            video_count = 0
            def count_videos(cid):
                nonlocal video_count
                try:
                    time.sleep(1.5) # ★ 强制防风控限流
                    res = client.fs_files({'cid': cid, 'limit': 1000, 'record_open_time': 0, 'count_folders': 0})
                    for item in res.get('data', []):
                        if str(item.get('fc')) == '1':
                            ext = str(item.get('fn', '')).split('.')[-1].lower()
                            if ext in ['mp4', 'mkv', 'avi', 'ts', 'iso']:
                                video_count += 1
                        elif str(item.get('fc')) == '0':
                            count_videos(item.get('fid'))
                except Exception as e:
                    logger.warning(f"  ⚠️ [联动删除] 检查空目录 {cid} 时报错: {e}")
                    # ★ 熔断保护：如果接口报错，假装里面还有视频，绝对不执行删目录操作！
                    video_count += 999 

            count_videos(base_cid)
            if video_count == 0:
                client.fs_delete(base_cid)
                P115CacheManager.delete_cid(base_cid) # 清理本地缓存
                logger.info(f"  🧹 [联动删除] 清理本地主目录缓存: {tmdb_folder_name}")
            else:
                logger.debug(f"  🛡️ [联动删除] 目录内仍有视频或检查受阻，保留主目录。")
        else:
            logger.warning(f"  ⚠️ [联动删除] 扫描完毕，但未在网盘找到匹配的提取码文件。")

    except Exception as e:
        logger.error(f"  ❌ [联动删除] 执行异常: {e}", exc_info=True)
