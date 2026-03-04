"""飞书 API 客户端 - 负责认证和基础数据读写"""
import re
import requests

APP_ID = "cli_a914e342be385bb6"
APP_SECRET = "L4fB7JHGTYqUeziwcanTMBKECugw2lia"

_token_cache = {"token": None, "expire": 0}


def get_tenant_access_token() -> str:
    """获取飞书 tenant_access_token，带缓存"""
    import time
    if _token_cache["token"] and time.time() < _token_cache["expire"]:
        return _token_cache["token"]

    resp = requests.post(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        json={"app_id": APP_ID, "app_secret": APP_SECRET},
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"获取 token 失败: {data}")

    _token_cache["token"] = data["tenant_access_token"]
    _token_cache["expire"] = time.time() + data["expire"] - 60
    return _token_cache["token"]


def _headers() -> dict:
    return {"Authorization": f"Bearer {get_tenant_access_token()}"}


def parse_feishu_url(url: str) -> tuple[str, str | None]:
    """
    从飞书电子表格 URL 中提取 spreadsheet_token 和 sheet_id。
    返回 (spreadsheet_token, sheet_id)，sheet_id 可能为 None。

    支持格式：
    - https://xxx.feishu.cn/sheets/shtXXXXXX?sheet=XXXXXX
    - https://xxx.feishu.cn/spreadsheets/shtXXXXXX
    - https://xxx.feishu.cn/sheets/shtXXXXXX（无 sheet 参数）
    """
    # 提取 spreadsheet_token
    match = re.search(r"/(?:sheets|spreadsheets)/([A-Za-z0-9]+)", url)
    if not match:
        raise ValueError(f"无法从 URL 解析 spreadsheet_token: {url}")
    spreadsheet_token = match.group(1)

    # 提取 sheet_id（可选）
    sheet_match = re.search(r"[?&]sheet=([A-Za-z0-9]+)", url)
    sheet_id = sheet_match.group(1) if sheet_match else None

    return spreadsheet_token, sheet_id


def get_sheets_meta(spreadsheet_token: str) -> list[dict]:
    """获取电子表格所有子表格的元信息（title、sheetId 等）"""
    url = f"https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/{spreadsheet_token}/sheets/query"
    resp = requests.get(url, headers=_headers())
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"获取子表格列表失败: {data}")
    return data["data"]["sheets"]


def find_sheet_by_name(spreadsheet_token: str, sheet_name: str) -> dict:
    """按名称查找子表格，返回其元信息（含 sheetId）"""
    sheets = get_sheets_meta(spreadsheet_token)
    for sheet in sheets:
        if sheet.get("title") == sheet_name:
            return sheet
    available = [s["title"] for s in sheets]
    raise ValueError(f"找不到子表格「{sheet_name}」，现有子表格：{available}")


def read_sheet_values(spreadsheet_token: str, sheet_id: str, range_str: str = None) -> list[list]:
    """
    读取子表格数据。
    range_str 示例："A1:Z1000"，不传则读取整个表格。
    返回二维列表（行 × 列）。
    """
    if range_str:
        range_param = f"{sheet_id}!{range_str}"
    else:
        range_param = sheet_id

    url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{range_param}"
    resp = requests.get(url, headers=_headers(), params={"valueRenderOption": "ToString"})
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"读取表格数据失败: {data}")
    return data["data"]["valueRange"].get("values", [])


def write_sheet_values(spreadsheet_token: str, sheet_id: str, range_str: str, values: list[list]):
    """
    写入子表格数据。
    range_str 示例："N2:N100"
    values 为二维列表。
    """
    range_param = f"{sheet_id}!{range_str}"
    url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values"
    payload = {
        "valueRange": {
            "range": range_param,
            "values": values,
        }
    }
    resp = requests.put(url, headers=_headers(), json=payload)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"写入表格数据失败: {data}")
    return data
