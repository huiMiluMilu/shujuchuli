"""
任务3：多源数据跨表去重与全量汇总

第一阶段：跨表冲突检测（群成员名单 vs AI BOSS）
第二阶段：单表冲突检测（群成员名单内部 UnionID 重复）
第三阶段：全量学员名单生成
"""

import requests
from datetime import datetime
from feishu_client import (
    parse_feishu_url, find_sheet_by_name, get_sheets_meta,
    read_sheet_values, write_sheet_values, _headers
)

# 群成员名单列索引
M_COL_UNIONID = 5      # F
M_COL_JOIN_TIME = 7    # H 进群时间
M_COL_JOIN_METHOD = 8  # I 进群方式
M_COL_PERIOD = 13      # N 期名称
M_COL_STATUS = 14      # O 新增状态列

# AI BOSS 列索引
B_COL_PERIOD = 1       # B 期名称
B_COL_UID = 14         # O uid
B_COL_ADD_TIME = 19    # T 添加时间
B_COL_STATUS = 47      # AV 新增状态列

TRIGGER_VALUE = "通过扫描群二维码入群"
STATUS_KEEP = "去重-保留"
STATUS_DISCARD = "去重-废弃"
STATUS_VALID = "有效"
GRAY = "#cccccc"
WHITE = "#FFFFFF"


def col_letter(idx):
    """0-based 列索引转列字母"""
    result = ""
    idx += 1
    while idx:
        idx, r = divmod(idx - 1, 26)
        result = chr(65 + r) + result
    return result


def parse_time(s) -> datetime | None:
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y/%m/%d, %H:%M", "%Y/%m/%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def set_row_background(spreadsheet_token: str, sheet_id: str, row: int, color: str):
    payload = {
        "appendStyle": {
            "range": f"{sheet_id}!A{row}:{col_letter(50)}{row}",
            "style": {"backColor": color}
        }
    }
    resp = requests.put(
        f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/style",
        headers=_headers(), json=payload
    )
    resp.raise_for_status()


def rename_sheet(spreadsheet_token: str, sheet_id: str, new_title: str):
    url = f"https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/{spreadsheet_token}/sheets/{sheet_id}"
    resp = requests.patch(url, headers=_headers(), json={"title": new_title})
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"重命名失败: {data}")


def task3_dedup_and_merge(spreadsheet_token: str):
    print("\n[任务3] 开始处理...")

    # ── 读取数据 ────────────────────────────────────────────────
    member_sheet = find_sheet_by_name(spreadsheet_token, "群成员名单")
    m_sid = member_sheet["sheet_id"]
    boss_sheet = find_sheet_by_name(spreadsheet_token, "AI BOSS")
    b_sid = boss_sheet["sheet_id"]

    m_rows = read_sheet_values(spreadsheet_token, m_sid)
    b_rows = read_sheet_values(spreadsheet_token, b_sid)
    m_data = m_rows[1:]
    b_data = b_rows[1:]

    # 初始化状态：所有行默认"有效"
    m_status = [STATUS_VALID] * len(m_data)
    b_status = [STATUS_VALID] * len(b_data)
    m_gray_rows = set()
    b_gray_rows = set()

    # ── 第一阶段：跨表冲突检测 ──────────────────────────────────
    print("\n--- 第一阶段：跨表冲突检测（群成员名单 vs AI BOSS）---")

    boss_uid_map: dict[str, list[int]] = {}
    for i, row in enumerate(b_data):
        uid = str(row[B_COL_UID]).strip() if B_COL_UID < len(row) and row[B_COL_UID] else ""
        if uid:
            boss_uid_map.setdefault(uid, []).append(i)

    cross_conflict = 0
    for mi, row in enumerate(m_data):
        join_method = str(row[M_COL_JOIN_METHOD]).strip() if M_COL_JOIN_METHOD < len(row) and row[M_COL_JOIN_METHOD] else ""
        if join_method != TRIGGER_VALUE:
            continue

        unionid = str(row[M_COL_UNIONID]).strip() if M_COL_UNIONID < len(row) and row[M_COL_UNIONID] else ""
        if not unionid or unionid not in boss_uid_map:
            continue

        m_time = parse_time(row[M_COL_JOIN_TIME] if M_COL_JOIN_TIME < len(row) else None)

        for bi in boss_uid_map[unionid]:
            b_row = b_data[bi]
            b_time = parse_time(b_row[B_COL_ADD_TIME] if B_COL_ADD_TIME < len(b_row) else None)
            cross_conflict += 1

            if m_time and b_time:
                if m_time <= b_time:
                    m_status[mi] = STATUS_KEEP
                    b_status[bi] = STATUS_DISCARD
                    b_gray_rows.add(bi + 2)
                else:
                    b_status[bi] = STATUS_KEEP
                    m_status[mi] = STATUS_DISCARD
                    m_gray_rows.add(mi + 2)
            else:
                m_status[mi] = STATUS_KEEP
                b_status[bi] = STATUS_DISCARD
                b_gray_rows.add(bi + 2)

    print(f"跨表冲突 {cross_conflict} 条")

    # ── 第二阶段：单表冲突检测（群成员名单内部 UnionID 重复）──────
    print("\n--- 第二阶段：单表冲突检测（群成员名单内部重复 UnionID）---")

    # 收集每个 unionid 对应的所有行索引
    uid_rows: dict[str, list[int]] = {}
    for i, row in enumerate(m_data):
        uid = str(row[M_COL_UNIONID]).strip() if M_COL_UNIONID < len(row) and row[M_COL_UNIONID] else ""
        if uid:
            uid_rows.setdefault(uid, []).append(i)

    intra_conflict = 0
    for uid, idxs in uid_rows.items():
        if len(idxs) <= 1:
            continue
        intra_conflict += 1

        # 找进群时间最早的行
        best_idx = None
        best_time = None
        for i in idxs:
            t = parse_time(m_data[i][M_COL_JOIN_TIME] if M_COL_JOIN_TIME < len(m_data[i]) else None)
            if best_time is None or (t and t < best_time):
                best_time = t
                best_idx = i

        for i in idxs:
            if i == best_idx:
                # 只有当前行还没被跨表标为废弃时才标保留
                if m_status[i] != STATUS_DISCARD:
                    m_status[i] = STATUS_KEEP
            else:
                if m_status[i] != STATUS_DISCARD:
                    m_status[i] = STATUS_DISCARD
                    m_gray_rows.add(i + 2)

    print(f"单表重复 UnionID {intra_conflict} 个")
    print(f"群成员名单：去重-保留 {m_status.count(STATUS_KEEP)}，去重-废弃 {m_status.count(STATUS_DISCARD)}，有效 {m_status.count(STATUS_VALID)}")
    print(f"AI BOSS：去重-保留 {b_status.count(STATUS_KEEP)}，去重-废弃 {b_status.count(STATUS_DISCARD)}，有效 {b_status.count(STATUS_VALID)}")

    # 写入状态列
    print("\n写入状态列...")
    write_sheet_values(spreadsheet_token, m_sid,
                       f"O2:O{len(m_data)+1}", [[s] for s in m_status])
    write_sheet_values(spreadsheet_token, b_sid,
                       f"AV2:AV{len(b_data)+1}", [[s] for s in b_status])

    # 设置灰色背景
    print(f"设置灰色背景：群成员名单 {len(m_gray_rows)} 行，AI BOSS {len(b_gray_rows)} 行...")
    for r in sorted(m_gray_rows):
        set_row_background(spreadsheet_token, m_sid, r, GRAY)
    for r in sorted(b_gray_rows):
        set_row_background(spreadsheet_token, b_sid, r, GRAY)

    # ── 第三阶段：全量学员名单生成 ──────────────────────────────
    print("\n--- 第三阶段：全量学员名单生成 ---")

    target_sheet = find_sheet_by_name(spreadsheet_token, "全部学员名单")
    t_sid = target_sheet["sheet_id"]
    t_rows = read_sheet_values(spreadsheet_token, t_sid)
    t_header = t_rows[0]
    num_cols = len(t_header)

    target_col_idx = {}
    for ci, h in enumerate(t_header):
        if h:
            target_col_idx[h] = ci

    # 群成员名单字段映射
    M_FIELD_MAP = {
        "群名": 0, "unionid": 5, "昵称": 6, "进群时间": 7,
        "进群方式": 8, "邀请人助教号": 9, "期名称": 13,
    }
    # AI BOSS 字段映射
    B_FIELD_MAP = {
        "unionid": 14, "昵称": 17, "手机号码": 15,
        "是否添加": 18, "添加时间": 19, "期名称": 1,
    }

    def fill_row(field_map, src_row):
        out = [None] * num_cols
        for field, src_ci in field_map.items():
            if field in target_col_idx and src_ci < len(src_row):
                out[target_col_idx[field]] = src_row[src_ci]
        return out

    result_rows = []
    seen_uids = set()

    # 从群成员名单抽取
    m_added = 0
    for i, row in enumerate(m_data):
        if m_status[i] in (STATUS_KEEP, STATUS_VALID):
            uid = str(row[M_COL_UNIONID]).strip() if M_COL_UNIONID < len(row) and row[M_COL_UNIONID] else ""
            result_rows.append(fill_row(M_FIELD_MAP, row))
            if uid:
                seen_uids.add(uid)
            m_added += 1

    # 从 AI BOSS 抽取（排除已在群成员名单中的 uid）
    b_added = 0
    for i, row in enumerate(b_data):
        if b_status[i] in (STATUS_KEEP, STATUS_VALID):
            uid = str(row[B_COL_UID]).strip() if B_COL_UID < len(row) and row[B_COL_UID] else ""
            if uid not in seen_uids:
                result_rows.append(fill_row(B_FIELD_MAP, row))
                b_added += 1

    print(f"群成员名单贡献 {m_added} 行，AI BOSS 贡献 {b_added} 行，合计 {len(result_rows)} 行")

    if result_rows:
        write_sheet_values(spreadsheet_token, t_sid,
                           f"A2:{col_letter(num_cols-1)}{len(result_rows)+1}",
                           result_rows)

    print(f"[任务3] 完成！全部学员名单共写入 {len(result_rows)} 行。")
