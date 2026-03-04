"""
任务2：基于成员邀请的归属匹配
- 触发：I 列含"邀请链接入群"或"直接邀请入群"，且 N 列为空
- 以群成员名单 F 列(unionid) 匹配 AI BOSS O 列(uid)
- 匹配到则写入 AI BOSS B 列(期名称)，否则写"未匹配到渠道"
"""

from feishu_client import parse_feishu_url, find_sheet_by_name, read_sheet_values, write_sheet_values

COL_UNIONID = 5       # F 列：unionid
COL_JOIN_METHOD = 8   # I 列：进群方式
COL_PERIOD_NAME = 13  # N 列：期名称

TRIGGER_KEYWORDS = ["邀请链接入群", "直接邀请入群"]

# AI BOSS 列索引
BOSS_COL_PERIOD = 1   # B 列：期名称
BOSS_COL_UID = 14     # O 列：uid


def task2_invite_match(spreadsheet_token: str, sheet_name: str = "群成员名单", boss_sheet_name: str = "AI BOSS"):
    print(f"\n[任务2] 开始处理...")

    # 1. 读取 AI BOSS，构建 uid -> 期名称 映射
    print(f"读取「{boss_sheet_name}」构建 uid 映射...")
    boss_sheet = find_sheet_by_name(spreadsheet_token, boss_sheet_name)
    boss_rows = read_sheet_values(spreadsheet_token, boss_sheet["sheet_id"])
    boss_rows = boss_rows[1:]  # 跳过表头

    uid_to_period: dict[str, str] = {}
    for row in boss_rows:
        uid = str(row[BOSS_COL_UID]).strip() if BOSS_COL_UID < len(row) and row[BOSS_COL_UID] else ""
        period = str(row[BOSS_COL_PERIOD]).strip() if BOSS_COL_PERIOD < len(row) and row[BOSS_COL_PERIOD] else ""
        if uid and period:
            uid_to_period[uid] = period

    print(f"AI BOSS 共加载 {len(uid_to_period)} 条 uid 映射")

    # 2. 读取群成员名单
    member_sheet = find_sheet_by_name(spreadsheet_token, sheet_name)
    sheet_id = member_sheet["sheet_id"]
    rows = read_sheet_values(spreadsheet_token, sheet_id)
    data_rows = rows[1:]
    print(f"群成员名单共 {len(data_rows)} 行数据")

    # 3. 扫描触发行
    updates: dict[int, str] = {}  # 飞书行号 -> 期名称
    matched = 0
    unmatched = 0

    for i, row in enumerate(data_rows):
        join_method = str(row[COL_JOIN_METHOD]).strip() if COL_JOIN_METHOD < len(row) and row[COL_JOIN_METHOD] else ""
        period_name = str(row[COL_PERIOD_NAME]).strip() if COL_PERIOD_NAME < len(row) and row[COL_PERIOD_NAME] not in (None, "") else ""

        # 触发条件：进群方式含关键词 且 N 列为空
        if any(kw in join_method for kw in TRIGGER_KEYWORDS) and not period_name:
            unionid = str(row[COL_UNIONID]).strip() if COL_UNIONID < len(row) and row[COL_UNIONID] else ""
            feishu_row = i + 2

            if unionid and unionid in uid_to_period:
                updates[feishu_row] = uid_to_period[unionid]
                matched += 1
            else:
                updates[feishu_row] = "未匹配到渠道"
                unmatched += 1

    print(f"触发行数：{matched + unmatched}（匹配成功 {matched}，未匹配 {unmatched}）")

    if not updates:
        print("无需更新，任务结束。")
        return

    # 4. 批量写入 N 列
    min_row = min(updates.keys())
    max_row = max(updates.keys())

    # 读取现有 N 列，保留非触发行的原值
    existing = read_sheet_values(spreadsheet_token, sheet_id, f"N{min_row}:N{max_row}")
    existing_flat = []
    for r in existing:
        existing_flat.append(r[0] if r else None)
    while len(existing_flat) < max_row - min_row + 1:
        existing_flat.append(None)

    for feishu_row, value in updates.items():
        existing_flat[feishu_row - min_row] = value

    write_values = [[v] for v in existing_flat]
    write_sheet_values(spreadsheet_token, sheet_id, f"N{min_row}:N{max_row}", write_values)
    print(f"[任务2] 完成！写入范围 N{min_row}:N{max_row}，共 {len(updates)} 行。")
