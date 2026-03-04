"""
任务1：进群渠道归属标记
- 扫描【群成员名单】子表格的"进群方式"列（I 列）
- 若值为"通过扫描群二维码入群"，读取"群名称"列（A 列）
- 从【群归属配置】子表格自动读取群名 -> 期名称映射
- 将结果填入"期名称"列（N 列）
"""

from feishu_client import find_sheet_by_name, read_sheet_values, write_sheet_values

# 列索引（0-based），对应 A=0, B=1, ...
COL_GROUP_NAME = 0    # A 列：群名
COL_JOIN_METHOD = 8   # I 列：进群方式
COL_INVITER = 9       # J 列：邀请人助教号
COL_PERIOD_NAME = 13  # N 列：期名称

TRIGGER_VALUE = "通过扫描群二维码入群"

# 邀请人助教号 -> 期名称 固定映射
INVITER_MAPPING = {
    "贝贝61": "芳群（二维码）",
    "加加154": "雪楠（二维码）",
}


def _load_group_mapping(spreadsheet_token: str) -> dict[str, str]:
    """从【群归属配置】表读取群名 -> 期名称映射"""
    sheet = find_sheet_by_name(spreadsheet_token, "群归属配置")
    rows = read_sheet_values(spreadsheet_token, sheet["sheet_id"])
    mapping = {}
    for row in rows[1:]:  # 跳过表头
        group = str(row[0]).strip() if len(row) > 0 and row[0] else ""
        period = str(row[1]).strip() if len(row) > 1 and row[1] else ""
        if group and period:
            mapping[group] = period
    return mapping


def task1_mark_channel(spreadsheet_token: str, sheet_name: str = "群成员名单"):
    """执行任务1：进群渠道归属标记"""

    print(f"\n[任务1] 开始处理子表格「{sheet_name}」...")

    # 1. 从群归属配置表加载群名映射
    group_mapping = _load_group_mapping(spreadsheet_token)
    print(f"从【群归属配置】加载 {len(group_mapping)} 条映射：")
    for k, v in group_mapping.items():
        print(f"  {k} → {v}")

    # 2. 定位子表格
    sheet = find_sheet_by_name(spreadsheet_token, sheet_name)
    sheet_id = sheet["sheet_id"]

    # 3. 读取全部数据
    rows = read_sheet_values(spreadsheet_token, sheet_id)
    if not rows:
        print("表格为空，跳过。")
        return

    data_rows = rows[1:]  # 跳过表头，数据从第2行开始（行索引1，飞书行号2）
    print(f"共读取 {len(data_rows)} 行数据（不含表头）")

    # 4. 扫描触发行，收集群名 -> 行索引映射
    group_to_rows: dict[str, list[int]] = {}
    for i, row in enumerate(data_rows):
        join_method = _get_cell(row, COL_JOIN_METHOD)
        if join_method == TRIGGER_VALUE:
            group_name = _get_cell(row, COL_GROUP_NAME)
            group_to_rows.setdefault(group_name, []).append(i)

    if not group_to_rows:
        print("未发现通过二维码进群的记录，任务结束。")
        return

    print(f"\n检测到以下群通过二维码进群：{list(group_to_rows.keys())}")

    # 未配置的群名给出警告
    for g in group_to_rows:
        if g and g not in group_mapping:
            print(f"  [警告] 群「{g}」未在群归属配置中找到，将跳过")

    # 5. 将结果写回 N 列（批量）
    updates: dict[int, str] = {}
    for group_name, rows_idx in group_to_rows.items():
        period_name = group_mapping.get(group_name, "")
        if not period_name:
            continue  # 未配置的群跳过
        for i in rows_idx:
            updates[i + 2] = period_name

    # 补充规则：N 列仍为空，且邀请人助教号在映射表中的行
    for i, row in enumerate(data_rows):
        feishu_row = i + 2
        if feishu_row in updates:
            continue  # 已被二维码规则覆盖
        current_period = _get_cell(row, COL_PERIOD_NAME)
        if current_period:
            continue  # N 列已有值
        inviter = _get_cell(row, COL_INVITER)
        if inviter in INVITER_MAPPING:
            updates[feishu_row] = INVITER_MAPPING[inviter]

    print(f"\n开始写入 {len(updates)} 行数据到 N 列...")
    min_row = min(updates.keys())
    max_row = max(updates.keys())
    existing = read_sheet_values(spreadsheet_token, sheet_id, f"N{min_row}:N{max_row}")
    existing_flat = [(r[0] if r else None) for r in existing]
    while len(existing_flat) < max_row - min_row + 1:
        existing_flat.append(None)
    for feishu_row, value in updates.items():
        existing_flat[feishu_row - min_row] = value
    write_sheet_values(spreadsheet_token, sheet_id, f"N{min_row}:N{max_row}",
                       [[v] for v in existing_flat])
    print(f"[任务1] 完成！共写入 {len(updates)} 行。")


def _get_cell(row: list, col_index: int) -> str:
    """安全获取行中指定列的值，超出范围返回空字符串"""
    if col_index < len(row):
        return str(row[col_index]).strip() if row[col_index] is not None else ""
    return ""
