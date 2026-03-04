"""
任务7：转化数据统计
- 订单详情 uid 匹配全部学员名单期名称，回填最后列
- 按渠道统计飞策/自研页转化数，写入数据汇总 H/I/J/L 列
"""

from feishu_client import (
    parse_feishu_url, find_sheet_by_name,
    read_sheet_values, write_sheet_values
)
from task3_dedup_merge import col_letter
from task6_live_stats import CHANNEL_ROWS, match_period

# 订单详情列索引
O_COL_UNIONID = 1   # B
O_COL_PLATFORM = 12 # M 正式课订单来源平台

# 全部学员名单列索引
T_COL_UNIONID = 1
T_COL_PERIOD = 9


def task7_conversion_stats(spreadsheet_token: str):
    print("\n[任务7] 开始处理...")

    # 读取全部学员名单（终），构建 uid -> 期名称
    t_sheet = find_sheet_by_name(spreadsheet_token, "全部学员名单（终）")
    t_rows = read_sheet_values(spreadsheet_token, t_sheet["sheet_id"])
    uid_to_period = {}
    for row in t_rows[1:]:
        uid = str(row[T_COL_UNIONID]).strip() if T_COL_UNIONID < len(row) and row[T_COL_UNIONID] else ""
        period = str(row[T_COL_PERIOD]).strip() if T_COL_PERIOD < len(row) and row[T_COL_PERIOD] else ""
        if uid:
            uid_to_period[uid] = period
    print(f"全部学员名单 uid 映射：{len(uid_to_period)} 条")

    # 读取订单详情
    o_sheet = find_sheet_by_name(spreadsheet_token, "订单详情")
    o_sid = o_sheet["sheet_id"]
    o_rows = read_sheet_values(spreadsheet_token, o_sid)
    o_header = o_rows[0]
    o_data = [r for r in o_rows[1:] if any(c for c in r)]
    print(f"订单详情共 {len(o_data)} 行")

    if not o_data:
        print("订单详情无数据，跳过。")
        return

    # 回填期名称到最后一列
    last_col = len(o_header)
    while last_col > 0 and not o_header[last_col - 1]:
        last_col -= 1
    new_col_letter = col_letter(last_col)
    print(f"期名称回填到第 {last_col+1} 列（{new_col_letter}列）")

    period_values = []
    for row in o_data:
        uid = str(row[O_COL_UNIONID]).strip() if O_COL_UNIONID < len(row) and row[O_COL_UNIONID] else ""
        period_values.append([uid_to_period.get(uid, "")])

    write_sheet_values(spreadsheet_token, o_sid,
                       f"{new_col_letter}2:{new_col_letter}{len(o_data)+1}",
                       period_values)
    print("期名称回填完成")

    # 构建每行 (期名称, 平台)
    row_data = []
    for i, row in enumerate(o_data):
        period = period_values[i][0]
        platform = str(row[O_COL_PLATFORM]).strip() if O_COL_PLATFORM < len(row) and row[O_COL_PLATFORM] else ""
        row_data.append((period, platform))

    # 读取数据汇总 D 列（公开课助教添加人数）
    s_sheet = find_sheet_by_name(spreadsheet_token, "数据汇总")
    s_sid = s_sheet["sheet_id"]
    s_rows = read_sheet_values(spreadsheet_token, s_sid, "A1:N12")
    d_col = {}
    for i, row in enumerate(s_rows):
        val = row[3] if len(row) > 3 else None
        try:
            d_col[i+1] = int(val) if val else 0
        except (ValueError, TypeError):
            d_col[i+1] = 0

    # 按渠道统计
    print("\n按渠道统计转化数据：")
    total_feice = total_ziran = total_conv = 0

    for feishu_row, mode, keyword in CHANNEL_ROWS:
        feice = sum(1 for p, pl in row_data if match_period(p, mode, keyword) and pl == "飞策")
        ziran = sum(1 for p, pl in row_data if match_period(p, mode, keyword) and pl == "自研页")
        conv = feice + ziran
        d_base = d_col.get(feishu_row, 0)
        rate = f"{conv/d_base*100:.1f}%" if d_base > 0 else "/"

        write_sheet_values(spreadsheet_token, s_sid,
                           f"H{feishu_row}:J{feishu_row}", [[feice, ziran, conv]])
        write_sheet_values(spreadsheet_token, s_sid,
                           f"L{feishu_row}:L{feishu_row}", [[rate]])

        total_feice += feice
        total_ziran += ziran
        total_conv += conv
        print(f"  行{feishu_row}: 飞策={feice}, 自研页={ziran}, 合计={conv}, 转化率={rate}")

    # 合计行
    d_total = d_col.get(11, 0)
    total_rate = f"{total_conv/d_total*100:.1f}%" if d_total > 0 else "/"
    write_sheet_values(spreadsheet_token, s_sid,
                       f"H11:J11", [[total_feice, total_ziran, total_conv]])
    write_sheet_values(spreadsheet_token, s_sid,
                       f"L11:L11", [[total_rate]])
    print(f"  合计: 飞策={total_feice}, 自研页={total_ziran}, 合计={total_conv}, 转化率={total_rate}")

    print(f"\n[任务7] 完成！")
