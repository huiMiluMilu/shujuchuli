"""
任务4：全部学员名单生成（终版）
- 以全部学员名单 UID 为基准，匹配助教好友表
- 找到则填添加时间(I列)和是否添加=是(H列)，找不到则填否
- 更名为全部学员名单（终）
"""

import requests
from feishu_client import (
    parse_feishu_url, find_sheet_by_name,
    read_sheet_values, write_sheet_values, _headers
)
from task3_dedup_merge import rename_sheet

# 全部学员名单列索引
T_COL_UNIONID = 1   # B
T_COL_IS_ADD = 7    # H 是否添加
T_COL_ADD_TIME = 8  # I 添加时间

# 助教好友列索引
F_COL_ADD_TIME = 4  # E 添加时间
F_COL_UNIONID = 5   # F unionid


def task4_final_member_list(spreadsheet_token: str):
    print("\n[任务4] 开始处理...")

    # 1. 读取助教好友，构建 unionid -> 添加时间 映射
    friend_sheet = find_sheet_by_name(spreadsheet_token, "助教好友")
    f_rows = read_sheet_values(spreadsheet_token, friend_sheet["sheet_id"])
    f_data = f_rows[1:]

    uid_to_add_time: dict[str, str] = {}
    for row in f_data:
        uid = str(row[F_COL_UNIONID]).strip() if F_COL_UNIONID < len(row) and row[F_COL_UNIONID] else ""
        add_time = str(row[F_COL_ADD_TIME]).strip() if F_COL_ADD_TIME < len(row) and row[F_COL_ADD_TIME] else ""
        if uid and add_time:
            uid_to_add_time[uid] = add_time

    print(f"助教好友共加载 {len(uid_to_add_time)} 条 uid 映射")

    # 2. 读取全部学员名单
    target_sheet = find_sheet_by_name(spreadsheet_token, "全部学员名单")
    t_sid = target_sheet["sheet_id"]
    t_rows = read_sheet_values(spreadsheet_token, t_sid)
    t_data = t_rows[1:]
    print(f"全部学员名单共 {len(t_data)} 行")

    # 3. 构建 H、I 列写入数据
    is_add_col = []
    add_time_col = []
    matched = 0
    unmatched = 0

    for row in t_data:
        uid = str(row[T_COL_UNIONID]).strip() if T_COL_UNIONID < len(row) and row[T_COL_UNIONID] else ""
        if uid and uid in uid_to_add_time:
            is_add_col.append(["是"])
            add_time_col.append([uid_to_add_time[uid]])
            matched += 1
        else:
            is_add_col.append(["否"])
            add_time_col.append([None])
            unmatched += 1

    print(f"匹配成功：{matched}，未匹配：{unmatched}")

    # 4. 批量写入
    n = len(t_data)
    write_sheet_values(spreadsheet_token, t_sid, f"H2:H{n+1}", is_add_col)
    write_sheet_values(spreadsheet_token, t_sid, f"I2:I{n+1}", add_time_col)
    print("H列（是否添加）、I列（添加时间）写入完成")

    # 5. 更名
    rename_sheet(spreadsheet_token, t_sid, "全部学员名单（终）")
    print("全部学员名单 → 全部学员名单（终）")

    print(f"[任务4] 完成！")
