"""
任务5：导流数据全口径统计
- 计算数据汇总表 C/D/E/F/G 列
"""

from feishu_client import (
    parse_feishu_url, find_sheet_by_name,
    read_sheet_values, write_sheet_values
)

# 全部学员名单列索引
T_COL_JOIN_TIME = 4   # E 进群时间
T_COL_IS_ADD = 7      # H 是否添加
T_COL_PERIOD = 9      # J 期名称

# AI BOSS（去重）列索引
B_COL_PERIOD = 1      # B 期名称

# 数据汇总行号（1-based飞书行号）及对应渠道配置
# 格式：(飞书行号, 渠道名称, 匹配模式, 匹配关键词)
# 匹配模式：'exact' = 精确匹配全部学员名单J列, 'keyword' = 关键词匹配AI BOSS B列
CHANNEL_CONFIG = [
    (4,  "用户运营部-公开课长尾-芳群",         "keyword", "芳群"),
    (5,  "用户运营-正式课和体验课-雪楠",        "keyword", "雪楠"),
    (6,  "正式课运营-坚平",                    "keyword", "坚平"),
    (7,  "老师外部渠道",                        "keyword", "老师"),
    (8,  "芳群（二维码）",                      "exact",   "芳群（二维码）"),
    (9,  "雪楠（二维码）",                      "exact",   "雪楠（二维码）"),
    (10, "未知渠道",                            "exact",   "未匹配到渠道"),
]


def task5_stats(spreadsheet_token: str):
    print("\n[任务5] 开始统计...")

    # 读取全部学员名单
    t_sheet = find_sheet_by_name(spreadsheet_token, "全部学员名单")
    t_rows = read_sheet_values(spreadsheet_token, t_sheet["sheet_id"])
    t_data = t_rows[1:]
    print(f"全部学员名单共 {len(t_data)} 行")

    # 读取 AI BOSS
    b_sheet = find_sheet_by_name(spreadsheet_token, "AI BOSS")
    b_rows = read_sheet_values(spreadsheet_token, b_sheet["sheet_id"])
    b_data = b_rows[1:]
    print(f"AI BOSS 共 {len(b_data)} 行")

    # 预处理全部学员名单
    t_periods = []
    t_is_add = []
    t_join_time = []
    for row in t_data:
        t_periods.append(str(row[T_COL_PERIOD]).strip() if T_COL_PERIOD < len(row) and row[T_COL_PERIOD] else "")
        t_is_add.append(str(row[T_COL_IS_ADD]).strip() if T_COL_IS_ADD < len(row) and row[T_COL_IS_ADD] else "")
        t_join_time.append(row[T_COL_JOIN_TIME] if T_COL_JOIN_TIME < len(row) and row[T_COL_JOIN_TIME] else None)

    # 预处理 AI BOSS（去重）期名称
    b_periods = []
    for row in b_data:
        b_periods.append(str(row[B_COL_PERIOD]).strip() if B_COL_PERIOD < len(row) and row[B_COL_PERIOD] else "")

    # 计算每个渠道的指标
    results = {}  # 飞书行号 -> (C分配, D添加, E添加率, F进群, G进群率)

    for feishu_row, label, mode, keyword in CHANNEL_CONFIG:
        # C: 导流方分配人数
        if mode == "exact":
            # 精确匹配全部学员名单 J 列
            c_count = sum(1 for p in t_periods if p == keyword)
        else:
            # 关键词匹配 AI BOSS B 列
            c_count = sum(1 for p in b_periods if keyword in p)

        # 匹配函数：exact=精确匹配，keyword=包含且排除二维码渠道
        if mode == "exact":
            def match(p): return p == keyword
        else:
            # keyword 模式：J 列包含关键词，且不是二维码精确渠道（避免"芳群"匹配到"芳群（二维码）"）
            exact_channels = {"芳群（二维码）", "雪楠（二维码）", "未匹配到渠道"}
            def match(p, kw=keyword): return kw in p and p not in exact_channels

        # D: 公开课助教添加人数
        d_count = sum(
            1 for i, p in enumerate(t_periods)
            if match(p) and t_is_add[i] == "是"
        )

        # E: 添加率
        e_val = f"{d_count/c_count*100:.1f}%" if c_count > 0 else "/"

        # F: 进群人数（进群时间非空）
        f_count = sum(
            1 for i, p in enumerate(t_periods)
            if match(p) and t_join_time[i]
        )

        # G: 进群率
        g_val = f"{f_count/c_count*100:.1f}%" if c_count > 0 else "/"

        results[feishu_row] = (c_count, d_count, e_val, f_count, g_val)
        print(f"  行{feishu_row} [{label}]: 分配={c_count}, 添加={d_count}({e_val}), 进群={f_count}({g_val})")

    # 合计行（第11行）
    total_c = sum(v[0] for v in results.values())
    total_d = sum(v[1] for v in results.values())
    total_e = f"{total_d/total_c*100:.1f}%" if total_c > 0 else "/"
    total_f = sum(v[3] for v in results.values())
    total_g = f"{total_f/total_c*100:.1f}%" if total_c > 0 else "/"
    results[11] = (total_c, total_d, total_e, total_f, total_g)
    print(f"  行11 [合计]: 分配={total_c}, 添加={total_d}({total_e}), 进群={total_f}({total_g})")

    # 写入数据汇总
    s_sheet = find_sheet_by_name(spreadsheet_token, "数据汇总")
    s_sid = s_sheet["sheet_id"]

    for feishu_row, (c, d, e, f, g) in results.items():
        write_sheet_values(spreadsheet_token, s_sid,
                           f"C{feishu_row}:G{feishu_row}",
                           [[c, d, e, f, g]])

    print(f"\n[任务5] 完成！已写入数据汇总 C-G 列。")
