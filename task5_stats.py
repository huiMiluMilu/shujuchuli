"""
任务5：导流数据全口径统计
- 计算数据汇总表 C/D/E/F/G 列（写入 COUNTIF 公式，便于核查）
"""

from feishu_client import (
    parse_feishu_url, find_sheet_by_name,
    read_sheet_values, write_sheet_values, write_formula_values
)

# 全部学员名单列索引
T_COL_JOIN_TIME = 4   # E 进群时间
T_COL_IS_ADD = 7      # H 是否添加
T_COL_PERIOD = 9      # J 期名称

# AI BOSS（去重）列索引
B_COL_PERIOD = 1      # B 期名称

# keyword 模式需要排除的精确渠道（与 task6 保持一致）
_EXCL_CHANNELS = ["芳群（二维码）", "雪楠（二维码）", "未匹配到渠道"]

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


def _build_formulas(feishu_row: int, mode: str, keyword: str) -> list:
    """
    返回 [C, D, E, F, G] 五个单元格的公式字符串。
    C = 分配人数, D = 添加人数, E = 添加率, F = 进群人数, G = 进群率
    """
    t = "'全部学员名单'"
    r = feishu_row

    if mode == "keyword":
        # C：AI BOSS B 列包含关键词
        c = f'=COUNTIF(\'AI BOSS\'!B:B,"*{keyword}*")'
        # keyword 模式排除精确渠道
        excl = "".join(f',{t}!J:J,"<>{ch}"' for ch in _EXCL_CHANNELS)
        d = f'=COUNTIFS({t}!J:J,"*{keyword}*"{excl},{t}!H:H,"是")'
        f_ = f'=COUNTIFS({t}!J:J,"*{keyword}*"{excl},{t}!E:E,"<>")'
    else:  # exact
        c = f'=COUNTIF({t}!J:J,"{keyword}")'
        d = f'=COUNTIFS({t}!J:J,"{keyword}",{t}!H:H,"是")'
        f_ = f'=COUNTIFS({t}!J:J,"{keyword}",{t}!E:E,"<>")'

    e = f'=IF(C{r}>0,TEXT(D{r}/C{r},"0.0%"),"/")'
    g = f'=IF(C{r}>0,TEXT(F{r}/C{r},"0.0%"),"/")'
    return [c, d, e, f_, g]


def task5_stats(spreadsheet_token: str):
    print("\n[任务5] 开始统计...")

    # 读取全部学员名单（用于打印日志验证）
    t_sheet = find_sheet_by_name(spreadsheet_token, "全部学员名单")
    t_rows = read_sheet_values(spreadsheet_token, t_sheet["sheet_id"])
    t_data = t_rows[1:]
    print(f"全部学员名单共 {len(t_data)} 行")

    # 读取 AI BOSS
    b_sheet = find_sheet_by_name(spreadsheet_token, "AI BOSS")
    b_rows = read_sheet_values(spreadsheet_token, b_sheet["sheet_id"])
    b_data = b_rows[1:]
    print(f"AI BOSS 共 {len(b_data)} 行")

    # 预处理（仅用于打印日志）
    exact_channels = set(_EXCL_CHANNELS)
    t_periods = [str(r[T_COL_PERIOD]).strip() if T_COL_PERIOD < len(r) and r[T_COL_PERIOD] else "" for r in t_data]
    t_is_add  = [str(r[T_COL_IS_ADD]).strip()  if T_COL_IS_ADD  < len(r) and r[T_COL_IS_ADD]  else "" for r in t_data]
    t_join    = [r[T_COL_JOIN_TIME] if T_COL_JOIN_TIME < len(r) and r[T_COL_JOIN_TIME] else None for r in t_data]
    b_periods = [str(r[B_COL_PERIOD]).strip() if B_COL_PERIOD < len(r) and r[B_COL_PERIOD] else "" for r in b_data]

    # 写入数据汇总
    s_sheet = find_sheet_by_name(spreadsheet_token, "数据汇总")
    s_sid = s_sheet["sheet_id"]

    total_c = total_d = total_f = 0
    for feishu_row, label, mode, keyword in CHANNEL_CONFIG:
        # 打印日志（Python 计算，用于核查公式结果）
        if mode == "exact":
            c = sum(1 for p in t_periods if p == keyword)
            def match(p): return p == keyword
        else:
            c = sum(1 for p in b_periods if keyword in p)
            def match(p, kw=keyword): return kw in p and p not in exact_channels
        d = sum(1 for i, p in enumerate(t_periods) if match(p) and t_is_add[i] == "是")
        f_ = sum(1 for i, p in enumerate(t_periods) if match(p) and t_join[i])
        e_val = f"{d/c*100:.1f}%" if c > 0 else "/"
        g_val = f"{f_/c*100:.1f}%" if c > 0 else "/"
        print(f"  行{feishu_row} [{label}]: 分配={c}, 添加={d}({e_val}), 进群={f_}({g_val})")
        total_c += c; total_d += d; total_f += f_

        # 写入公式
        formulas = _build_formulas(feishu_row, mode, keyword)
        write_formula_values(spreadsheet_token, s_sid,
                             f"C{feishu_row}:G{feishu_row}", [formulas])

    total_e = f"{total_d/total_c*100:.1f}%" if total_c > 0 else "/"
    total_g = f"{total_f/total_c*100:.1f}%" if total_c > 0 else "/"
    print(f"  行11 [合计]: 分配={total_c}, 添加={total_d}({total_e}), 进群={total_f}({total_g})")

    # 合计行用 SUM 公式
    total_formulas = [
        "=SUM(C4:C10)",
        "=SUM(D4:D10)",
        '=IF(C11>0,TEXT(D11/C11,"0.0%"),"/")',
        "=SUM(F4:F10)",
        '=IF(C11>0,TEXT(F11/C11,"0.0%"),"/")',
    ]
    write_formula_values(spreadsheet_token, s_sid, "C11:G11", [total_formulas])

    print(f"\n[任务5] 完成！已写入数据汇总 C-G 列（公式）。")
