"""
任务6：直播转化数据多维统计
- 预处理：以直播表 unionid 匹配全部学员名单，回填期名称到最后一列
- 按渠道统计到播人数、有效观看人数，写入数据汇总
"""

from feishu_client import (
    find_sheet_by_name,
    read_sheet_values, write_sheet_values
)
from task3_dedup_merge import col_letter

# 直播表列索引（默认值，运行时动态检测）
L_COL_UNIONID = 6       # G 列：直播间 unionid
L_COL_DURATION = 7      # H 列：停留时长(分钟)
L_COL_PERIOD = 11       # L 列：期名称（原表已有）

# 全部学员名单列索引
T_COL_UNIONID = 1       # B
T_COL_PERIOD = 9        # J

# 数据汇总各直播区域起始列（0-based）
LIVE_CONFIG = [
    ("直播1", 12),  # M列起
    ("直播2", 18),  # S列起
    ("直播3", 24),  # Y列起
]

# 渠道行配置（行号, 模式, 关键词）
CHANNEL_ROWS = [
    (4,  "keyword", "芳群"),
    (5,  "keyword", "雪楠"),
    (6,  "keyword", "坚平"),
    (7,  "keyword", "老师"),
    (8,  "exact",   "芳群（二维码）"),
    (9,  "exact",   "雪楠（二维码）"),
    (10, "exact",   {"未匹配到渠道", "未知渠道"}),  # 两种值都归入未知渠道
]
EXACT_CHANNELS = {"芳群（二维码）", "雪楠（二维码）", "未匹配到渠道"}

VALID_WATCH_MINUTES = 30  # 有效观看阈值


def match_period(period: str, mode: str, keyword) -> bool:
    if mode == "exact":
        if isinstance(keyword, (set, list, tuple)):
            return period in keyword
        return period == keyword
    else:
        return keyword in period and period not in EXACT_CHANNELS


def _is_number(v) -> bool:
    try:
        float(str(v))
        return True
    except (ValueError, TypeError):
        return False


def _detect_uid_col(header: list, data: list) -> int:
    """动态找实际含 unionid（o7...）的列，优先非「风变」列"""
    counts = {}
    for row in data:
        for i, v in enumerate(row):
            s = str(v).strip() if v else ""
            if s.startswith('o7') and len(s) > 15:
                counts[i] = counts.get(i, 0) + 1
    if not counts:
        return L_COL_UNIONID
    # 优先选非「风变」列
    non_fv = {i: c for i, c in counts.items()
              if i >= len(header) or '风变' not in str(header[i] or '')}
    pool = non_fv if non_fv else counts
    return max(pool, key=lambda i: (pool[i], -i))


def _detect_dur_col(header: list, data: list, uid_col: int) -> int:
    """动态找实际含停留时长（数值）的列"""
    # 先按 header 名称找
    for i, h in enumerate(header):
        if h and '停留时长' in str(h):
            nums = sum(1 for row in data
                       if i < len(row) and row[i] is not None and _is_number(row[i]))
            if nums > len(data) // 3:
                return i
    # 找紧跟 uid_col 之后的数值列
    for offset in range(1, 5):
        col = uid_col + offset
        nums = sum(1 for row in data
                   if col < len(row) and row[col] is not None and _is_number(row[col]))
        if nums > len(data) // 3:
            return col
    return L_COL_DURATION


def task6_live_stats(spreadsheet_token: str):
    print("\n[任务6] 开始处理...")

    # 读取全部学员名单，构建 uid -> 期名称
    t_sheet = find_sheet_by_name(spreadsheet_token, "全部学员名单")
    t_rows = read_sheet_values(spreadsheet_token, t_sheet["sheet_id"])
    uid_to_period = {}
    for row in t_rows[1:]:
        uid = str(row[T_COL_UNIONID]).strip() if T_COL_UNIONID < len(row) and row[T_COL_UNIONID] else ""
        period = str(row[T_COL_PERIOD]).strip() if T_COL_PERIOD < len(row) and row[T_COL_PERIOD] else ""
        if uid:
            uid_to_period[uid] = period
    print(f"全部学员名单 uid 映射：{len(uid_to_period)} 条")

    # 读取数据汇总 D 列（公开课助教添加人数），用作分母
    s_sheet = find_sheet_by_name(spreadsheet_token, "数据汇总")
    s_sid = s_sheet["sheet_id"]
    s_rows = read_sheet_values(spreadsheet_token, s_sid, "A1:AE12")
    d_col = {}
    for i, row in enumerate(s_rows):
        feishu_row = i + 1
        val = row[3] if len(row) > 3 else None
        try:
            d_col[feishu_row] = int(val) if val else 0
        except (ValueError, TypeError):
            d_col[feishu_row] = 0

    # 处理每个直播表
    for live_name, col_start in LIVE_CONFIG:
        print(f"\n--- 处理「{live_name}」---")

        l_sheet = find_sheet_by_name(spreadsheet_token, live_name)
        l_sid = l_sheet["sheet_id"]
        l_rows = read_sheet_values(spreadsheet_token, l_sid)

        l_header = l_rows[0] if l_rows else []
        l_data = [r for r in l_rows[1:] if any(c for c in r)] if len(l_rows) > 1 else []
        print(f"  共 {len(l_data)} 行数据")

        if not l_data:
            print(f"  {live_name} 无数据，填充 0")
            _write_zeros(spreadsheet_token, s_sid, col_start)
            continue

        # 动态检测 uid 列和停留时长列
        uid_col = _detect_uid_col(l_header, l_data)
        dur_col = _detect_dur_col(l_header, l_data, uid_col)
        uid_header = l_header[uid_col] if uid_col < len(l_header) else '?'
        dur_header = l_header[dur_col] if dur_col < len(l_header) else '?'
        print(f"  uid列={uid_col}({uid_header}), 时长列={dur_col}({dur_header})")

        # 步骤1：预处理回填期名称到最后一列（新增列）
        last_col = len(l_header)
        while last_col > 0 and not l_header[last_col - 1]:
            last_col -= 1
        new_col_letter = col_letter(last_col)
        print(f"  期名称回填到第 {last_col+1} 列（{new_col_letter}列）")

        period_values = []
        for row in l_data:
            uid = str(row[uid_col]).strip() if uid_col < len(row) and row[uid_col] else ""
            # 原表期名称：跳过纯数字（这是期ID而非期名称）
            raw = row[L_COL_PERIOD] if L_COL_PERIOD < len(row) else None
            orig_period = str(raw).strip() if raw and not _is_number(raw) else ""
            if orig_period:
                period_values.append([orig_period])
            elif uid in uid_to_period:
                period_values.append([uid_to_period[uid]])
            else:
                period_values.append([""])

        write_sheet_values(spreadsheet_token, l_sid,
                           f"{new_col_letter}2:{new_col_letter}{len(l_data)+1}",
                           period_values)
        print(f"  期名称回填完成")

        # 步骤2：按渠道统计到播人数和有效观看人数
        row_data = []
        for i, row in enumerate(l_data):
            period = period_values[i][0]
            try:
                duration = float(row[dur_col]) if dur_col < len(row) and row[dur_col] else 0
            except (ValueError, TypeError):
                duration = 0
            row_data.append((period, duration))

        result_rows = {}
        total_arrive = 0
        total_valid = 0

        for feishu_row, mode, keyword in CHANNEL_ROWS:
            arrive = sum(1 for p, _ in row_data if match_period(p, mode, keyword))
            valid = sum(1 for p, d in row_data if match_period(p, mode, keyword) and d > VALID_WATCH_MINUTES)
            d_base = d_col.get(feishu_row, 0)
            arrive_rate = f"{arrive/d_base*100:.1f}%" if d_base > 0 else "/"
            valid_rate = f"{valid/d_base*100:.1f}%" if d_base > 0 else "/"
            result_rows[feishu_row] = [arrive, arrive_rate, valid, valid_rate, 0, 0]
            total_arrive += arrive
            total_valid += valid
            print(f"  行{feishu_row}: 到播={arrive}({arrive_rate}), 有效观看={valid}({valid_rate})")

        d_total = d_col.get(11, 0)
        result_rows[11] = [
            total_arrive,
            f"{total_arrive/d_total*100:.1f}%" if d_total > 0 else "/",
            total_valid,
            f"{total_valid/d_total*100:.1f}%" if d_total > 0 else "/",
            0, 0
        ]
        print(f"  合计: 到播={total_arrive}, 有效观看={total_valid}")

        for feishu_row, vals in result_rows.items():
            write_sheet_values(spreadsheet_token, s_sid,
                               f"{col_letter(col_start)}{feishu_row}:{col_letter(col_start+5)}{feishu_row}",
                               [vals])

        print(f"  已写入数据汇总 {col_letter(col_start)}-{col_letter(col_start+5)} 列")

    print(f"\n[任务6] 完成！")


def _write_zeros(spreadsheet_token: str, s_sid: str, col_start: int):
    """直播无数据时整块填0"""
    for feishu_row in list(range(4, 11)) + [11]:
        write_sheet_values(spreadsheet_token, s_sid,
                           f"{col_letter(col_start)}{feishu_row}:{col_letter(col_start+5)}{feishu_row}",
                           [[0, 0, 0, 0, 0, 0]])
