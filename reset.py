"""各任务历史数据清除"""

from feishu_client import find_sheet_by_name, read_sheet_values, write_sheet_values
from task3_dedup_merge import rename_sheet, col_letter, set_row_background, STATUS_DISCARD

WHITE = "#FFFFFF"


def _empty_col(token, sid, col, n_rows):
    """清空某列数据行（从第2行起）"""
    if n_rows > 0:
        write_sheet_values(token, sid, f"{col}2:{col}{n_rows + 1}",
                           [[""] for _ in range(n_rows)])


# ── 各任务重置函数 ────────────────────────────────────────────

def reset_tasks_1_2(token):
    """清空群成员名单 N 列（任务1、2 共用）"""
    print("  清空群成员名单 N 列...")
    sheet = find_sheet_by_name(token, "群成员名单")
    sid = sheet["sheet_id"]
    rows = read_sheet_values(token, sid)
    _empty_col(token, sid, "N", len(rows) - 1)


def reset_task3(token):
    """清空任务3写入：O列(群成员名单)、AV列(AI BOSS)、灰色背景、全部学员名单数据"""
    # 群成员名单：先恢复灰色行背景，再清 O 列
    print("  清空群成员名单 O 列 + 恢复背景色...")
    m_sheet = find_sheet_by_name(token, "群成员名单")
    m_sid = m_sheet["sheet_id"]
    m_rows = read_sheet_values(token, m_sid)
    m_data = m_rows[1:]
    for i, row in enumerate(m_data):
        val = str(row[14]).strip() if 14 < len(row) and row[14] else ""
        if val == STATUS_DISCARD:
            set_row_background(token, m_sid, i + 2, WHITE)
    _empty_col(token, m_sid, "O", len(m_data))

    # AI BOSS：先恢复灰色行背景，再清 AV 列
    print("  清空 AI BOSS AV 列 + 恢复背景色...")
    b_sheet = find_sheet_by_name(token, "AI BOSS")
    b_sid = b_sheet["sheet_id"]
    b_rows = read_sheet_values(token, b_sid)
    b_data = b_rows[1:]
    for i, row in enumerate(b_data):
        val = str(row[47]).strip() if 47 < len(row) and row[47] else ""
        if val == STATUS_DISCARD:
            set_row_background(token, b_sid, i + 2, WHITE)
    _empty_col(token, b_sid, "AV", len(b_data))

    # 全部学员名单：清空数据行
    print("  清空全部学员名单数据行...")
    t_sheet = find_sheet_by_name(token, "全部学员名单")
    t_sid = t_sheet["sheet_id"]
    t_rows = read_sheet_values(token, t_sid)
    num_cols = len(t_rows[0]) if t_rows else 22
    n_data = len(t_rows) - 1
    if n_data > 0:
        write_sheet_values(token, t_sid,
                           f"A2:{col_letter(num_cols - 1)}{n_data + 1}",
                           [[""] * num_cols for _ in range(n_data)])


def reset_task4(token):
    """清空全部学员名单 H、I 列"""
    print("  清空全部学员名单 H、I 列...")
    try:
        t_sheet = find_sheet_by_name(token, "全部学员名单")
        t_sid = t_sheet["sheet_id"]
        t_rows = read_sheet_values(token, t_sid)
        n = len(t_rows) - 1
        if n > 0:
            write_sheet_values(token, t_sid, f"H2:I{n + 1}",
                               [["", ""] for _ in range(n)])
    except Exception:
        pass


def reset_task5(token):
    """清空数据汇总 C-G 列第 4-11 行"""
    print("  清空数据汇总 C-G 列...")
    s_sheet = find_sheet_by_name(token, "数据汇总")
    s_sid = s_sheet["sheet_id"]
    for row in list(range(4, 11)) + [11]:
        write_sheet_values(token, s_sid, f"C{row}:G{row}", [["", "", "", "", ""]])


def reset_task6(token):
    """清空数据汇总 M-X 列，清空直播表回填的期名称列"""
    print("  清空数据汇总 M-X 列...")
    s_sheet = find_sheet_by_name(token, "数据汇总")
    s_sid = s_sheet["sheet_id"]
    for row in list(range(4, 11)) + [11]:
        write_sheet_values(token, s_sid, f"M{row}:X{row}", [[""] * 12])

    for live_name in ["直播1", "直播2", "直播3"]:
        try:
            l_sheet = find_sheet_by_name(token, live_name)
            l_sid = l_sheet["sheet_id"]
            l_rows = read_sheet_values(token, l_sid)
            if not l_rows:
                continue
            l_header = l_rows[0]
            last_col = len(l_header)
            while last_col > 0 and not l_header[last_col - 1]:
                last_col -= 1
            n_data = len(l_rows) - 1
            if n_data > 0:
                lc = col_letter(last_col)
                print(f"  清空{live_name}期名称列（{lc}列）...")
                write_sheet_values(token, l_sid,
                                   f"{lc}2:{lc}{n_data + 1}",
                                   [[""] for _ in range(n_data)])
        except Exception:
            pass


def reset_task7(token):
    """清空数据汇总 H-J、L 列，清空订单详情回填列"""
    print("  清空数据汇总 H-J、L 列...")
    s_sheet = find_sheet_by_name(token, "数据汇总")
    s_sid = s_sheet["sheet_id"]
    for row in list(range(4, 11)) + [11]:
        write_sheet_values(token, s_sid, f"H{row}:J{row}", [["", "", ""]])
        write_sheet_values(token, s_sid, f"L{row}:L{row}", [[""]])

    try:
        o_sheet = find_sheet_by_name(token, "订单详情")
        o_sid = o_sheet["sheet_id"]
        o_rows = read_sheet_values(token, o_sid)
        if o_rows:
            o_header = o_rows[0]
            last_col = len(o_header)
            while last_col > 0 and not o_header[last_col - 1]:
                last_col -= 1
            n_data = len(o_rows) - 1
            if n_data > 0:
                lc = col_letter(last_col)
                print(f"  清空订单详情期名称列（{lc}列）...")
                write_sheet_values(token, o_sid,
                                   f"{lc}2:{lc}{n_data + 1}",
                                   [[""] for _ in range(n_data)])
    except Exception:
        pass


# ── 重置入口 ─────────────────────────────────────────────────

# 每个起始任务号对应需要执行的重置函数（按顺序）
_RESET_STEPS = [
    (1, reset_tasks_1_2),   # 任务1、2 都写 N 列
    (3, reset_task3),
    (4, reset_task4),
    (5, reset_task5),
    (6, reset_task6),
    (7, reset_task7),
]


def reset_from_task(token: str, start: int):
    """清空 start 及后续任务的历史数据"""
    # 任务2 也依赖 N 列为空，等同于从任务1开始清
    effective_start = 1 if start == 2 else start
    print(f"\n[重置] 清空任务 {start}-7 的历史数据...")
    for task_no, func in _RESET_STEPS:
        if task_no >= effective_start:
            func(token)
    print("[重置] 完成！\n")
