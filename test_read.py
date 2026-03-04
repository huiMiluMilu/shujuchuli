"""快速测试：读取指定表格数据并展示关键列"""
from feishu_client import parse_feishu_url, find_sheet_by_name, read_sheet_values

URL = "https://forchangesz.feishu.cn/sheets/RuTVsTCE9hc2Ept99hWcRQK9nte?sheet=io0jDe"
SHEET_NAME = "群成员名单"

COL_GROUP_NAME = 0    # A 列：群名
COL_JOIN_METHOD = 8   # I 列：进群方式
COL_PERIOD_NAME = 13  # N 列：期名称

def main():
    token, sheet_id_from_url = parse_feishu_url(URL)
    print(f"spreadsheet_token: {token}")
    print(f"URL 中的 sheet_id: {sheet_id_from_url}")

    try:
        sheet = find_sheet_by_name(token, SHEET_NAME)
        print(f"\n找到子表格「{SHEET_NAME}」，sheet_id = {sheet['sheet_id']}")
    except ValueError as e:
        print(f"\n{e}")
        return

    sheet_id = sheet["sheet_id"]
    rows = read_sheet_values(token, sheet_id)
    print(f"共读取 {len(rows)} 行（含表头）")

    if not rows:
        print("表格为空")
        return

    print(f"\n表头（第1行）：{rows[0]}")

    # 扫描二维码进群的行
    trigger_rows = []
    for i, row in enumerate(rows[1:], start=2):
        join_method = row[COL_JOIN_METHOD] if COL_JOIN_METHOD < len(row) else ""
        if str(join_method).strip() == "通过扫描群二维码入群":
            group_name = row[COL_GROUP_NAME] if COL_GROUP_NAME < len(row) else ""
            period_name = row[COL_PERIOD_NAME] if COL_PERIOD_NAME < len(row) else ""
            trigger_rows.append((i, group_name, period_name))

    print(f"\n触发「通过扫描群二维码入群」的行数：{len(trigger_rows)}")
    if trigger_rows:
        print("前10条预览（行号, 群名称, 当前期名称）：")
        for r in trigger_rows[:10]:
            print(f"  行{r[0]}: 群={r[1]!r}, 当前期名称={r[2]!r}")

        unique_groups = list(dict.fromkeys(r[1] for r in trigger_rows))
        print(f"\n涉及的不重复群名：{unique_groups}")

if __name__ == "__main__":
    main()
