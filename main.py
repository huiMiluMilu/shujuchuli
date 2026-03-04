"""主入口：数据处理 skill"""

import sys
from feishu_client import parse_feishu_url, get_sheets_meta
from reset import reset_from_task
from task1_channel_mark import task1_mark_channel
from task2_invite_match import task2_invite_match
from task3_dedup_merge import task3_dedup_and_merge
from task4_final_list import task4_final_member_list
from task5_stats import task5_stats
from task6_live_stats import task6_live_stats
from task7_conversion import task7_conversion_stats

TASKS = {
    1: ("进群渠道归属标记",   task1_mark_channel),
    2: ("基于成员邀请匹配",   task2_invite_match),
    3: ("跨表去重与全量汇总", task3_dedup_and_merge),
    4: ("助教好友关联",       task4_final_member_list),
    5: ("导流漏斗统计",       task5_stats),
    6: ("直播转化统计",       task6_live_stats),
    7: ("订单转化统计",       task7_conversion_stats),
}


def run_tasks_from(spreadsheet_token: str, start: int):
    for i in range(start, 8):
        print(f"\n{'=' * 40}")
        print(f">>> 任务 {i}：{TASKS[i][0]}")
        TASKS[i][1](spreadsheet_token)
    print(f"\n{'=' * 40}")
    print("全部任务执行完毕！")


def main():
    print("=== 飞书数据处理 Skill ===\n")

    url = input("请粘贴飞书电子表格链接：> ").strip()
    spreadsheet_token, _ = parse_feishu_url(url)
    print(f"解析成功，spreadsheet_token = {spreadsheet_token}")

    sheets = get_sheets_meta(spreadsheet_token)
    print("\n该表格包含以下子表格：")
    for i, s in enumerate(sheets, 1):
        print(f"  {i}. {s['title']}")

    # 自动执行任务 1-7
    run_tasks_from(spreadsheet_token, 1)

    # 询问是否重跑
    while True:
        print()
        print("任务列表：")
        for num, (name, _) in TASKS.items():
            print(f"  {num}. {name}")
        answer = input("要重新跑任务吗？（输入任务号从该任务开始重跑，直接回车或输入 q 退出）：> ").strip().lower()
        if answer == "q" or answer == "":
            print("退出程序，拜拜！")
            break
        elif answer.isdigit() and 1 <= int(answer) <= 7:
            start = int(answer)
            reset_from_task(spreadsheet_token, start)
            run_tasks_from(spreadsheet_token, start)
        else:
            print("输入无效，请输入 1-7 或 q。")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n[错误] {e}")
        import traceback
        traceback.print_exc()
    finally:
        if sys.platform == "win32":
            input("\n按回车键退出...")
