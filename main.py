"""主入口：数据处理 skill（交互模式）"""

from feishu_client import parse_feishu_url, get_sheets_meta
from task1_channel_mark import task1_mark_channel
from task2_invite_match import task2_invite_match
from task3_dedup_merge import task3_dedup_and_merge
from task4_final_list import task4_final_member_list
from task5_stats import task5_stats
from task6_live_stats import task6_live_stats
from task7_conversion import task7_conversion_stats


def main():
    print("=== 飞书数据处理 Skill (交互模式) ===\n")

    url = input("请粘贴飞书电子表格链接：> ").strip()
    spreadsheet_token, _ = parse_feishu_url(url)
    print(f"解析成功，spreadsheet_token = {spreadsheet_token}")

    sheets = get_sheets_meta(spreadsheet_token)
    print("\n该表格包含以下子表格：")
    for i, s in enumerate(sheets, 1):
        print(f"  {i}. {s['title']}")

    while True:
        print("\n" + "=" * 40)
        print("请选择要执行的任务（输入 q 退出）：")
        print("  1. 进群渠道归属标记 (Task 1)")
        print("  2. 基于成员邀请匹配 (Task 2)")
        print("  3. 跨表去重与全量汇总 (Task 3)")
        print("  4. 助教好友关联 (Task 4)")
        print("  5. 导流漏斗统计 (Task 5)")
        print("  6. 直播转化统计 (Task 6)")
        print("  7. 订单转化统计 (Task 7)")
        print("  q. 退出程序")

        choice = input("> ").strip().lower()

        if choice == "q":
            print("退出程序，拜拜！")
            break
        elif choice == "1":
            task1_mark_channel(spreadsheet_token)
        elif choice == "2":
            task2_invite_match(spreadsheet_token)
        elif choice == "3":
            task3_dedup_and_merge(spreadsheet_token)
        elif choice == "4":
            task4_final_member_list(spreadsheet_token)
        elif choice == "5":
            task5_stats(spreadsheet_token)
        elif choice == "6":
            task6_live_stats(spreadsheet_token)
        elif choice == "7":
            task7_conversion_stats(spreadsheet_token)
        else:
            print("输入无效，请重新选择。")


if __name__ == "__main__":
    main()
