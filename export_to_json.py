import sqlite3
import json
import os
from datetime import datetime

# 数据库路径
DB_PATH = "data/stock_analysis.db" 
# 静态页面所在的产物目录
OUTPUT_DIR = "public"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "daily_report.json")

def export_data():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    if not os.path.exists(DB_PATH):
        print(f"❌ 找不到数据库文件: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        # 【核心修改点】：通过子查询找到 7 天内每只股票的最新日期，再关联原表获取完整记录
        cursor.execute("""
            SELECT a.* FROM analysis_history a
            INNER JOIN (
                SELECT code, MAX(created_at) as max_date
                FROM analysis_history
                WHERE created_at >= date('now', '-7 days', 'localtime')
                GROUP BY code
            ) b ON a.code = b.code AND a.created_at = b.max_date
            ORDER BY a.created_at DESC
        """)
        rows = cursor.fetchall()

        data = []
        for row in rows:
            row_dict = dict(row)

            # 1. 核心解析：反序列化 raw_result，提取完整的分析 dashboard
            if 'raw_result' in row_dict and row_dict['raw_result']:
                try:
                    raw_parsed = json.loads(row_dict['raw_result'])
                    row_dict.update(raw_parsed)
                except json.JSONDecodeError:
                    pass

            # 2. 补充解析：从 context_snapshot 中提取实时行情 (涨跌幅、现价等)
            if 'context_snapshot' in row_dict and row_dict['context_snapshot']:
                try:
                    context_parsed = json.loads(row_dict['context_snapshot'])
                    if 'realtime_quote' in context_parsed:
                        row_dict['realtime_quote'] = context_parsed['realtime_quote']
                except json.JSONDecodeError:
                    pass

            # 3. 瘦身：移除不需要暴露给前端的冗余纯文本字段
            row_dict.pop('raw_result', None)
            row_dict.pop('context_snapshot', None)
            row_dict.pop('news_content', None) 

            data.append(row_dict)

        # 组装最终结构
        final_json = {
            "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_count": len(data),
            "reports": data
        }

        # 写入文件
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(final_json, f, ensure_ascii=False, indent=2)

        print(f"✅ 成功提取 {len(data)} 只股票的最新记录 (7天内) 至 {OUTPUT_FILE}")

    except sqlite3.OperationalError as e:
        print(f"❌ 数据库查询错误: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    export_data()