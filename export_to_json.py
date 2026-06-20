import sqlite3
import json
import os
from datetime import datetime

# 数据库路径
DB_PATH = "data/stock_analysis.db" 
# 静态页面所在的产物目录
OUTPUT_DIR = "public"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "daily_report.json")

def get_trend_score(report) -> int:
    dashboard = report.get("dashboard") or {}
    data_perspective = dashboard.get("data_perspective") or {}
    trend_status = data_perspective.get("trend_status") or {}
    
    # In JS: Number(item?.dashboard?.data_perspective?.trend_status?.trend_score ?? item?.sentiment_score ?? 0)
    score = trend_status.get("trend_score")
    if score is None or score == "N/A":
        score = report.get("sentiment_score")
    if score is None:
        score = 0
    try:
        return int(float(score))
    except (ValueError, TypeError):
        return 0

def get_sentiment_score(report) -> int:
    dashboard = report.get("dashboard") or {}
    # In JS: Number(item?.dashboard?.sentiment_score ?? item?.sentiment_score ?? 50)
    score = dashboard.get("sentiment_score")
    if score is None or score == "N/A":
        score = report.get("sentiment_score")
    if score is None:
        score = 50
    try:
        return int(float(score))
    except (ValueError, TypeError):
        return 50

def calculate_overall_score(trend: int, sentiment: int) -> int:
    # 🎯 严格遵循前端“顺势交易”的动态加权计算公式
    if trend <= 30:
        val = trend * 0.8 + sentiment * 0.2
    elif trend >= 60:
        val = trend * 0.45 + sentiment * 0.55
    else:
        val = trend * 0.55 + sentiment * 0.45
    return int(val + 0.5)

def update_history_file(new_reports, update_time_str):
    history_file = os.path.join(OUTPUT_DIR, "score_history.json")
    
    # 1. 读取历史数据，不存在或读取失败时初始化为空白结构
    history = {}
    if os.path.exists(history_file):
        try:
            with open(history_file, "r", encoding="utf-8") as f:
                hist_data = json.load(f)
                history = hist_data.get("history") or {}
        except Exception as e:
            print(f"读取历史数据文件失败，将以空白结构继续: {e}")
        
    # 2. 增量更新新导出的记录
    for r in new_reports:
        code = r.get("code")
        if not code or code == "MARKET" or not r.get("dashboard"):
            continue
            
        trend = get_trend_score(r)
        sentiment = get_sentiment_score(r)
        overall = calculate_overall_score(trend, sentiment)
        
        # 优先使用报告里的时间，如果不存在则使用 update_time_str 作为 fallback
        report_time = r.get("created_at") or update_time_str
        date_str = report_time.split(" ")[0].split("T")[0]
        
        if code not in history:
            history[code] = []
            
        # 查找该日期是否已存在
        existing_idx = -1
        for idx, item in enumerate(history[code]):
            if item.get("date") == date_str:
                existing_idx = idx
                break
                
        entry = {
            "date": date_str,
            "trend_score": trend,
            "sentiment_score": sentiment,
            "overall_score": overall
        }
        
        if existing_idx >= 0:
            history[code][existing_idx] = entry
        else:
            history[code].append(entry)
            
    # 3. 排序并裁剪到最近 60 天
    for code in list(history.keys()):
        valid_items = [item for item in history[code] if item.get("date")]
        sorted_list = sorted(valid_items, key=lambda x: x["date"])
        history[code] = sorted_list[-60:]
        
    # 4. 写入文件
    final_json = {
        "update_time": update_time_str,
        "history": history
    }
    
    try:
        with open(history_file, "w", encoding="utf-8") as f:
            json.dump(final_json, f, ensure_ascii=False, indent=2)
        print(f"成功将历史得分记录同步至 {history_file}，共记录了 {len(history)} 只股票的历史数据")
    except Exception as e:
        print(f"写入历史记录文件失败: {e}")


def export_data():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    if not os.path.exists(DB_PATH):
        print(f"找不到数据库文件: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        # 通过子查询找到 7 天内所有项目的最新日期（含大盘与股票），再关联原表获取完整记录
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

        print(f"成功提取 {len(data)} 条最新记录 (含大盘与个股) 至 {OUTPUT_FILE}")

        # 同步更新历史得分文件
        try:
            update_history_file(data, final_json["update_time"])
        except Exception as eh:
            print(f"同步历史得分文件失败: {eh}")


    except sqlite3.OperationalError as e:
        print(f"数据库查询错误: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    export_data()