# 实现逻辑，分析sqlite3文件的tweets表的author_username字段，统计每个作者的推文数量，并按照推文数量降序排序

import sqlite3
import os
import csv
import json
import statistics

# Database path
db_path = 'grok.sqlite3'
output_csv = 'author_tweet_stats.csv'

def analyze_author_tweets():
    """
    分析sqlite3文件的tweets表的author_username字段，
    统计每个作者的推文数量，并按照推文数量降序排序保存到CSV文件。
    
    同时统计整个表的总体数据：
    - is_reply = 0 和 is_reply = 1 的数量
    - is_grok_reply = 0 和 is_grok_reply = 1 的数量
    - is_reply 和 is_grok_reply 的组合统计：(0,0), (0,1), (1,0), (1,1)
    - parent_id 为空和不为空的数量
    """
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at {db_path}")
        return

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 执行SQL查询：统计每个作者的推文数量，按推文数量降序排序
        query = """
        SELECT author_username, COUNT(*) as tweet_count 
        FROM tweets 
        GROUP BY author_username 
        ORDER BY tweet_count DESC
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # 统计整个表的总体数据
        stats_query = """
        SELECT 
            COUNT(*) as total_tweets,
            SUM(CASE WHEN is_reply = 0 THEN 1 ELSE 0 END) as is_reply_0_count,
            SUM(CASE WHEN is_reply = 1 THEN 1 ELSE 0 END) as is_reply_1_count,
            SUM(CASE WHEN is_grok_reply = 0 THEN 1 ELSE 0 END) as is_grok_reply_0_count,
            SUM(CASE WHEN is_grok_reply = 1 THEN 1 ELSE 0 END) as is_grok_reply_1_count,
            SUM(CASE WHEN parent_id IS NULL OR parent_id = '' THEN 1 ELSE 0 END) as parent_id_null_count,
            SUM(CASE WHEN parent_id IS NOT NULL AND parent_id != '' THEN 1 ELSE 0 END) as parent_id_not_null_count,
            SUM(CASE WHEN is_reply = 0 AND is_grok_reply = 0 THEN 1 ELSE 0 END) as combo_0_0_count,
            SUM(CASE WHEN is_reply = 0 AND is_grok_reply = 1 THEN 1 ELSE 0 END) as combo_0_1_count,
            SUM(CASE WHEN is_reply = 1 AND is_grok_reply = 0 THEN 1 ELSE 0 END) as combo_1_0_count,
            SUM(CASE WHEN is_reply = 1 AND is_grok_reply = 1 THEN 1 ELSE 0 END) as combo_1_1_count
        FROM tweets
        """
        cursor.execute(stats_query)
        stats = cursor.fetchone()
        
        # 写入CSV文件
        with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            # 写入表头
            writer.writerow(['author_username', 'tweet_count'])
            # 写入数据
            for author_username, tweet_count in results:
                writer.writerow([author_username, tweet_count])
        
        print(f"Successfully saved {len(results)} authors' statistics to {output_csv}")
        print(f"Total authors: {len(results)}")
        if results:
            total_tweets = sum(count for _, count in results)
            print(f"Total tweets: {total_tweets}")
            print(f"Top author: {results[0][0]} with {results[0][1]} tweets")
        
        # 输出总体统计信息
        if stats:
            print(f"\n=== Overall Table Statistics ===")
            print(f"Total tweets in table: {stats[0]}")
            print(f"\nis_reply statistics:")
            print(f"  is_reply = 0: {stats[1]}")
            print(f"  is_reply = 1: {stats[2]}")
            print(f"\nis_grok_reply statistics:")
            print(f"  is_grok_reply = 0: {stats[3]}")
            print(f"  is_grok_reply = 1: {stats[4]}")
            print(f"\nCombined statistics (is_reply, is_grok_reply):")
            print(f"  (0, 0): {stats[7]}")
            print(f"  (0, 1): {stats[8]}")
            print(f"  (1, 0): {stats[9]}")
            print(f"  (1, 1): {stats[10]}")
            print(f"\nparent_id statistics:")
            print(f"  parent_id is NULL or empty: {stats[5]}")
            print(f"  parent_id is NOT NULL: {stats[6]}")
        
        conn.close()

    except sqlite3.Error as e:
        print(f"Database error occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

def analyze_engagement_metrics():
    """
    从tweets表的json字段中提取互动指标并统计分布
    提取的指标：likeCount, viewCount, bookmarkCount, quoteCount, replyCount, retweetCount
    """
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at {db_path}")
        return

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 查询所有推文的json字段
        print("Loading tweets from database...")
        cursor.execute("SELECT json FROM tweets")
        rows = cursor.fetchall()
        
        # 定义要提取的指标
        metrics = ['likeCount', 'viewCount', 'bookmarkCount', 'quoteCount', 'replyCount', 'retweetCount']
        metric_data = {metric: [] for metric in metrics}
        
        # 统计解析成功和失败的数量
        success_count = 0
        error_count = 0
        
        print(f"Processing {len(rows)} tweets...")
        for i, (json_str,) in enumerate(rows):
            if (i + 1) % 10000 == 0:
                print(f"  Processed {i + 1}/{len(rows)} tweets...")
            
            try:
                tweet_data = json.loads(json_str)
                for metric in metrics:
                    value = tweet_data.get(metric, 0)
                    # 确保值是数字类型
                    if value is None:
                        value = 0
                    elif isinstance(value, (int, float)):
                        metric_data[metric].append(int(value))
                    else:
                        metric_data[metric].append(0)
                success_count += 1
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                error_count += 1
                # 如果解析失败，为所有指标添加0
                for metric in metrics:
                    metric_data[metric].append(0)
        
        print(f"\nJSON parsing: {success_count} successful, {error_count} errors")
        
        # 统计每个指标的分布
        print(f"\n=== Engagement Metrics Distribution ===")
        
        for metric in metrics:
            values = metric_data[metric]
            if not values:
                print(f"\n{metric}: No data")
                continue
            
            # 过滤掉0值（用于某些统计）
            non_zero_values = [v for v in values if v > 0]
            
            # 基本统计
            total = len(values)
            zeros = values.count(0)
            non_zeros = len(non_zero_values)
            
            if total > 0:
                print(f"\n{metric}:")
                print(f"  Total tweets: {total:,}")
                print(f"  Zero values: {zeros:,} ({zeros/total*100:.2f}%)")
                print(f"  Non-zero values: {non_zeros:,} ({non_zeros/total*100:.2f}%)")
                
                if non_zero_values:
                    print(f"  Min (non-zero): {min(non_zero_values):,}")
                    print(f"  Max: {max(values):,}")
                    print(f"  Mean (all): {statistics.mean(values):.2f}")
                    print(f"  Mean (non-zero): {statistics.mean(non_zero_values):.2f}")
                    print(f"  Median (all): {statistics.median(values):.2f}")
                    print(f"  Median (non-zero): {statistics.median(non_zero_values):.2f}")
                    
                    # 分位数
                    sorted_values = sorted(values)
                    print(f"  25th percentile: {sorted_values[int(len(sorted_values)*0.25)]:,}")
                    print(f"  75th percentile: {sorted_values[int(len(sorted_values)*0.75)]:,}")
                    print(f"  90th percentile: {sorted_values[int(len(sorted_values)*0.90)]:,}")
                    print(f"  95th percentile: {sorted_values[int(len(sorted_values)*0.95)]:,}")
                    print(f"  99th percentile: {sorted_values[int(len(sorted_values)*0.99)]:,}")
                    
                    # 分桶统计
                    print(f"  Distribution buckets:")
                    buckets = [
                        (0, 0, "= 0"),
                        (1, 10, "1-10"),
                        (11, 100, "11-100"),
                        (101, 1000, "101-1K"),
                        (1001, 10000, "1K-10K"),
                        (10001, 100000, "10K-100K"),
                        (100001, float('inf'), "> 100K")
                    ]
                    
                    for min_val, max_val, label in buckets:
                        if max_val == float('inf'):
                            count = sum(1 for v in values if v > min_val)
                        else:
                            count = sum(1 for v in values if min_val <= v <= max_val)
                        percentage = count / total * 100
                        print(f"    {label:12s}: {count:6,} ({percentage:5.2f}%)")
                else:
                    print(f"  All values are zero")
        
        # 保存详细数据到CSV
        output_metrics_csv = 'engagement_metrics_distribution.csv'
        print(f"\nSaving detailed statistics to {output_metrics_csv}...")
        
        with open(output_metrics_csv, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['metric', 'value'])
            
            for metric in metrics:
                for value in metric_data[metric]:
                    writer.writerow([metric, value])
        
        print(f"Successfully saved engagement metrics to {output_metrics_csv}")
        
        conn.close()

    except sqlite3.Error as e:
        print(f"Database error occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    analyze_author_tweets()
    print("\n" + "="*60)
    analyze_engagement_metrics()
