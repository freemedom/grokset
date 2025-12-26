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
    按照author_username是否为grok进行分类统计
    """
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at {db_path}")
        return

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 查询所有推文的author_username和json字段
        print("Loading tweets from database...")
        cursor.execute("SELECT author_username, json FROM tweets")
        rows = cursor.fetchall()
        
        # 定义要提取的指标
        metrics = ['likeCount', 'viewCount', 'bookmarkCount', 'quoteCount', 'replyCount', 'retweetCount']
        
        # 按作者类型分类存储数据：grok, users, total
        metric_data = {
            'grok': {metric: [] for metric in metrics},
            'users': {metric: [] for metric in metrics},
            'total': {metric: [] for metric in metrics}
        }
        
        # 统计解析成功和失败的数量
        success_count = 0
        error_count = 0
        grok_tweet_count = 0
        user_tweet_count = 0
        
        print(f"Processing {len(rows)} tweets...")
        for i, (author_username, json_str) in enumerate(rows):
            if (i + 1) % 10000 == 0:
                print(f"  Processed {i + 1}/{len(rows)} tweets...")
            
            is_grok = (author_username and author_username.lower() == 'grok')
            
            if is_grok:
                grok_tweet_count += 1
            else:
                user_tweet_count += 1
            
            try:
                tweet_data = json.loads(json_str)
                for metric in metrics:
                    value = tweet_data.get(metric, 0)
                    # 确保值是数字类型
                    if value is None:
                        value = 0
                    elif isinstance(value, (int, float)):
                        value = int(value)
                    else:
                        value = 0
                    
                    # 添加到对应的分类
                    if is_grok:
                        metric_data['grok'][metric].append(value)
                    else:
                        metric_data['users'][metric].append(value)
                    metric_data['total'][metric].append(value)
                
                success_count += 1
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                error_count += 1
                # 如果解析失败，为所有指标添加0
                for metric in metrics:
                    if is_grok:
                        metric_data['grok'][metric].append(0)
                    else:
                        metric_data['users'][metric].append(0)
                    metric_data['total'][metric].append(0)
        
        print(f"\nJSON parsing: {success_count} successful, {error_count} errors")
        print(f"Grok tweets: {grok_tweet_count:,}, User tweets: {user_tweet_count:,}")
        
        # 统计每个指标的分布
        print(f"\n=== Engagement Metrics Distribution ===")
        
        # 定义统计函数
        def calculate_stats(values, label):
            """计算并返回统计信息"""
            if not values:
                return None
            
            non_zero_values = [v for v in values if v > 0]
            
            stats = {
                'count': len(values),
                'min': min(values),
                'max': max(values),
                'mean': statistics.mean(values),
                'min_non_zero': min(non_zero_values) if non_zero_values else 0,
                'mean_non_zero': statistics.mean(non_zero_values) if non_zero_values else 0
            }
            return stats
        
        for metric in metrics:
            print(f"\n{'='*60}")
            print(f"{metric}:")
            print(f"{'='*60}")
            
            # 统计三个分类
            categories = [
                ('Grok Tweets', 'grok'),
                ('User Tweets', 'users'),
                ('Total (All)', 'total')
            ]
            
            for category_name, category_key in categories:
                values = metric_data[category_key][metric]
                stats = calculate_stats(values, category_name)
                
                if stats:
                    print(f"\n{category_name}:")
                    print(f"  Count: {stats['count']:,}")
                    print(f"  Min: {stats['min']:,}")
                    print(f"  Max: {stats['max']:,}")
                    print(f"  Mean: {stats['mean']:.2f}")
                    if stats['min_non_zero'] > 0:
                        print(f"  Min (non-zero): {stats['min_non_zero']:,}")
                        print(f"  Mean (non-zero): {stats['mean_non_zero']:.2f}")
                else:
                    print(f"\n{category_name}: No data")
        
        # 保存详细数据到CSV
        output_metrics_csv = 'engagement_metrics_distribution.csv'
        print(f"\n\nSaving detailed statistics to {output_metrics_csv}...")
        
        with open(output_metrics_csv, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['author_type', 'metric', 'value'])
            
            for category_key, category_name in [('grok', 'grok'), ('users', 'users'), ('total', 'total')]:
                for metric in metrics:
                    for value in metric_data[category_key][metric]:
                        writer.writerow([category_name, metric, value])
        
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
