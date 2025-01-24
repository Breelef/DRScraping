import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from tabulate import tabulate
import numpy as np
from datetime import timedelta
import sqlite3


def cluster_dates(df):
    """Clusters articles with proper date ranges"""
    df['date'] = pd.to_datetime(df['date'])
    reference_date = pd.to_datetime('2025-01-14')

    # Calculate weeks difference
    df['week_num'] = np.where(
        df['date'] < reference_date,
        -1,  # For dates before reference
        ((df['date'] - reference_date).dt.days // 7) + 1
    )

    # Create date range labels
    def get_date_range(week_num):
        if week_num == -1:
            return 'Before 14/01/2025'
        start_date = reference_date + timedelta(days=(week_num - 1) * 7)
        end_date = start_date + timedelta(days=6)
        return f"{start_date.strftime('%d/%m')} - {end_date.strftime('%d/%m')}"

    df['cluster_date'] = df['week_num'].apply(get_date_range)
    df['sort_order'] = df['week_num']

    return df.sort_values('sort_order')


def display_article_summary():
    try:
        conn = sqlite3.connect("dr_articles.db")
        df = pd.read_sql_query("SELECT * FROM articles", conn)

        if df.empty:
            print("No articles found in the database.")
            return None, None

        df["published"] = pd.to_datetime(df["published"], errors="coerce")
        df = df.dropna(subset=["published"])
        df["date"] = df["published"].dt.date

        df = cluster_dates(df)

        print("\n🔍 Unique regions in dataset:", df["region"].unique())

        summary = df.groupby(["cluster_date", "region"]).size().reset_index(name="article_count")

        # Custom sorter
        def get_sort_key(date_str):
            if date_str == 'Before 14/01/2025':
                return '0'
            return date_str.split(' - ')[0]

        # Sort by custom key
        summary['sort_key'] = summary['cluster_date'].apply(get_sort_key)
        summary = summary.sort_values('sort_key')
        summary = summary.drop('sort_key', axis=1)

        print("\n📊 Article Count by Region and Cluster Date:")
        print(tabulate(summary, headers="keys", tablefmt="pretty"))

        unknown_articles = df[df["region"].str.lower() == "unknown"][["title", "link", "date"]]

        print("\n🛠️ Sample articles to verify classification:")
        print(df[["title", "region"]].sample(10))

        if not unknown_articles.empty:
            print("\n🚨 Articles with Unknown Region:")
            print(tabulate(unknown_articles, headers="keys", tablefmt="pretty"))
        else:
            print("\n✅ No articles classified as 'Unknown'.")

        # Bar Chart
        plt.figure(figsize=(12, 6))
        sns.barplot(x="cluster_date", y="article_count", hue="region", data=summary, palette="tab10")
        plt.xticks(rotation=45)
        plt.xlabel("Clustered Date")
        plt.ylabel("Number of Articles")
        plt.title("Article Count by Region and Clustered Date")
        plt.legend(title="Region", bbox_to_anchor=(1.05, 1), loc="upper left")
        plt.tight_layout()
        plt.show()

        # Heatmap
        pivot_table = summary.pivot(index="region", columns="cluster_date", values="article_count").fillna(0)
        # Reorder columns chronologically
        column_order = ["Before 14/01/2025"] + [col for col in pivot_table.columns if col != "Before 14/01/2025"]
        pivot_table = pivot_table[column_order]

        plt.figure(figsize=(12, 6))
        sns.heatmap(pivot_table, cmap="coolwarm", annot=True, fmt=".0f", linewidths=0.5)
        plt.xlabel("Clustered Date")
        plt.ylabel("Region")
        plt.title("Article Distribution Heatmap")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()

        # Pie Chart of Total Articles by Region
        total_by_region = summary.groupby('region')['article_count'].sum()
        plt.figure(figsize=(10, 8))
        plt.pie(total_by_region, labels=total_by_region.index, autopct='%1.1f%%')
        plt.title("Distribution of Articles by Region")
        plt.show()

        return summary, unknown_articles

    except Exception as e:
        print(f"Error occurred: {e}")
        return None, None


summary_df, unknown_df = display_article_summary()