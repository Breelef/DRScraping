import requests
from bs4 import BeautifulSoup
import feedparser
import pandas as pd
import nltk
import os
import sqlite3
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from collections import Counter
from datetime import datetime

# Download necessary NLTK components
nltk.download("punkt")
nltk.download("stopwords")

# DR regional and Indland RSS feeds
rss_feeds = {
    'Hovedstadsområdet': 'https://www.dr.dk/nyheder/service/feeds/regionale/kbh',
    'Bornholm': 'https://www.dr.dk/nyheder/service/feeds/regionale/bornholm',
    'Syd og Sønderjylland': 'https://www.dr.dk/nyheder/service/feeds/regionale/syd',
    'Fyn': 'https://www.dr.dk/nyheder/service/feeds/regionale/fyn',
    'Midt- og Vestjylland': 'https://www.dr.dk/nyheder/service/feeds/regionale/vest',
    'Nordjylland': 'https://www.dr.dk/nyheder/service/feeds/regionale/nord',
    'Trekantområdet': 'https://www.dr.dk/nyheder/service/feeds/regionale/trekanten',
    'Sjælland': 'https://www.dr.dk/nyheder/service/feeds/regionale/sjaelland',
    'Østjylland': 'https://www.dr.dk/nyheder/service/feeds/regionale/oestjylland'
}

indland_feed = "https://www.dr.dk/nyheder/service/feeds/indland"

# Danish region keywords
region_keywords = {
    'Hovedstadsområdet': [
        "København", "Frederiksberg", "Amager", "Dragør", "Tårnby", "Hvidovre", "Rødovre", "Glostrup",
        "Brøndby", "Herlev", "Ballerup", "Gentofte", "Lyngby", "Gladsaxe", "Ishøj", "Vallensbæk", "Høje-Taastrup",
        "Albertslund", "Egedal", "Furesø", "Rudersdal"
    ],
    'Bornholm': [
        "Bornholm", "Rønne", "Nexø", "Aakirkeby", "Allinge", "Gudhjem", "Hasle", "Svaneke", "Østermarie"
    ],
    'Syd og Sønderjylland': [
        "Esbjerg", "Haderslev", "Aabenraa", "Sønderborg", "Tønder", "Varde", "Vejen", "Fanø", "Ribe", "Gråsten",
        "Bramming", "Nordborg", "Augustenborg", "Tinglev", "Toftlund", "Løgumkloster"
    ],
    'Fyn': [
        "Odense", "Svendborg", "Nyborg", "Middelfart", "Assens", "Faaborg", "Kerteminde", "Ringe", "Bogense",
        "Munkebo", "Otterup", "Årslev", "Langeskov", "Marstal", "Ærøskøbing"
    ],
    'Midt- og Vestjylland': [
        "Herning", "Holstebro", "Viborg", "Skive", "Struer", "Ikast", "Ringkøbing", "Lemvig", "Silkeborg", "Brande",
        "Karup", "Kjellerup", "Haderup"
    ],
    'Nordjylland': [
        "Aalborg", "Hjørring", "Frederikshavn", "Thisted", "Brønderslev", "Hobro", "Nørresundby", "Skagen", "Sæby",
        "Aabybro", "Løgstør", "Hirtshals", "Nibe", "Støvring", "Fjerritslev", "Hadsund", "Brovst", "Dronninglund"
    ],
    'Trekantområdet': [
        "Vejle", "Kolding", "Fredericia", "Billund", "Middelfart", "Give", "Børkop", "Egtved", "Jelling"
    ],
    'Sjælland': [
        "Roskilde", "Næstved", "Slagelse", "Køge", "Holbæk", "Kalundborg", "Ringsted", "Sorø", "Vordingborg",
        "Nykøbing Falster", "Faxe", "Nakskov", "Maribo", "Stege", "Præstø", "Haslev"
    ],
    'Østjylland': [
        "Aarhus", "Randers", "Horsens", "Skanderborg", "Grenaa", "Ebeltoft", "Hadsten", "Hammel", "Odder",
        "Hinnerup", "Hedensted", "Ry", "Tilst", "Rønde", "Samsø"
    ]
}

# SQLite DB setup
def create_db():
    conn = sqlite3.connect("dr_articles.db")
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS articles (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        title TEXT NOT NULL,
                        link TEXT NOT NULL UNIQUE,
                        region TEXT,
                        published TEXT,
                        source TEXT)''')
    conn.commit()
    conn.close()

def article_exists(url):
    """Checks if the article already exists in the SQLite DB by URL."""
    conn = sqlite3.connect("dr_articles.db")
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM articles WHERE link = ?", (url,))
    exists = cursor.fetchone() is not None
    conn.close()
    return exists

def insert_article(article):
    """Inserts a new article into the SQLite DB."""
    conn = sqlite3.connect("dr_articles.db")
    cursor = conn.cursor()
    cursor.execute("INSERT INTO articles (title, link, region, published, source) VALUES (?, ?, ?, ?, ?)",
                   (article['title'], article['link'], article['region'], article['published'], article['source']))
    conn.commit()
    conn.close()

def fetch_article_text(url):
    """Fetches full article text from a DR.dk article page."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise error for HTTP issues
        soup = BeautifulSoup(response.text, "html.parser")

        # Find paragraphs inside the main article container
        article_body = soup.find("article")
        if article_body:
            paragraphs = article_body.find_all("p")
            full_text = " ".join(p.get_text() for p in paragraphs)
            return full_text
    except requests.RequestException as e:
        print(f"Error fetching article {url}: {e}")
    return ""

def classify_article(text):
    """Determines the Danish region an article is about based on keywords in full text."""
    words = set(word_tokenize(text.lower()))
    words = {w for w in words if w.isalpha() and w not in stopwords.words("danish")}

    region_counts = Counter()

    for region, keywords in region_keywords.items():
        keyword_set = {k.lower() for k in keywords}
        matches = words & keyword_set
        if matches:
            region_counts[region] += len(matches)

    return region_counts.most_common(1)[0][0] if region_counts else "Unknown"

def fetch_and_store_articles():
    """Scrapes DR.dk articles, categorizes them, and stores in SQLite without duplicates."""
    create_db()  # Ensure the DB is created

    # Scrape regional articles
    for region, url in rss_feeds.items():
        feed = feedparser.parse(url)

        for entry in feed.entries:
            if not article_exists(entry.link):
                article = {
                    "region": region,
                    "title": entry.title.strip(),
                    "link": entry.link.strip(),
                    "published": entry.published if hasattr(entry, 'published') else "Unknown",
                    "source": "Regional"
                }
                insert_article(article)

    # Scrape and analyze "Indland" articles with full text classification
    indland_feed_data = feedparser.parse(indland_feed)
    for entry in indland_feed_data.entries:
        full_text = fetch_article_text(entry.link)
        region = classify_article(full_text) if full_text else "Unknown"

        if not article_exists(entry.link):
            article = {
                "region": region,
                "title": entry.title.strip(),
                "link": entry.link.strip(),
                "published": entry.published if hasattr(entry, 'published') else "Unknown",
                "source": "Indland"
            }
            insert_article(article)

    print(f"Scraped new articles on {datetime.now()}")

if __name__ == "__main__":
    fetch_and_store_articles()
