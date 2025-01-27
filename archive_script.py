import requests
from bs4 import BeautifulSoup
import sqlite3
from collections import Counter
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from datetime import datetime
import time  # Import time module for rate limiting

# Define your region keywords
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


def create_db():
    conn = sqlite3.connect("dr_articles.db")
    cursor = conn.cursor()

    # Create table for current articles
    cursor.execute('''CREATE TABLE IF NOT EXISTS articles (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        title TEXT NOT NULL,
                        link TEXT NOT NULL UNIQUE,
                        region TEXT,
                        published TEXT,
                        source TEXT)''')

    # Create table for archived articles
    cursor.execute('''CREATE TABLE IF NOT EXISTS archived_articles (
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


def archived_article_exists(url):
    """Checks if the archived article already exists in the SQLite DB by URL."""
    conn = sqlite3.connect("dr_articles.db")
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM archived_articles WHERE link = ?", (url,))
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


def insert_archived_article(article):
    """Inserts a new archived article into the SQLite DB."""
    conn = sqlite3.connect("dr_articles.db")
    cursor = conn.cursor()
    cursor.execute("INSERT INTO archived_articles (title, link, region, published, source) VALUES (?, ?, ?, ?, ?)",
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


def scrape_articles(date):
    url = f'https://web.archive.org/web/{date}/http://www.dr.dk/'
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        articles = soup.find_all('div', class_='dre-article-teaser')  # Adjust based on actual HTML structure

        for article in articles:
            title_tag = article.find('a', class_='dre-teaser-title')
            if title_tag:
                title = title_tag.get_text(strip=True)
                link = title_tag['href']  # This is the Wayback link

                # Ensure the link is a full URL
                if link.startswith('/web/'):
                    original_link = link.split("/web/")[1]  # Extract the original link
                    original_link = f"http://www.dr.dk{original_link}"  # Construct the full original link
                else:
                    original_link = link  # If it's already a full link

                published = date  # You can adjust this to extract the actual published date

                # Scrape the article content using the Wayback link
                full_text = fetch_article_text(f"https://web.archive.org{link}")
                region = classify_article(full_text) if full_text else "Unknown"

                if not archived_article_exists(original_link):
                    article_data = {
                        "region": region,
                        "title": title,
                        "link": original_link,
                        "published": published,
                        "source": "DR"
                    }
                    insert_archived_article(article_data)

                # Rate limiting: wait for 2 seconds before the next request
                time.sleep(2)  # Adjust the delay as needed


def main():
    create_db()  # Ensure the DB is created
    dates = [
        '20220215', '20220315', '20220415', '20220515', '20220615',
        '20220715', '20220815', '20220915', '20221015', '20221115',
        '20221215', '20240115', '20240215', '20240315', '20240415',
        '20240515', '20240615', '20240715', '20240815', '20240915',
        '20241015', '20241115', '20241215'
    ]

    for date in dates:
        scrape_articles(date)
        # Additional rate limiting between different dates
        time.sleep(5)  # Wait for 5 seconds before scraping the next date


if __name__ == "__main__":
    main()
