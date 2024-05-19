import requests
from bs4 import BeautifulSoup
import pandas as pd

# Search Query
query = 'US Economy'

# Encode special characters in a text string
def encode_special_characters(text):
    encoded_text = ''
    special_characters = {'&': '%26', '=': '%3D', '+': '%2B', ' ': '%20'}  # Add more special characters as needed
    for char in text.lower():
        encoded_text += special_characters.get(char, char)
    return encoded_text

query2 = encode_special_characters(query)
url = f"https://news.google.com/search?q={query2}&hl=en-US&gl=US&ceid=US%3Aen"

response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

articles = soup.find_all('article')
links = [article.find('a')['href'] for article in articles]
links = [link.replace("./articles/", "https://news.google.com/articles/") for link in links]

news_text = [article.get_text(separator='\n') for article in articles]
news_text_split = [text.split('\n') for text in news_text]

news_df = pd.DataFrame({
    'Title': [text[2] for text in news_text_split],
    'Source': [text[0] for text in news_text_split],
    'Time': [text[3] if len(text) > 3 else 'Missing' for text in news_text_split],
    'Author': [text[4].split('By ')[-1] if len(text) > 4 else 'Missing' for text in news_text_split],
    'Link': links
})

# Write to CSV
news_df.to_csv('news.csv', index=False)