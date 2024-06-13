#Objective to webscrape a website, i.e extract info from a website using the requests and beautifulsoup libraries and save it in desired formats

import requests #preinstalled in python3, used to communicate with the website
import pandas as pd #to manipulate the structured data extracted in tabular form(dataframe)
import sqlite3 #make and query a database
from bs4 import BeautifulSoup  #to read the html or parse the html code


#Initialization of known entities
#known entities are the url, the db name, the csv_path, the df we want to create

url = "https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films"

db_name = "Movies.db"

table_name = "Top_25"

csv_path = "~/Documents/IBM data engineering course/IBM Python for Data Engineering/webscraping/top_25_films.csv"

df = pd.DataFrame(columns=["Film", "Year", "Rotten Tomatoes' Top 100"])

count = 0



### Loading the webpage for webscraping

# we use the requests.get().text function to load the entire web page as html doc.
#we use Beautifulsoup to parse the text in the html doc so as to enable extraction of relevant info

html_page = requests.get(url).text

data = BeautifulSoup(html_page, "html.parser")


#scraping the webpage

tables = data.find_all('tbody')

rows = tables[0].find_all('tr')

#write a loop function to extract desired info
#desired rows can be accessed using the find_all() with the beautifulsoup object

for row in rows:
    if count < 25: 
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {"Film":str(col[1].contents[0]), 
                         "Year": int(col[2].contents[0]),
                         "Rotten Tomatoes' Top 100": str(col[3].contents[0])}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
            count+=1
    else:
        break


#filter output to print only 2000s films
filt_year = df[df['Year'] >= 2000]

print(filt_year)



#storing the Data

df.to_csv(csv_path)

#storing in a database

conn = sqlite3.connect(db_name)

df.to_sql(table_name, conn, if_exists = 'replace', index = False)

conn.close()