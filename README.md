# test.py
Here I have put code that allows integration of fastAPI with a sample relational (PostgreSQL) database called dvdrental.
In the code, I use GET requests with fastAPI to access data in different tables and columns in the database

# dvdrental.zip and dvdrental.tar
This is the relational database that I was testing. It has 15 tables, namely: "actor", "store", "address", "category", "city", "country", "customer", "film_actor", "film_category", "inventory", "language", "rental", "staff", "payment", "film"

# tonydb.py
Here I have general code that allows the user to perform CRUD operations on the database tonydb (but it works for any local db). Note that the PostgreSQL username is "postgres" and the password is "postgres".
I have used GET, POST, PUT, DELETE and PATCH methods.

# scraper.py
This is the twitter scraper that is used to access twitter.com, access the profile Elon Musk, scrape tweets and save to a json file.

# elon_tweets.json
This is the json object that contains the scraped tweets

# lastdbtest.py
Work in progress. Currently debugging since I am facing some issues.
