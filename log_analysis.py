#!/usr/bin/env python3

import psycopg2
import datetime

DBNAME = "news"


def main():
    # Open file to results and database
    example = open("submission.txt", "w")
    db = psycopg2.connect(database=DBNAME)

    # Query 1 about most popular articles
    example.write("1. What are the most popular three" +
                  "articles of all time?\n\n")
    c = db.cursor()
    c.execute("SELECT a.title, COUNT(*) as count from articles A, log L " +
              "WHERE SUBSTRING(L.path FROM '/article/(.+)') = A.slug " +
              "AND L.path SIMILAR TO '/article/%'" +
              "GROUP BY a.title ORDER BY count DESC LIMIT 3 ;")
    posts = c.fetchall()
    for article, count in posts:
        formatted = str("{} - {} views\n").format(article, count)
        example.write(formatted)

    # Query 2 about most popular authors
    example.write("\n\n2. Who are the most popular article authors" +
                  " of all time?\n\n")
    c = db.cursor()
    c.execute("SELECT w.name, COUNT(*) as count " +
              "from articles A, log L, authors W WHERE " +
              "SUBSTRING(L.path FROM '/article/(.+)') = A.slug AND L.path " +
              "SIMILAR TO '/article/%' AND A.author = w.id "
              "AND L.status = '200 OK' GROUP BY w.name ORDER by count DESC;")

    posts = c.fetchall()
    for author, count in posts:
        formatted = str("{} - {} views\n").format(author, count)
        example.write(formatted)

    # Query 3 about days with errors more than 1%
    example.write("\n\n3. On which days did more than 1% of requests" +
                  " lead to errors?\n\n")
    c = db.cursor()
    c.execute("SELECT date_trunc('day', time) AS date, " +
              "(1.0 *SUM(CASE WHEN status != '200 OK' THEN 1 ELSE 0 END)" +
              "/COUNT(*)) as percentage FROM log GROUP BY date " +
              "HAVING (1.0 * SUM(CASE WHEN STATUS != '200 OK' THEN 1 ELSE 0" +
              "END)/COUNT(*)) > 0.01 ORDER BY percentage DESC;")

    posts = c.fetchall()
    for date, percentage in posts:
        # Format datetime string as mm/dd/yy
        date = date.strftime('%m/%d/%Y')
        # Format percentages to have 2 floating decimal points
        formatted = str("{} - {:.2%} errors\n").format(date, percentage / 1)
        example.write(formatted)

    # Close database and textfile
    db.close()
    example.close()

if __name__ == '__main__':
    main()
