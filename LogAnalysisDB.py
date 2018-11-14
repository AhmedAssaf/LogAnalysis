# Database code for the DB News

import psycopg2

DBNAME = "news"

def get_articles():
    """1. What are the most popular three articles of all time?."""
    query = """
    select a.title , count(a.title) as views 
    from log l 
    inner join articles a  on l.path like '%'|| a.slug || '%' 
    where l.status != '404 NOT FOUND'
    group by a.title 
    order by count(a.title) desc limit 3;
    """
    db = psycopg2.connect(database=DBNAME)
    cur = db.cursor()
    cur.execute(query)
    articles = cur.fetchall()
    cur.close()
    db.close()
    return articles

def get_authors():
    """2. Who are the most popular article authors of all time?."""
    query = """
    select b.name , count(b.name) as views 
    from log l 
    inner join articles a  on l.path like '%'|| a.slug || '%' 
    inner join authors b on a.author=b.id 
    where l.status != '404 NOT FOUND'
    group by b.name 
    order by count(b.name) desc limit 4;
    """
    db = psycopg2.connect(database=DBNAME)
    cur = db.cursor()
    cur.execute(query)
    articles = cur.fetchall()
    cur.close()
    db.close()
    return articles

def get_requests_error():
    """3. On which days did more than 1% of requests lead to errors?"""
    query = """
    WITH totalviews AS
        (
        SELECT date_trunc('day',time)  as dte, 
        status, Count(*) AS rowscount, 
        (SUM(Count(*)) OVER (PARTITION BY date_trunc('day',time))) as totalcount
        FROM log
        GROUP BY date_trunc('day',time), status 
        )
    SELECT dte, round( CAST(float8 ((rowscount*100)/totalcount) as numeric), 2) AS P 
    FROM totalviews
    WHERE status = '404 NOT FOUND' 
    and ((rowscount*100)/totalcount) > 1;
    """
    db = psycopg2.connect(database=DBNAME)
    cur = db.cursor()
    cur.execute(query)
    articles = cur.fetchall()
    cur.close()
    db.close()
    return articles