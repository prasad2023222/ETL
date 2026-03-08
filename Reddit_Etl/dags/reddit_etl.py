from airflow import DAG
from airflow.sdk import task
#from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
from textblob import TextBlob

POSTGRES_CONN_ID="postgres_default"

default_args={
    "owner":"airflow",
    "start_date":datetime(2024,2,1)
}

with DAG(dag_id="reddit_etl",default_args=default_args,schedule="@daily",catchup=False)as dags:
    @task()
    def extract_reddit():

        url="https://api.reddit.com/r/python/hot"
        headers={
            "User-Agent": "python:reddit.etl:v1.0 (by /u/u/Odd-Thought6360)"
        }

        response=requests.get(url,headers=headers)

        data=response.json()

        posts=[]
        for post in data["data"]["children"]:
            p=post["data"]
            posts.append({
                "id":p["id"],
                "title":p["title"],
                "comments":p["num_comments"],
                "author":p["author"],
                "score":p["score"]
            })

        return posts

    @task()
    def transform_data(posts):
        transformed=[]

        for p in posts:
            sentiment=TextBlob(p["title"]).sentiment.polarity

            transformed.append((
                p["id"],
                p["title"],
                p["comments"],
                p["author"],
                p["score"],
                sentiment
            ))

            return transformed

    @task()
    def load_data(records):
        pg_hook=PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        create_table="""create table if not exists reddit_posts(post_id text PRIMARY KEY,title text,comments text,author text,score int,sentiment float ,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP); 
        """
        pg_hook.run(create_table)


        insert_sql="""insert into reddit_posts(post_id,title,comments,author,score,sentiment) values(%s,%s,%s,%s,%s,%s) ON CONFLICT (post_id) DO NOTHING;"""

        for record in records:
            pg_hook.run(insert_sql,parameters=record)

    posts=extract_reddit()
    transform=transform_data(posts)
    load_data(transform)

