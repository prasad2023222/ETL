import requests
import json
import pandas as pd 

url = "https://api.reddit.com/r/python/hot"

headers = {
    "User-Agent": "python:reddit.etl:v1.0 (by /u/u/Odd-Thought6360)"
}

response = requests.get(url, headers=headers)
data=response.json()

posts=[]

for post in data["data"]["children"]:
    p=post["data"]
    posts.append({
        "title":p["title"],
        "author":p["author"],
        "score":p["score"],
        "comments":p["num_comments"],
        "url":p["url"]

    })

df=pd.DataFrame(posts)

df.to_csv("reddit_posts.csv",index=False)

print(df.head())

#print("Status:", response.status_code)

#if response.status_code==200:
    #data=response.json()
    #print(json.dumps(data,indent=2)[:1000])
#else:
   # print("response failed")
    #print(response.text)
