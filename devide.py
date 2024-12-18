import json
import re
import dataset
import csv 
from datetime import datetime

def get_season(month):
    if month in [3, 4, 5]:
        return "spring"
    elif month in [6, 7, 8]:
        return "summer"
    elif month in [9, 10, 11]:
        return "autumn"
    else:
        return "winter"

movieflie = open('table/movie.csv','a',newline='',encoding='utf-8')
timefile= open('table/time.csv','a',newline='',encoding='utf-8')
actorfile = open('table/actor.csv','a',newline='',encoding='utf-8')
directorfile = open('table/director.csv','a',newline='',encoding='utf-8')
stylefile = open('table/movie_style.csv','a',newline='',encoding= 'utf-8')
versionfile = open('table/movie_version.csv','a',newline='',encoding='utf-8')

movie_writer = csv.DictWriter(movieflie,fieldnames=["MOVIE_ID", "MOVIE_NAME", "MOVIE_VERSIONS_NUM", "MOVIE_RELEASE_TIME", "COMMENT_NUM", "SCORE"])
movie_writer.writeheader()
director_writer = csv.DictWriter(directorfile,fieldnames=["DIRECTOR_NAME","MOVIE_ID","MOVIE_NAME"])
director_writer.writeheader()
actor_writer = csv.DictWriter(actorfile,fieldnames=["ACTOR_NAME","MOVIE_ID"])
actor_writer.writeheader()
style_writer = csv.DictWriter(stylefile,fieldnames=["STYLE","MOVIE_ID"])
style_writer.writeheader()
version_writer = csv.DictWriter(versionfile,fieldnames=["VERSION","MOVIE_ID"])
version_writer.writeheader()
time_writer = csv.DictWriter(timefile,fieldnames=["MOVIE_ID","YEAR","MONTH","DAY","SEASON","WEEKDAY","TIME"])
time_writer.writeheader()
with open('data/moviess.json', 'r', encoding='utf-8') as file:
    for line in file:
        data = json.loads(line.strip())  
        mset = dataset.dict_to_movieset(data)
        data_part = {"MOVIE_ID":mset.index, 
            "MOVIE_NAME":mset.movieName,
            "MOVIE_VERSIONS_NUM":len(mset.movieID),   # 取第一个元素
            "MOVIE_RELEASE_TIME": mset.movieReleaseTime,
            "COMMENT_NUM": mset.commentCount,
            "SCORE":mset.score
        }
        movie_writer.writerow(data_part)
        for i in mset.director:
            data_part = {
                "DIRECTOR_NAME":i,
                "MOVIE_ID": mset.index,
                "MOVIE_NAME":mset.movieName
            }
            director_writer.writerow(data_part)
        for i in mset.actors:
            data_part = {
                "ACTOR_NAME":i,
                "MOVIE_ID": mset.index
            }
            actor_writer.writerow(data_part)
        for i in mset.movieStyle:
            data_part = {
                "STYLE":i,
                "MOVIE_ID": mset.index
            }
            style_writer.writerow(data_part)
        for i in mset.movieID:
            data_part = {
                "VERSION":i,
                "MOVIE_ID": mset.index
            }
            version_writer.writerow(data_part)
        t = datetime.strptime(mset.movieReleaseTime, "%Y-%m-%d")
        
        data_part={
            "MOVIE_ID": mset.index,
            "YEAR": t.year,
            "MONTH": t.month,
            "DAY": t.day,
            "SEASON": get_season(t.month),
            "WEEKDAY": t.strftime("%A"),
            "TIME": mset.movieReleaseTime
        }
        time_writer.writerow(data_part)
movieflie.close()
actorfile.close()
directorfile.close()
versionfile.close()
stylefile.close()
timefile.close()
