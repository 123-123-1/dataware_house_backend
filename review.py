import csv
import json
import dataset


reviewfile = open('table/movie_review0.csv','a',newline='',encoding='utf-8')
review_writer = csv.DictWriter(reviewfile,fieldnames=["INDEX", "ID","ASIN", "PROFILENAME", "HELPFULNESS", "SCORE", "TIME","SUMMARY"])
review_writer.writeheader()

with open('data/comments.json', 'r', encoding='utf-8') as file:
    for i,line in enumerate(file):
        data = json.loads(line.strip())  
        mset = dataset.dict_to_movieReview(data)
        data_part = {"INDEX":mset.index, 
            "ID":mset.ID,
            "PROFILENAME":mset.ProfileName,   # 取第一个元素
            "HELPFULNESS": mset.helpfulness,
            "TIME": mset.time,
            "SCORE":mset.score,
            "SUMMARY":mset.summary,
            "ASIN":mset.asin
        }
        review_writer.writerow(data_part)
        if(i+1)%100000==0:
            reviewfile = open(f'table/movie_review{int((i+1)/100000)}.csv','a',newline='',encoding='utf-8')
            review_writer = csv.DictWriter(reviewfile,fieldnames=["INDEX", "ID","ASIN", "PROFILENAME", "HELPFULNESS", "SCORE", "TIME","SUMMARY"])
            review_writer.writeheader()
