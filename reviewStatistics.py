from collections import defaultdict
import csv
import json
import dataset


statisticsfile = open('table/movie_review_statistics.csv','a',newline='',encoding='utf-8')
statistics_writer = csv.DictWriter(statisticsfile,fieldnames=["MOVIE_ID", "THREE","FOUR","FIVE"])
statistics_writer.writeheader()
distribution = defaultdict(lambda: {"3": 0, "4": 0, "5": 0})
with open('data/comments.json', 'r', encoding='utf-8') as file:
    for line in file:
        data = json.loads(line.strip())  
        mset = dataset.dict_to_movieReview(data)
       
        if 3<= float(mset.score) <4 :
            distribution[mset.index]["3"] += 1
        elif 4<= float(mset.score)<5 :
            distribution[mset.index]["4"] += 1
        elif float(mset.score)>=5:
            distribution[mset.index]["5"]+=1
for movie_id, counts in distribution.items():
    row = {'MOVIE_ID': movie_id, 'THREE': counts['3'], 'FOUR': counts['4'], 'FIVE': counts['5']}
    statistics_writer.writerow(row)