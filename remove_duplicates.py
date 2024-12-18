import json
import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

import dataset

# 权重和阈值设定

stop_words = {"a", "of", "the", "and", "to", "in", "for", "on", "with", "at", "by", "from"}

def clean_movie_name(movie_name):
    # 将电影名称转换为小写，并移除停用词
    words = movie_name.lower().split()
    cleaned_words = [word for word in words if word not in stop_words]
    return " ".join(cleaned_words)

movies:dataset.movieset = []
file = "data/movie.json"
result_path = "data/movies.json"
with open(file,"r",encoding='utf-8') as f:
    for line in f:
        data = json.loads(line.strip())  # 逐行读取并解析为 JSON 对象
        mset = dataset.dict_to_movieset(data)
        movies.append(mset)
movies.sort(key=lambda x: x.movieName)
print("排序完成")
similarity_threshold = 90
merged_indices = set()
with open(result_path, 'a', encoding='utf-8') as f:
    for i in range(len(movies)) :
        # 如果当前行已经被合并，则跳过
        if i in merged_indices:
            continue
        
        movie:dataset.movieset = movies[i]
        if not movie.movieName:
            continue
        for j in range(i + 1, min(i + 10, len(movies))):
            # 如果 comparison_row 已经被合并，跳过它
            if j in merged_indices:
                continue
           # 计算各属性相似度得分
            movie_name_score = fuzz.ratio(clean_movie_name(movie.movieName),clean_movie_name(movies[j].movieName))
            
            if movie_name_score >= similarity_threshold:
                # 将该行的索引添加到 merged_indices 中
                movie.movieName = movie.movieName.strip() if len(movie.movieName)<=len(movies[j].movieName) else movies[j].movieName.strip()
            
                merged_indices.add(j)
                movie.movieID.extend(movies[j].movieID)
                
                # 合并 movieReleaseTime 取最早的日期
                current_release = pd.to_datetime(movie.movieReleaseTime, errors='coerce')
                comparison_release = pd.to_datetime(movies[j].movieReleaseTime, errors='coerce')
                earliest_release = min(current_release, comparison_release)
                movie.movieReleaseTime = earliest_release.strftime('%Y-%m-%d')

                movie.movieStyle = list(set(movies[j].movieStyle)|set(movie.movieStyle))

                movie.actors = list(set(movies[j].actors)|set(movie.actors))
                movie.director = list(set(movies[j].director)|set(movie.director))
                movie.score += movies[j].score
                movie.commentCount += movies[j].commentCount
        
        movie.score = movie.score/movie.commentCount
        json_line = json.dumps(movie.to_dict(), ensure_ascii=False)
        f.write(json_line+'\n')
