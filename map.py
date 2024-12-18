

import json

import dataset

#p = {}
i = 0
with open("data/movies.json", 'r', encoding='utf-8') as file:
    with open("data/moviess.json", 'a', encoding='utf-8') as f:
        for line in file:
            data = json.loads(line.strip())  # 逐行读取并解析为 JSON 对象
            mset = dataset.dict_to_movieset(data)
            #for j in mset.movieID:
                #p[j.strip()]=i
            mset.index = i
            json_line = json.dumps(mset.to_dict(), ensure_ascii=False)
            f.write(json_line+'\n')
            i+=1
