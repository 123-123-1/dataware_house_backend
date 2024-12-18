import json
import os

from amazon.dataset import Dataset
from amazon.dataset import Review
from amazon.dataset import dict_to_dataset
from amazon.dataset import dict_to_review


header = [
    'product/productId: ',
    'review/userId: ',
    'review/profileName: ',
    'review/helpfulness: ',
    'review/score: ',
    'review/time: ',
    'review/summary: ',
    'review/text: '
]


def read_comment(file_path,log_path):
   
    line_number = 0
    if os.path.exists(log_path):
        with open(log_path, 'r') as file:
            last_line = file.readlines()[-1]  # 读取最后一行
            # 提取已读行数
            if last_line.startswith("已读行数: "):
                line_number = int(last_line.split(":")[1])

    lines = []
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            for _ in range(line_number):  # 跳过已读的行
                next(file)

            for line in file:
                line = line.strip()
                line_number += 1
                
                # 检查行是否为空，如果为空说明一个评论结束
                if not line :
                    continue
                elif len(lines) == 8 :
                    if header[0] in line :
                        yield lines
                        lines =[]
                        
                        lines.append(line.replace(header[len(lines)],''))
                    else :
                        lines[-1] += line
                elif header[len(lines)] in line:
                    
                    lines.append(line.replace(header[len(lines)],'') )
                else :
                    lines[-1]+= line
            if len(lines) == 8 :
                yield lines
                
    finally:
        with open(log_path, 'a') as log_file:
            log_file.write(f"已读行数: {line_number}\n")

def extract_from_txt(file_path,log_path):
    data = None
    try:
        for comment in read_comment(file_path,log_path):
            if not data or data.movieID != comment[0] :
                if data :

                    yield data
                data = Dataset(comment[0])
            review = Review(comment[1],comment[2],comment[3],comment[4],comment[5],comment[6],comment[7])
            data.add_Review(review)

    finally :
        if data :
            yield data 

def extract_from_json(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                data = json.loads(line.strip())  # 逐行读取并解析为 JSON 对象
                dataset = dict_to_dataset(data)
                yield dataset
    finally:
        return 

if __name__ == "__main__":
    i = 0 
    path = 'D:\\myfile\\etl\\amazon\\'
    file_path = path+'data\\data0.json'
    for i,data in enumerate(extract_from_txt(path+'data\\movies.txt', path+'data\\log.txt') ): 
        with open(file_path, 'a', encoding='utf-8') as file:
            json_line = json.dumps(data.to_dict(), ensure_ascii=False)
            file.write(json_line+'\n')
        if i== 9999 :
            file_path = path + 'data\\data1.json'
        if i>=19999:
            break
                    

    
   #for data in extract_from_json(file_path) :
    #   print(data.movieID)