import csv
import json
import re

import dataset


#分词
def clean():
    with open('data/movie.json', 'r', encoding='utf-8') as file:
        with open('data/movie-1.json', 'a', encoding='utf-8') as f:
            for line in file:
                data = json.loads(line.strip())  # 逐行读取并解析为 JSON 对象
                mset = dataset.dict_to_movieset(data)
                o = []
                for j in mset.actors:
                    if j !="":
                        o.append(j.strip())
                mset.actors = o
                q = []
                for j in mset.director:
                    if j !="":
                        q.append(j.strip())
                mset.director = q
                p = []
                for j in mset.movieStyle:
                    if j !="":
                        p .extend(re.split('[,，。.、与和&]+', j))
                r = [s.strip() for s in p]

                mset.movieStyle = r
                json_line = json.dumps(mset.to_dict(), ensure_ascii=False)
                f.write(json_line+'\n')


#清理名字中多余的东西
def clean1():
    with open('data/movie.json', 'r', encoding='utf-8') as file:
        with open('data/movie-1.json', 'a', encoding='utf-8') as f:
            for line in file:
                data = json.loads(line.strip())  # 逐行读取并解析为 JSON 对象
                mset = dataset.dict_to_movieset(data)
                mset.movieName = re.sub(r'[\[【].*?[\]】]', '', mset.movieName)
                json_line = json.dumps(mset.to_dict(), ensure_ascii=False)
                f.write(json_line+'\n')


#去除非style
stop_words = {"VHS","DVD","Blu-ray","vhs","dvd","blu-ray"}

def clean2():
    with open('data/movie.json', 'r', encoding='utf-8') as file:
        with open('data/movie-1.json', 'a', encoding='utf-8') as f:
            for line in file:
                data = json.loads(line.strip())  # 逐行读取并解析为 JSON 对象
                mset = dataset.dict_to_movieset(data)
                p=[]
                for i in mset.movieStyle:
                    if i not in stop_words:
                        p.append(i)
                mset.movieStyle=p
                json_line = json.dumps(mset.to_dict(), ensure_ascii=False)
                f.write(json_line+'\n')

#抽取一些数据
def clean3():
    with open('data/comments.json', 'r', encoding='utf-8') as file:
         with open('data/example.json', 'a', encoding='utf-8') as f:
            for i,line in enumerate(file):
                data = json.loads(line.strip())  # 逐行读取并解析为 JSON 对象
                mset = dataset.dict_to_movieReview(data)
                jline=json.dumps(mset.to_dict(),ensure_ascii=False)
                f.write(jline+'\n')
                if i >=10000:
                    break

#清理空行
def clean4():

# 输入和输出文件路径
    input_file = 'D:\\myfile\\amazon\\table\\director.csv'
    output_file = 'D:\\myfile\\amazon\\table\\directors.csv'
    
    # 打开原始 CSV 文件并读取数据
    with open(input_file, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        rows = []
        
        # 过滤掉所有列值为空或只有空格的行
        for row in reader:
            if row['DIRECTOR_NAME'].strip() :  # 检查演员和电影ID是否有效
                rows.append(row)

    # 打开输出文件并写入处理后的数据
    with open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()  # 写入表头
        writer.writerows(rows)  # 写入所有行

    print(f"已处理并保存到 {output_file}")


stop_words = {"VHS","DVD","Blu-ray","vhs","dvd","blu-ray"}
#清理空行
def clean5():

# 输入和输出文件路径
    input_file = 'D:\\myfile\\amazon\\table\\directors.csv'
    output_file = 'D:\\myfile\\amazon\\table\\directorss.csv'
    
    # 打开原始 CSV 文件并读取数据
    with open(input_file, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        rows = []
        for row in reader:
            i=row['DIRECTOR_NAME']
            if  len(i)>2:
                rows.append((i.strip(),row['MOVIE_ID'],row['MOVIE_NAME']))

    # 打开输出文件并写入处理后的数据
    with open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['DIRECTOR_NAME', 'MOVIE_ID','MOVIE_NAME'])  # 表头
        writer.writerows(rows) # 写入所有行

    print(f"已处理并保存到 {output_file}")
clean5()