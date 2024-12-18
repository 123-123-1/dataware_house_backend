from extract import extract_from_json

def checknull():
    with open('dags\data\movies.txt', 'r', encoding='utf-8', errors='ignore') as file:
                for i,line in enumerate(file):
                    line = line.strip()  # 去掉行首行尾的空白字符
                # 检查行是否为空，如果为空说明一个评论结束
                    if line :
                       try:
                           if not line.split(":")[1] :
                               with open('dags\data\errormessage.txt','a') as f:
                                    f.write(f"{i} :  {line} \n")
                       except Exception as e :
                            with open('dags\data\errormessage.txt','a') as f:
                                f.write(f"{i} :  {line} \n")

def checkComplete():
    with open('dags\data\movies.txt', 'r', encoding='utf-8', errors='ignore') as file:
           lines = []
           for i,line in enumerate(file):
               line = line.strip()  # 去掉行首行尾的空白字符
                # 检查行是否为空，如果为空说明一个评论结束
               if line :
                  lines.append(line)
               elif lines :
                    if len(lines)< 8 :
                        with open('dags\data\errorLease.txt','a') as f: 
                            f.write(f'\n************{i}***********\n')
                            f.write('\n'.join(lines) + '\n\n')
                           
                    lines = []
           if lines and len(lines) < 8:
                with open('dags/data/errorLease.txt', 'a', encoding='utf-8') as f:
                    f.write('\n'.join(lines) + '\n\n')
                    
                    

def  checkCompletennessRate(path):
    j= 0
    k= 0
    for i in extract_from_json(path):
        if i.movieName:
           j+=1
        k+=1
    return j/k  

def checkReview(path):
    j= 0
    k= 0
    for i in extract_from_json(path):
        p = 0
        for l in i.reviews:
           p+= len(l.text)+ 44
        if p >32767:
            j+= p-32767
        k+=p
    return j/k   
     

if  __name__ =="__main__":
    i=12
    j=18
    while i<=j:
        path = f'data\data{i}-1.json'
        k= checkReview(path)
        print(path+'  的评论截断比例为:  '+str(k))
        i += 1