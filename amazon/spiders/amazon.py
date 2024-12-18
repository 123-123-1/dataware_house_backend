import re
from bs4 import BeautifulSoup
import scrapy

import json
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time

from amazon.extract import extract_from_json

propath = 'D:/myfile/amazon/'

class AmazonSpider(scrapy.Spider):
    name = "amazon"
    allowed_domains = ["amazon.com"]
    start_urls = ["https://amazon.com"]


    def __init__(self):
        # 配置 Chrome 选项
        chrome_options = Options()
        #chrome_options.add_argument("--headless")  # 无头模式
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        self.driver = webdriver.Chrome( options=chrome_options) 

    def start_requests(self):
        # 商品ID列表
        i = 18
        j = 18

        n = 0
        while i <= j:
            #填写原数据位置
            k = 0
            filePath = propath+f'data/data{i}.json'
                #填写数据存储位置
            savePath = propath+f'data/data{i}-1.json'
            for k, item in enumerate(extract_from_json(filePath)):
                if k<n:
                    continue
                print('第'+ str(k) +'部')
                if not item.movieName:
                    url = 'https://www.amazon.com/dp/'+item.movieID
                    yield scrapy.Request(url=url, callback=self.parse, meta={'item':item,'savePath':savePath})
                else :
                    print('已经爬过了')
                    with open(savePath, 'a', encoding='utf-8') as file:
                        json_line = json.dumps(item.to_dict(), ensure_ascii=False)
                        file.write(json_line+'\n')
            i+=1
            print(filePath+"全部下载完毕!")

    
    def parse(self, response):
        retry = 5
        self.driver.get(response.url)  # 访问页面
        time.sleep(3) 
        while True:
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            movie_title = str(soup.select('title')[0].get_text())
            if movie_title not in ["Amazon.com", "Sorry! Something went wrong!", 'Robot Check']:
               break
            retry -= 1
            if retry  <= 0 :
                print('失败')
                with open(response.meta['savePath'], 'a', encoding='utf-8') as file:
                    json_line = json.dumps(response.meta['item'].to_dict(), ensure_ascii=False)
                    file.write(json_line+'\n')
                return
            self.driver.refresh()

       
        item = response.meta['item']

        data = soup.select('#imdbInfo_feature_div > span > i')
        emptyData = soup.select('#cannotbefound')  # 创造一个空的data

        if data != emptyData:  # 判断是不是电影
            print('成功')
            try:
                name = soup.select_one("#title span").get_text(strip=True)

                item.movieName = name.strip()
                ############
                ul_element = soup.select_one("#detailBullets_feature_div > ul")
                # 获取所有 li 元素
                if ul_element:
                    li_elements = ul_element.find_all('li')

                    for li in li_elements:
                        # 选择标签和对应的值
                        label = li.select_one('span > span:nth-of-type(1)').get_text(strip=True)
                        value = li.select_one('span > span:nth-of-type(2)').get_text(strip=True)

                        # 清理 label
                        if label:
                            label = re.sub(r'[\u200e\u200f\s]+', ' ', label).strip()
                            print(label + " ##### " + value)
                            # 根据标签类型填充 Dataset 实例
                            if label in ['Director :','导演']:

                                item.director.append(value.strip())
                            elif label in ['Actors :','演员']:
                                item.actors.append(value.strip())
                            elif label in ['Date First Available :', 'Release date :','发布日期','上架日期']:
                                item.movieReleaseTime = value.strip()
                    # style（风格）
                styledata = soup.select("#wayfinding-breadcrumbs_feature_div > ul > li > span > a")
                if styledata:
                    item.movieStyle = styledata[-1].get_text().strip()

            finally:
                with open(response.meta['savePath'], 'a', encoding='utf-8') as file:
                    json_line = json.dumps(item.to_dict(), ensure_ascii=False)
                    file.write(json_line + '\n')
        else:
            title = soup.select_one('h1[data-automation-id="title"]')
            if title:
                print(title.get_text(strip=True)+"###########")  
                try:
                    item.movieName = title.get_text(strip=True)
                    #风格
                    genres = soup.select('div[data-testid="genresMetadata"] a')
                    # 提取所有文本
                    if genres:
                        genres_text = [genre.get_text() for genre in genres]
                        all_text = ','.join(genres_text)  # 使用分隔符连接
                        print(all_text+"########")
                        item.movieStyle = all_text.strip()

                    #详情
                    # 找到所有的dl元素
                    metadata_rows = soup.select('dl[data-testid="metadata-row"]')
                    # 存储名字的列表
                    directors_names = []
                    cast_names = []
                    if metadata_rows:
                        # 提取Directors和Cast后的文本
                        for row in metadata_rows:
                            label = row.dt.get_text(strip=True)
                            names = row.dd.select('a')
                            names_list = [name.get_text(strip=True) for name in names]

                            if label in ['Directors','导演']:
                                directors_names.extend(names_list)
                            elif label in ['Cast','演员']:
                                cast_names.extend(names_list)
                        # 用逗号连接名字
                        directors_result = ','.join(directors_names)
                        cast_result = ','.join(cast_names)
                        item.director.append(directors_result)
                        item.actors.append(cast_result)
                        print(directors_result + "########"+cast_result)

                finally:
                    with open(response.meta['savePath'], 'a', encoding='utf-8') as file:
                        json_line = json.dumps(item.to_dict(), ensure_ascii=False)
                        file.write(json_line + '\n')
            else:
                print('不是电影')
                return

    
    def close(self,reason):
        self.driver.close()
