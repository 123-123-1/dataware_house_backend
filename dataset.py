#电影ID，评论用户ID，评论用户ProfileName，评论用户评价Helpfulness，
# 评论用户Score，评论时间Time，评论结论Summary，评论结论Text，电影上映时间，
# 电影风格，电影导演，电影主演，电影演员，电影版本等信息
import re

'''
class Review:
    def __init__(self, ID="", ProfileName="", helpfulness="", score="", time="", summary="", text=""):
        self.ID = ID  # 用户ID
        self.ProfileName = ProfileName  # 用户名
        self.helpfulness = helpfulness  # 有用投票数
        self.score = score  # 评论分数
        self.time = time  # 评论时间
        self.summary = summary  # 评论摘要
        self.text = text  # 评论正文
    def to_dict(self):
        return {
            "ID": self.ID,
            "ProfileName": self.ProfileName,
            "helpfulness": self.helpfulness,
            "score": self.score,
            "time": self.time,
            "summary": self.summary,
            "text": self.text
        }


class Dataset:
    def __init__(self, movieID="",movieName = "",movieReleaseTime ="",movieStyle = "",director = [],actors = []):
        self.movieID = movieID # 电影ID
        self.movieName = movieName
        self.movieReleaseTime =movieReleaseTime  # 电影上映时间
        self.movieStyle = movieStyle  # 电影类型
        self.director = director  # 导演
        self.actors = actors # 演员列表
        self.reviews = []  
    def add_Review(self,review):
        self.reviews.append(review)
    def to_dict(self):
        return {
            "movieID": self.movieID,
            "movieName": self.movieName,
            "movieReleaseTime": self.movieReleaseTime,
            "movieStyle": self.movieStyle,
            "director": self.director,
            "actors": self.actors,
            "reviews": [review.to_dict() for review in self.reviews]
        }
    

    
def dict_to_dataset(data):
    dataset = Dataset(
        movieID=data.get("movieID", []),
        movieName=data.get("movieName",""),
        movieReleaseTime=data.get( "movieReleaseTime",""),
        movieStyle=data.get("movieStyle",""),
        director= data.get("director",[]),
        actors = data.get("actors",[])
    )

        # 将每个评论字典转换为 Review 对象并添加到 Dataset 中
    reviews = data.get("reviews", [])
    for review_data in reviews:
        review = dict_to_review(review_data)
        dataset.add_Review(review)
    return dataset



def dict_to_review(data):
    return Review(
        ID=data.get("ID", ""),
        ProfileName=data.get("ProfileName", ""),
        helpfulness=data.get("helpfulness", ""),
        score=data.get("score", ""),
        time=data.get("time", ""),
        summary=data.get("summary", ""),
        text=data.get("text", "")
    )
'''
class movieset:
    def __init__(self,  movieID=[], movieName=None, movieReleaseTime=None, 
                 movieStyle=[], director=[], actors=[],commentCount = 0,score = 0.0,index = 0):
        self.movieID = movieID
        self.movieName = movieName
        self.movieReleaseTime = movieReleaseTime
        self.movieStyle = movieStyle 
        self.director =director
        self.actors = actors 
        self.commentCount = commentCount
        self.score = score
        self.index = index
    


    def to_dict(self):
        return {
            "index": self.index,
            "movieID": self.movieID,
            "movieName": self.movieName,
            "movieReleaseTime": self.movieReleaseTime,
            "movieStyle": self.movieStyle,
            "director": self.director,
            "actors": self.actors,
            "commentCount": self.commentCount,
            "score": self.score
        }
    
   
def dict_to_movieset(data):
    return movieset(
        index= data.get("index",0),
        movieID=data.get("movieID", []),
        movieName=data.get("movieName",""),
        movieReleaseTime=data.get( "movieReleaseTime",""),
        movieStyle=data.get("movieStyle",[]),
        director= data.get("director",[]),
        actors = data.get("actors",[]),
        commentCount= data.get("commentCount",0),
        score = data.get("score",0.0)
    )

class movieReview:
    def __init__(self,ID=None, ProfileName=None,asin=None, helpfulness=None, score=0,
                 time=None, summary=None, text=None,index = None):
        self.index = index
        self.ID = ID
        self.ProfileName = ProfileName
        self.helpfulness = helpfulness
        self.score = score
        self.time = time
        self.summary = summary
        self.text = text
        self.asin = asin

    def to_dict(self):
        return {
            "index": self.index,
            "ID": self.ID,
            "asin": self.asin,
            "ProfileName": self.ProfileName,
            "helpfulness": self.helpfulness,
            "score": self.score,
            "time": self.time,
            "summary": self.summary,
            "text": self.text
        }


def dict_to_movieReview(data):
    return movieReview(
        index = data.get("index",""),
        ID=data.get("ID", ""),
        asin = data.get("asin",""),
        ProfileName=data.get("ProfileName", ""),
        helpfulness=data.get("helpfulness", ""),
        score=data.get("score", ""),
        time=data.get("time", ""),
        summary=data.get("summary", ""),
        text=data.get("text", "")
    )