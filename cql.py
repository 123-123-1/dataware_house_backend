import datetime
import re
from neo4j import GraphDatabase
from flask import Flask, jsonify, request

# 设置连接
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "hkf2157871359"))
app = Flask(__name__)

def import_movie_CSV():
    query ="""LOAD CSV WITH HEADERS FROM 'file:///movie.csv' AS row CREATE (m:Movie {
    movie_id: toInteger(row.MOVIE_ID),
    movie_name: row.MOVIE_NAME,
    movie_versions_num: toInteger(row.MOVIE_VERSIONS_NUM),
    movie_release_time: date(row.MOVIE_RELEASE_TIME),
    comment_num: toInteger(row.COMMENT_NUM),
    score: toFloat(row.SCORE)
    })"""
    with driver.session() as session:
        session.run(query)


def import_movie_style_CSV():
    query="""
    create index movie_style_index for (n:Movie_style) on (n.style)
    """
    with driver.session() as session:
        session.run(query)
    query ="""
    LOAD CSV WITH HEADERS FROM 'file:///movie_styles.csv' AS row
    MATCH (m:Movie {movie_id: toInteger(row.MOVIE_ID)})
    Merge (d:Movie_style {style: row.STYLE})
    MERGE (d)-[:MOVIE_STYLE]-(m)
    """
    with driver.session() as session:
        session.run(query)



def import_movie_version_CSV():
    query="""
    create index movie_version_index for (n:Movie_version) on (n.version)
    """
    with driver.session() as session:
        session.run(query)
    query ="""
    LOAD CSV WITH HEADERS FROM 'file:///movie_version.csv' AS row
    MATCH (m:Movie {movie_id: toInteger(row.MOVIE_ID)})
    MERGE (d:Movie_version {version: row.VERSION})
    MERGE (d)-[:MOVIE_VERSION]-(m)
    """
    with driver.session() as session:
        session.run(query)


def import_time_CSV():
    query ="""
    LOAD CSV WITH HEADERS FROM 'file:///time.csv' AS row
    MATCH (m:Movie {movie_id: toInteger(row.MOVIE_ID)})
    MERGE (d:Week {weekday: row.WEEKDAY})
    MERGE (d)-[:WEEKDAY]->(m)
    """
    with driver.session() as session:
        session.run(query)


def import_actor_CSV():
    query="""
    create index actor__name_index for (n:Actor) on (n.actor_name)
    """
    with driver.session() as session:
        session.run(query)
    query ="""
    LOAD CSV WITH HEADERS FROM 'file:///actors.csv' AS row
    MATCH (m:Movie {movie_id: toInteger(row.MOVIE_ID)})
    MERGE (d:Actor {actor_name: row.ACTOR_NAME})
    MERGE (d)-[:ACTED]-(m)
    """
    with driver.session() as session:
        session.run(query)


def import_director_CSV():
    query="""
    create index director__name_index for (n:Director) on (n.director_name)
    """
    with driver.session() as session:
        session.run(query)
    query ="""
    LOAD CSV WITH HEADERS FROM 'file:///directors.csv' AS row
    MATCH (m:Movie {movie_id: toInteger(row.MOVIE_ID)})
    MERGE (d:Director {director_name: row.DIRECTOR_NAME})
    MERGE (d)-[:DIRECTED]-(m)
    """
    with driver.session() as session:
        session.run(query)


#不可行
def import_movie_review_CSV():
    for i in range(0,66):
        file = f"file:///movie_review{i}.csv"
        query ="""
        LOAD CSV WITH HEADERS FROM $file AS row
        MATCH (m:Movie {movie_id: toInteger(row.INDEX)})
        MERGE (d:Movie_review {user_id:row.ID,user_name: row.PROFILENAME,review_asin:row.ASIN,review_time: row.TIME,review_score: toFloat(row.SCORE),review_summary:row.SUMMARY})
        MERGE (d)<-[:MOVIE_REVIEW]-(m)
        """
        with driver.session() as session:
            session.run(query,file = file)
        print(datetime.datetime.now())        
        print(str(i)+"0万条"+"已完成")



def import_movie_review_statistics_CSV():
    query ="""
    LOAD CSV WITH HEADERS FROM 'file:///movie_review_statistics.csv' AS row
    MATCH (m:Movie {movie_id: toInteger(row.MOVIE_ID)})
    SET m.score_three = toInteger(row.THREE)
    SET m.score_four = toInteger(row.FOUR)
    SET m.score_five = toInteger(row.FIVE)
    """
    with driver.session() as session:
        session.run(query)


def create_actor_cooperate():
    query = """
    MATCH (m:Movie)<-[:ACTED]-(a:Actor)
    WITH m, collect(a) AS actors
    WHERE size(actors)>2
    UNWIND range(0, size(actors)-1) AS i
    WITH m, actors[i] AS actor1, actors[CASE WHEN i + 1 < size(actors) THEN i + 1 ELSE 0 END] AS actor2
    WITH m, 
        CASE 
            WHEN actor1.actor_name < actor2.actor_name THEN actor1 
                ELSE actor2 
            END AS actor1_sorted,
        CASE 
            WHEN actor1.actor_name < actor2.actor_name THEN actor2
                ELSE actor1 
            END AS actor2_sorted
        MERGE (actor1_sorted)-[r:ACTED_WITH]->(actor2_sorted)
        ON CREATE SET r.count = 1
        ON MATCH SET r.count = r.count + 1
    """
    with driver.session() as session:
        session.run(query)   

    query="""
    MATCH (m:Movie)<-[:ACTED]-(a:Actor)
    WITH m, collect(a) AS actors
    WHERE size(actors) = 2
    WITH m, actors[0] AS actor1, actors[1] AS actor2
    WITH m, 
        CASE 
            WHEN actor1.actor_name < actor2.actor_name THEN actor1 
            ELSE actor2 
        END AS actor1_sorted,
        CASE 
            WHEN actor1.actor_name < actor2.actor_name THEN actor2
            ELSE actor1 
        END AS actor2_sorted
    MERGE (actor1_sorted)-[r:ACTED_WITH]->(actor2_sorted)
    ON CREATE SET r.count = 1
    ON MATCH SET r.count = r.count + 1
    """ 

    with driver.session() as session:
        session.run(query) 


def create_actor_director():
    query = """
    MATCH (m:Movie)
    MATCH (d:Director)-[:DIRECTED]->(m)
    MATCH (a:Actor)-[:ACTED]->(m)
    MERGE (d)-[r:WORKED_WITH]->(a)
    ON CREATE SET r.count = 1
    ON MATCH SET r.count = r.count + 1
    """
    with driver.session() as session:
        session.run(query) 


def getInfo(p):
    return p.result_consumed_after,p.profile['args']['string-representation']

@app.route('/tasks/<int:year>', methods=['GET'])
def year(year):
    query = """
    PROFILE
    MATCH (m:Movie)
    WHERE m.movie_release_time.year = $year 
    RETURN m.movie_name as n
    """
    with driver.session() as session:
        result = session.run(query, year=year)
        r = [record["n"] for record in result]
        plan = result.consume()
        a,b = getInfo(plan)
        return b

def year_and_month(year,month):
    query = """
    PROFILE
    MATCH (m:Movie)
    WHERE m.movie_release_time.year = $year and m.movie_release_time.month = $month
    RETURN m.movie_name as n
    """
    with driver.session() as session:
        result = session.run(query, year=year,month=month)
        r = [record["n"] for record in result]
        plan = result.consume()
        a,b = getInfo(plan)
        return r,a,b      

    
def year_and_season(year,season):
    query = """
    PROFILE
    MATCH (m:Movie)
    WHERE m.movie_release_time.year = $year and toInteger(((m.movie_release_time.month-1)/3)+1) = $season
    RETURN m.movie_name as n
    """
    with driver.session() as session:
        result = session.run(query, year=year,season = season) 
        r = [record["n"] for record in result]
        plan = result.consume()
        a,b = getInfo(plan)
        return r,a,b    

def weekday(weekday):
    query = """
    PROFILE
    MATCH (m:Week)<-[:WEEKDAY]-(w:Movie)
    WHERE m.weekday = $weekday
    RETURN w.movie_name as n
    """
    with driver.session() as session:
        result = session.run(query, weekday = weekday) 
        r = [record["n"] for record in result]
        plan = result.consume()
        a,b = getInfo(plan)
        return r,a,b    

def name_strict(name):
    query = """
    PROFILE
    MATCH (m:Movie)
    WHERE trim(m.movie_name) = trim($name)
    RETURN m as n
    """
    with driver.session() as session:
        result = session.run(query,name=name) 
        r = [record["n"] for record in result]
        plan = result.consume()
        a,b = getInfo(plan)
        return r,a,b    
    
def name_slack(name):
    query = """
    PROFILE
    MATCH (m:Movie)
    WHERE toLower(trim(m.movie_name)) contains toLower(trim($name))
    RETURN m as n
    """
    with driver.session() as session:
        result = session.run(query,name=name) 
        r = [record["n"] for record in result]
        plan = result.consume()
        a,b = getInfo(plan)
        return r,a,b    
     

def name_version_strict(name):
    query = """
    PROFILE
    MATCH (m:Movie)-[:MOVIE_VERSION]-(w:Movie_version)
    WHERE trim(m.movie_name) = trim($name)
    RETURN w.version as n
    """
    with driver.session() as session:
        result = session.run(query,name=name) 
        r = [record["n"] for record in result]
        plan = result.consume()
        a,b = getInfo(plan)
        return r,a,b    
    
def name_version_slack(name):
    query = """
    PROFILE
    MATCH (m:Movie)-[:MOVIE_VERSION]-(w:Movie_version)
    WHERE toLower(trim(m.movie_name)) contains toLower(trim($name))
    RETURN w.version as n
    """
    with driver.session() as session:
        result = session.run(query,name=name) 
        r = [record["n"] for record in result]
        plan = result.consume()
        a,b = getInfo(plan)
        return r,a,b    
     

def director(name):
    query = """
    PROFILE
    MATCH (m:Movie)-[:DIRECTED]-(w:Director)
    WHERE trim(w.director_name) = trim($name)
    RETURN m.movie_name as n
    """
    with driver.session() as session:
        result = session.run(query,name=name) 
        r = [record["n"] for record in result]
        plan = result.consume()
        a,b = getInfo(plan)
        return r,a,b 

def actor(name):
    query = """
    PROFILE
    MATCH (m:Movie)-[:ACTED]-(w:Actor)
    WHERE trim(w.actor_name) = trim($name)
    RETURN m.movie_name as n
    """
    with driver.session() as session:
        result = session.run(query,name=name) 
        r = [record["n"] for record in result]
        plan = result.consume()
        a,b = getInfo(plan)
        return r,a,b     


def acted_with(num):
    query = """
    PROFILE
    MATCH (m:Actor)-[r:ACTED_WITH]->(n:Actor)
    WHERE r.count>=$num
    RETURN m.actor_name as d,n.actor_name as f
    """
    with driver.session() as session:
        result = session.run(query,num=num) 
        r = [record["d"]+" "+record["f"] for record in result]
        plan = result.consume()
        a,b = getInfo(plan)
        return r,a,b  
    
def worked_with(num):
    query = """
    PROFILE
    MATCH (m:Director)-[r:WORKED_WITH]->(n:Actor)
    WHERE r.count>=$num
    RETURN m.director_name as d,n.actor_name as f
    """
    with driver.session() as session:
        result = session.run(query,num=num) 
        r = [record["d"]+" "+record["f"] for record in result]
        plan = result.consume()
        a,b = getInfo(plan)
        return r,a,b  

def style_strict(style):
    query = """
    PROFILE
    MATCH (m:Movie_style)-[:MOVIE_STYLE]->(n:Movie)
    WHERE m.style = $style
    RETURN n.movie_name as f
    """
    with driver.session() as session:
        result = session.run(query,style=style) 
        r = [record["f"] for record in result]
        plan = result.consume()
        a,b = getInfo(plan)
        return r,a,b  

def style_slack(style):
    query = """
    PROFILE
    MATCH (m:Movie_style)-[:MOVIE_STYLE]->(n:Movie)
    WHERE m.style contains $style
    RETURN n.movie_name as f
    """
    with driver.session() as session:
        result = session.run(query,style=style) 
        r = [record["f"] for record in result]
        plan = result.consume()
        a,b = getInfo(plan)
        return r,a,b  
    
def score(score):
    query = """
    PROFILE
    MATCH (m:Movie)
    WHERE m.score>=$score
    RETURN m.movie_name as f
    """
    with driver.session() as session:
        result = session.run(query,score=score) 
        r = [record["f"] for record in result]
        plan = result.consume()
        a,b = getInfo(plan)
        return r,a,b  

def review_score(num,score):
    if score==3:
        query = """
        PROFILE
        MATCH (m:Movie)
        WHERE m.score_three+m.score_four+m.score_five >=$num
        RETURN 
             m.movie_name as n
        """
    elif score==4:
        query = """
        PROFILE
        MATCH (m:Movie)
        WHERE m.score_four+m.score_five >=$num
        RETURN 
             m.movie_name as n
        """
    elif score==5:
        query = """ 
        PROFILE
        MATCH (m:Movie)
        WHERE m.score_five >=$num
        RETURN 
             m.movie_name as n
        """
    else :
        return [],0,""
    with driver.session() as session:
        result = session.run(query,score=score,num=num) 
        r = [record["n"] for record in result]
        plan = result.consume()
        a,b = getInfo(plan)
        return r,a,b  

@app.route('/', methods=['GET'])
def hello():
    return "2132456"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)