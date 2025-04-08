import pandas as pd
import re
import konlpy
import math
import numpy as np
from tqdm import tqdm
from pymongo import MongoClient

# 형태소 분석기 사용을 위해 미리 선언해 줍니다.
okt = konlpy.tag.Okt()

#mongoDB 서버에 접속해서 필요한 정보를 불러옵니다.
client = MongoClient(host="", port=, username='', password='')
db = client['OneAsia2024']

# 데이터 불러오기
collection = db['youtube_oneasia_crawling_korean_year']
data = list(collection.find({}, {"_id" : 1, "유튜브": 1, "유튜브언급량" : 1, "키워드" : 1}))    # 필요한 데이터의 내용

# 감성분석에 활용할 단어 사전을 불러옵니다.
sent_dic = pd.read_csv('SentiWord_Dict.txt',sep = '\t',header=None)

# 함수를 작성합니다.
# 한글을 정규화 해줍니다.
def text_preprocess(x):
    text=[]
    a = re.sub('[^가-힣0-9a-zA-Z\\s]', '',x)
    for j in a.split():
        text.append(j)
    return ' '.join(text)

# 정규화해준 단어를 토큰화 해줍니다.
def tokenize(x):
    text = []
    tokens = okt.pos(x)
    for token in tokens :
        if token[1] == 'Adjective' or token[1]=='Adverb' or token[1] == 'Determiner' or token[1] == 'Noun' or token[1] == 'Verb' or 'Unknown':
            text.append(token[0])
    return text

sum_score = 0

# 감성분석을 시작합니다.
for detail in tqdm(data):
      preprocess_title = ''
      preprocess_summary = ''
      preprocess_comment = ''
      positive_score = 0
      negative_score = 0
      total_score = 0
      긍정단어 = []
      부정단어 = []
      
      # 필요한 데이터를 정규화해줍니다.
      for youtube in detail['유튜브']:
         preprocess_title += text_preprocess(str(youtube['제목']))
         preprocess_summary += text_preprocess(str(youtube['설명']))
         for comment in youtube['댓글']:
            preprocess_comment += text_preprocess(str(comment['댓글 내용'])) 

      #정규화한 데이터를 토큰화해줍니다.
      words = tokenize(str(preprocess_summary)) + tokenize(str(preprocess_title)) + tokenize(str(preprocess_comment))

      # 토큰화한 데이터의 감성점수를 매기고 각 분류에 맞게 list에 넣어줍니다.
      for word in words:
         target = sent_dic[sent_dic[0]==word]
         if len(target)==1:
            score = float(target[1])
            if score > 0:
               긍정단어.append(word)
               positive_score += score
            elif score < 0:
               부정단어.append(word)
               negative_score += score

      total_count = detail['유튜브언급량']
      if total_count != 0:
         total_score = (negative_score + positive_score) / total_count
      else:
         total_score = 0

      detail['긍정단어'] = np.array(긍정단어)                       # mongoDB에 올려주기위해 배열로 변경
      detail['부정단어'] = np.array(부정단어)                       # mongoDB에 올려주기위해 배열로 변경
      detail['총합점수'] = float(total_score)

      # mongoDB에 저장
      collection.update_one({"_id": detail["_id"]}, {"$set": {"긍정단어": detail['긍정단어'].tolist()}})
      collection.update_one({"_id": detail["_id"]}, {"$set": {"부정단어": detail['부정단어'].tolist()}})

      sum_score += total_score

var_score = 0     # 분산 점수 구하기 위해 선언

mean_score  = sum_score / len(data)    # 평균점수

# 분산점수
for detail in tqdm(data):
         var_score += (detail['총합점수'] - mean_score)**2
var_score = var_score / len(data)

std_score = math.sqrt(var_score)    # 표준편차

for detail in tqdm(data):
      final_score = (detail['총합점수'] - mean_score) / std_score    # 표준점수
      # 표준점수가 5이상이면 5, -5 이하면 -5가 되게 해줍니다.
      if final_score >= 5:
         final_score = 5
      elif final_score <= -5:
         final_score = -5

      percent_score = (final_score / 5 * 100 + 100) / 2     # 표준점수(final_score)가 -5 ~ 5 사이로 나와서 그에 맞게 백분율 공식을 만들었다.
      detail['표준점수'] = final_score
      detail['표준점수(백분율)'] = round(float(percent_score), 2)
      collection.update_one({"_id": detail["_id"]}, {"$set": {"감성점수": detail['표준점수(백분율)']}})

client.close()

sum_score   # 전체 합
var_score   # 분산
std_score   # 표준편차
mean_score  # 평균
