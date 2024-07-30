import pandas as pd
import re
import konlpy
import math
import numpy as np
from tqdm import tqdm
from janome.tokenizer import Tokenizer
from pymongo import MongoClient

okt = konlpy.tag.Okt()

client = MongoClient(host="1.234.51.110", port=38019, username='clawling', password='goodtime**95')
db = client['OneAsia2024']

collection = db['youtube_oneasia_crawling_japanese_year']
data = list(collection.find({}, {"_id" : 1, "유튜브": 1, "유튜브언급량" : 1, "키워드" : 1}))    # 필요한 데이터의 내용

# Janome 토크나이저를 초기화합니다.
tokenizer = Tokenizer()

# 감성 분석을 위해 단어 사전을 불러옵니다.
sent_dic = {}
with open('SentimentLookupTable.txt', 'r', encoding='utf-8') as file:
    for line in file:
        word, score = line.split('\t')
        sent_dic[word] = float(score)

# 정규 표현식을 사용하여 지정된 범위에 포함되지 않는 일본어외의 문자를 제거합니다.
def text_preprocess(x):
    return re.sub(r'[^\u3040-\u309F\u30A0-\u30FF\uFF66-\uFF9Fぁ-ゖ゛-ゞァ-ヶヽヾ一-龠々〆ヵヶ0-9a-zA-Z\s]', '', x)

# 단어 토큰화 함수
def tokenize(x):
    words = []
    for token in tokenizer.tokenize(x):
        # 명사, 동사, 형용사만 추출
        if token.part_of_speech.startswith('名詞') or token.part_of_speech.startswith('動詞') or token.part_of_speech.startswith('形容詞'):
            words.append(token.surface)
    return words

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
        score = sent_dic.get(word, None)
        if score is not None:
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

      # dictionary에 저장해줍니다.
      detail['긍정단어'] = np.array(긍정단어)                       # mongoDB에 올려주기위해 배열로 변경
      detail['부정단어'] = np.array(부정단어)                       # mongoDB에 올려주기위해 배열로 변경
      detail['총합점수'] = float(total_score)

      # mongoDB에 저장해줍니다.
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