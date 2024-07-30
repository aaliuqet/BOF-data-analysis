import re
import math
import numpy as np
from tqdm import tqdm
from pymongo import MongoClient
from snownlp import SnowNLP
import jieba

# MongoDB 서버에 접속해서 필요한 정보를 불러옵니다.
client = MongoClient(host="1.234.51.110", port=38019, username='clawling', password='goodtime**95')
db = client['OneAsia2024']

# 데이터 불러오기
collection = db['baidu_oneasia_crawling_chinese_copy1']
data = list(collection.find({}, {"_id": 1, "바이두": 1, "바이두언급량": 1, "키워드": 1}))

# 감성분석에 활용할 단어 사전을 불러옵니다.
positive_words = set()
negative_words = set()
with open('正面詞無重複_9365詞.txt', 'r', encoding='utf-8') as f:
    for line in f:
        positive_words.add(line.strip())
with open('負面詞無重複_11230詞.txt', 'r', encoding='utf-8') as f:
    for line in f:
        negative_words.add(line.strip())

# 함수를 작성합니다.
# 중국어 텍스트를 정규화해줍니다.
def text_preprocess(x):
    a = re.sub('[^\u4e00-\u9fff0-9a-zA-Z\\s]', '', x)  # 한자를 포함한 정규식
    return a

sum_score = 0

# 감성분석을 시작합니다.
for detail in tqdm(data):
    # 필요한 데이터의 null 값을 선언해줍니다.
    preprocess_title = ''
    preprocess_summary = ''
    preprocess_comment = ''
    positive_score = 0
    negative_score = 0
    total_score = 0
    긍정단어 = []
    부정단어 = []

    # 필요한 데이터를 정규화해줍니다.
    for youtube in detail['바이두']:
        preprocess_title += text_preprocess(str(youtube['제목']))
        preprocess_summary += text_preprocess(str(youtube['설명']))
        for comment in youtube['댓글']:
            preprocess_comment += text_preprocess(str(comment['댓글 내용']))

    # 정규화한 데이터를 합칩니다.
    combined_text = preprocess_summary + preprocess_title + preprocess_comment

    if combined_text.strip():  # 비어 있지 않을 경우
        # SnowNLP를 이용하여 감성분석을 합니다.
        s = SnowNLP(combined_text)
        sentiment_score = s.sentiments  # 0 ~ 1 사이의 값, 0에 가까울수록 부정적, 1에 가까울수록 긍정적

        # 단어별로 감성분석을 수행합니다.
        words = jieba.lcut(combined_text)
        for word in words:
            if word in positive_words:
                긍정단어.append(word)
                positive_score += 1
            elif word in negative_words:
                부정단어.append(word)
                negative_score -= 1

    else:  # 비어 있을 경우
        sentiment_score = 0.5  # 중립적인 점수
        positive_score = 0
        negative_score = 0

    total_count = detail['바이두언급량']
    if total_count != 0:
        total_score = (negative_score + positive_score) / total_count
    else:
        total_score = 0

    # dictionary에 저장해줍니다.
    detail['긍정단어'] = np.array(긍정단어)  # MongoDB에 올려주기 위해 배열로 변경
    detail['부정단어'] = np.array(부정단어)  # MongoDB에 올려주기 위해 배열로 변경
    detail['총합점수'] = float(total_score)

    # MongoDB에 저장해줍니다.
    collection.update_one({"_id": detail["_id"]}, {"$set": {"긍정단어": detail['긍정단어'].tolist()}})
    collection.update_one({"_id": detail["_id"]}, {"$set": {"부정단어": detail['부정단어'].tolist()}})

    sum_score += total_score

var_score = 0  # 분산 점수 구하기 위해 선언

mean_score = sum_score / len(data)  # 평균점수

# 분산점수
for detail in tqdm(data):
    var_score += (detail['총합점수'] - mean_score) ** 2
var_score = var_score / len(data)

std_score = math.sqrt(var_score)  # 표준편차

for detail in tqdm(data):
    final_score = (detail['총합점수'] - mean_score) / std_score  # 표준점수
    # 표준점수가 5 이상이면 5, -5 이하면 -5가 되게 해줍니다.
    if final_score >= 5:
        final_score = 5
    elif final_score <= -5:
        final_score = -5

    percent_score = (final_score / 5 * 100 + 100) / 2  # 표준점수(final_score)가 -5 ~ 5 사이로 나와서 그에 맞게 백분율 공식을 만들었다.
    detail['표준점수'] = final_score
    detail['표준점수(백분율)'] = round(float(percent_score), 2)
    collection.update_one({"_id": detail["_id"]}, {"$set": {"감성점수": detail['표준점수(백분율)']}})

client.close()

sum_score  # 전체 합
var_score  # 분산
std_score  # 표준편차
mean_score  # 평균