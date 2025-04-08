from collections import Counter
import os
import matplotlib.pyplot as plt
from tqdm import tqdm
import pymongo
import jieba
from wordcloud import WordCloud
import re

# MongoDB 서버에 접속해서 필요한 정보를 불러옵니다.
client = pymongo.MongoClient(host="", port=, username='', password='')
db = client['OneAsia2024']

# 데이터 불러오기
collection = db['baidu_oneasia_crawling_chinese_year']
data = list(collection.find({"기준년": "2022"}, {"_id": 1, "바이두": 1}))

# 중국어 텍스트 처리를 위한 불용어 리스트 생성
stopwords = ['釜山', '亚洲', '艺术节', '中字', '脱单']

# jpg파일을 저장할 디렉토리 명
output_directory = 'C:/Users/123/Desktop/손현정/워드클라우드'

os.makedirs(output_directory, exist_ok=True)

# _id별로 워드 클라우드 생성
for document in tqdm(data):
    keyid = str(document.get('_id'))

    cloudimage = os.path.join(output_directory, f'{keyid.replace("/", "_")}.jpg')

    if os.path.exists(cloudimage):
        continue

    youtube_videos = document.get("바이두", [])

    # 형태소 단위로 토큰화된 텍스트 저장할 리스트
    tokens = []

    for youtube in youtube_videos:
        # 제목
        title_tokens = jieba.lcut(youtube.get('제목', ''))
        tokens.extend(title_tokens)
        
        # 설명
        desc_tokens = jieba.lcut(youtube.get('설명', ''))
        tokens.extend(desc_tokens)
        
        # 댓글 내용
        comments = youtube.get('댓글', [])
        for comment in comments:
            comment_tokens = jieba.lcut(comment.get('댓글 내용', ''))
            tokens.extend(comment_tokens)

    # 단어 길이가 1보다 큰 것만 선택
    tokens = [token for token in tokens if len(token) > 1]

    # 불용어 제거
    tokens = [word for word in tokens if word not in stopwords]

    # 각 단어의 빈도 계산
    word_counts = Counter(tokens)

    word_counts_list = [{"word": word, "count": count} for word, count in word_counts.items()]

    word_counts_list.sort(key=lambda x: x["count"], reverse=True)

    # MongoDB에 워드 카운트 리스트 저장
    collection.update_one({"_id": document["_id"]}, {"$set": {"wordcounts": word_counts_list}})

    if tokens:
        # 중국어 정규식 패턴
        chinese_pattern = re.compile(r'[\u4e00-\u9fff]+')
        chinese_tokens = []

    # 중국어만 추출
    for token in tokens:
        if chinese_pattern.match(token):
            chinese_tokens.append(token)

    # 중국어 토큰이 있는 경우에만 처리
    if chinese_tokens:
        # 워드 카운트를 위한 중국어 토큰 생성
        chinese_word_counts = Counter(chinese_tokens)

        chinese_word_counts_list = [{"word": word, "count": count} for word, count in chinese_word_counts.items()]

        chinese_word_counts_list.sort(key=lambda x: x["count"], reverse=True)

        # MongoDB에 워드 카운트 정보 저장
        collection.update_one({"_id": document["_id"]}, {"$set": {"wordcounts": chinese_word_counts_list}})

        # 중국어 토큰을 사용하여 워드 클라우드 생성
        text = " ".join(chinese_tokens)
        wordcloud = WordCloud(font_path="C:/Users/123/AppData/Local/Microsoft/Windows/Fonts/Microsoft-YaHei-Bold.ttf",
                              width=2000, height=1000, background_color='white', margin=0).generate(text)  

        plt.figure(figsize=(20, 10))  # 이미지 크기를 조정합니다.
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')

        # 이미지 저장
        plt.savefig(cloudimage, bbox_inches='tight')

        cloudimage_filename = os.path.basename(cloudimage)

        collection.update_one({"_id": document["_id"]}, {"$set": {"wordcloud_image": cloudimage_filename}})

    else:
        # 중국어 토큰이 없는 경우 처리
        word_counts_list = [{"word": " ", "count": 0}]
        collection.update_one({"_id": document["_id"]}, {"$set": {"wordcounts": word_counts_list}})

    plt.close()

client.close()
print("워드 카운트 리스트와 이미지 파일명이 MongoDB에 추가되었습니다.")
