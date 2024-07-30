# 필요한 라이브러리 가져오기
from janome.tokenizer import Tokenizer  # 일본어 형태소 분석기 라이브러리
from wordcloud import WordCloud
from collections import Counter  # 카운트 라이브러리
import pymongo  # 파이썬에서 MongoDB 다루기 위한 라이브러리
import os  # OS 라이브러리
import matplotlib.pyplot as plt
from tqdm import tqdm
import re

# MongoDB 서버에 접속해서 필요한 정보를 불러옵니다.
client = pymongo.MongoClient(host="1.234.51.110", port=38019, username='clawling', password='goodtime**95')
db = client['OneAsia2024']

# 데이터 불러오기
collection = db['youtube_oneasia_crawling_japanese_year']
data = list(collection.find({"기준년": "2022"}, {"_id": 1, "유튜브": 1}))

# Janome의 Tokenizer 초기화
tokenizer = Tokenizer()

# 일본어 텍스트 처리를 위한 불용어 리스트 생성
stopwords = ["あり", '釜山', 'ワン', 'アジア', 'フェスティバル', '悠太', '嗨的悠', '啊越跳']  

japanese_pattern = re.compile(r'[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9faf\uF900-\uFAFF]+')

# jpg파일을 저장할 디렉토리 명
output_directory = 'C:/Users/123/Desktop/손현정/워드클라우드'

os.makedirs(output_directory, exist_ok=True)

# _id별로 워드 클라우드 생성
for document in tqdm(data):
    keyid = str(document.get('_id'))

    # 이미지 파일 경로 설정
    cloudimage = os.path.join(output_directory, f'{keyid.replace("/", "_")}.jpg')

    # 이미지 파일이 이미 존재하는지 확인
    if os.path.exists(cloudimage):
        continue  # 이미지 파일이 있으면 다음으로 건너뜀

    youtube_videos = document.get("유튜브", [])

    text1 = ' '.join([
            f"{youtube['제목']} {youtube['설명']} {' '.join([comment['댓글 내용'] for comment in youtube.get('댓글', []) if '댓글 내용' in comment])}" 
            for youtube in youtube_videos
        ])

    # 일본어 이외의 문자를 제거
    text1 = ' '.join(re.findall(japanese_pattern, text1))

    if not text1.strip():
        print(f"No Japanese text found for document ID {keyid}")
        continue  # 텍스트가 비어 있으면 다음으로 건너뜀

    # 형태소로 나눔
    tokens = [token.surface for token in tokenizer.tokenize(text1) if len(token.surface) > 1]

    # 불용어 리스트를 이용해 불용어 제거
    tokens = [word for word in tokens if word not in stopwords]

    if not tokens:
        print(f"No tokens found for document ID {keyid}")
        continue  # 토큰이 비어 있으면 다음으로 건너뜀

    # 각 단어의 횟수를 세고 저장
    word_counts = Counter(tokens)

    if not word_counts:
        print(f"No word counts found for document ID {keyid}")
        continue  # 워드 카운트가 비어 있으면 다음으로 건너뜀

    # 결과를 타입에 맞게 MongoDB에 추가하기 위해 리스트의 딕셔너리로 변환
    word_counts_list = [{"word": word, "count": count} for word, count in word_counts.items()]

    # word_counts_list를 count가 큰 순서대로 정렬
    word_counts_list.sort(key=lambda x: x["count"], reverse=True)

    # 워드 클라우드 생성
    wordcloud = WordCloud(font_path="C:/Users/123/AppData/Local/Microsoft/Windows/Fonts/DelaGothicOne-Regular.ttf",
                          width=2000, height=1000, background_color='white').generate_from_frequencies(word_counts)

    # 워드 클라우드 시각화
    plt.figure(figsize=(11, 7))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.title(f'워드 클라우드 - {keyid}', fontsize=16)
    plt.axis('off')

    # 워드클라우드를 jpg 파일로 저장
    wordcloud.to_file(cloudimage)

    # 파일 경로+파일이름 에서 파일 이름만 추출
    cloudimage_filename = os.path.basename(cloudimage)

    # MongoDB에 "wordcloud_image" 필드를 만들고 워드클라우드 이미지 파일명을 저장
    collection.update_one({"_id": document["_id"]}, {"$set": {"wordcloud_image": cloudimage_filename}})

    # MongoDB에 "wordcounts" 필드를 만들고 각 _id에 해당하는 word_counts_list를 저장
    collection.update_one({"_id": document["_id"]}, {"$set": {"wordcounts": word_counts_list}})

    # 플롯 닫기
    plt.close()

client.close()
print("워드 카운트 리스트와 이미지 파일명이 MongoDB에 추가되었습니다.")
