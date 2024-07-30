# 필요한 라이브러리 가져오기
from konlpy.tag import Okt  # 형태소 분석기 라이브러리
from wordcloud import WordCloud
from collections import Counter  # 카운트 라이브러리
import pymongo  # 파이썬에서 MongoDB 다루기 위한 라이브러리
import os  # OS 라이브러리
import matplotlib.pyplot as plt
from tqdm import tqdm

# MongoDB 서버에 접속해서 필요한 정보를 불러옵니다.
client = pymongo.MongoClient(host="1.234.51.110", port=38019, username='clawling', password='goodtime**95')
db = client['OneAsia2024']

# 데이터 불러오기
collection = db['youtube_oneasia_crawling_korean_year']
data = list(collection.find({"기준년": "2023"}, {"_id": 1, "유튜브": 1}))

# KoNLPy의 Okt 형태소 분석기 초기화
okt = Okt()

# 한국어 텍스트 처리를 위한 불용어 리스트 생성
stopwords = ['부산', '방문', '여기', '우리', '생각']

# jpg파일을 저장할 디렉토리 명
output_directory = 'C:/Users/123/Desktop/손현정/워드클라우드'   

#디렉토리를 생성하고 이미 존재해도 오류 없게끔
os.makedirs(output_directory, exist_ok=True)

# _id별로 워드 클라우드 생성
for document in tqdm(data):
    keyid = str(document.get('_id'))

    # 이미지 파일 경로 설정
    cloudimage = os.path.join(output_directory, f'{keyid.replace("/", "_")}.jpg')

    # 이미지 파일이 이미 존재하는지 확인
    if os.path.exists(cloudimage):
        continue  # 이미지 파일이 있으면 다음으로 건너뜀

    # 유튜브 데이터를 가져옴.
    youtube_videos = document.get("유튜브", [])

    # 유튜브 제목과 설명, 댓글 내용을 가져와서 text1으로 합침.
    text1 = ' '.join([
            f"{youtube['제목']} {youtube['설명']} {' '.join([comment['댓글 내용'] for comment in youtube.get('댓글', []) if '댓글 내용' in comment])}" 
            for youtube in youtube_videos
        ])

    # 두 텍스트를 합쳐서 형태소로 나눔
    tokens = okt.nouns(text1)

    # 형태소의 길이가 1보다 큰 것만 선택
    tokens = [token for token in tokens if len(token) > 1]

    # 불용어 리스트를 이용해 불용어 제거
    tokens = [word for word in tokens if word not in stopwords]

    # 각 단어의 횟수를 세고 저장
    word_counts = Counter(tokens)

    # 결과를 타입에 맞게 MongoDB에 추가하기 위해 리스트의 딕셔너리로 변환
    word_counts_list = [{"word": word, "count": count} for word, count in word_counts.items()]

    # word_counts_list를 count가 큰 순서대로 정렬
    word_counts_list.sort(key=lambda x: x["count"], reverse=True)

    # 토큰에 단어가 있는 경우
    if tokens:
        # 워드 클라우드 생성
        wordcloud = WordCloud(font_path="C:/Users/123/AppData/Local/Microsoft/Windows/Fonts/비트로코어TTF.ttf",
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

    # 토큰에 단어가 없는 경우
    else:
        word_counts_list = [{"word": " ", "count": 0}]
        collection.update_one({"_id": document["_id"]}, {"$set": {"wordcloud_image": " "}})

    # MongoDB에 "wordcounts" 필드를 만들고 각 _id에 해당하는 word_counts_list를 저장
    collection.update_one({"_id": document["_id"]}, {"$set": {"wordcounts": word_counts_list}})

    # 플롯 닫기
    plt.close()
client.close()
print("워드 카운트 리스트와 이미지 파일명이 MongoDB에 추가되었습니다.")