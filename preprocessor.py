import re
import numpy as np
from konlpy.tag import Okt


def preprocess_text(text):
    if not isinstance(text, str):
        return ""
    # 해시태그 삭제
    text = re.sub(r'#(\w+)', r'\1', text)
    # 멘션 삭제
    text = re.sub(r'@\w+', '', text)
    # 개행 문자 삭제
    text = re.sub(r'\n+', ' ', text)

    # 이메일을 빈 문자열로 대체
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
    text = re.sub(email_pattern, '', text)

    # URL을 빈 문자열로 대체
    url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    text = re.sub(url_pattern, '', text)

    # 특수 문자와 숫자를 빈 문자열로 대체 (한글, 영문자, 공백 제외)
    special_chars_and_numbers_pattern = r'[^A-Za-z\uAC00-\uD7AF\s]+'
    text = re.sub(special_chars_and_numbers_pattern, '', text)
    return text


def tokenize_text(content):
    tokens = Okt().morphs(content)
    return tokens


def get_vector(model, text):
    tokens = tokenize_text(preprocess_text(text))
    word_vectors = [model.wv[token]
                    for token in tokens if token in model.wv]

    if not word_vectors:  # 만약 벡터가 없다면
        return np.zeros(model.vector_size)  # 영벡터 반환

    sentence_vector = np.mean(word_vectors, axis=0)
    return sentence_vector


def cosine_similarity(vec1, vec2):
    # 두 벡터의 내적
    dot_product = np.dot(vec1, vec2)

    # 벡터의 크기 (L2 Norm)
    norm_vec1 = np.linalg.norm(vec1)
    norm_vec2 = np.linalg.norm(vec2)

    # 코사인 유사도 계산
    if norm_vec1 == 0 or norm_vec2 == 0:
        # 벡터 중 하나라도 영벡터인 경우 유사도를 0으로 설정
        return 0.0
    cosine_sim = dot_product / (norm_vec1 * norm_vec2)

    return cosine_sim
