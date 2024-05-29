import re
import time
import pandas as pd
import numpy as np
from konlpy.tag import Okt
from connector import conn


class PostRecommender:
    # 텍스트 전처리 함수
    def preprocess_text(self, text):
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

    def tokenize_text(self, content):
        tokens = Okt().morphs(content)
        return tokens

    def get_vector(self, model, text):
        tokens = self.tokenize_text(self.preprocess_text(text))
        word_vectors = [model.wv[token]
                        for token in tokens if token in model.wv]

        if not word_vectors:  # 만약 벡터가 없다면
            return np.zeros(model.vector_size)  # 영벡터 반환

        sentence_vector = np.mean(word_vectors, axis=0)
        return sentence_vector

    def cosine_similarity(self, vec1, vec2):
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

    def get_post_recommendations(self, model, post_like, posts, member_info, page_num, page_size=10):
        # 좋아요 표시한 게시글이 없을 경우
        # 자신의 직무 또는 관심 분야를 설정하지 않은 경우

        # redis에 탐색 후 이미 반환했던 게시글 제거

        # Extract vectors from posts and store them in a data of tuples (post_id, vector)
        # Convert lists of tuples to DataFrames for easier manipulation
        post_like_vectors = [(post['id'], self.get_vector(
            model, post['content'])) for _, post in post_like.iterrows()]
        post_data_vectors = [(post['id'], self.get_vector(
            model, post['content'])) for _, post in posts.iterrows()]

        post_like_df = pd.DataFrame(
            post_like_vectors, columns=['id', 'vector'])
        post_data_df = pd.DataFrame(
            post_data_vectors, columns=['id', 'vector'])

        # 코사인 유사도 계산
        similarity_scores = []
        for _, post_row in post_data_df.iterrows():
            post_id = post_row['id']
            post_vector = post_row['vector']
            for _, like_row in post_like_df.iterrows():
                like_vector = like_row['vector']
                similarity = self.cosine_similarity(like_vector, post_vector)
                similarity = max(similarity, 0)
                similarity_scores.append((post_id, similarity))

        # 각 게시글들의 유사도 값을 모두 더하고, 평균
        similarity_df = pd.DataFrame(similarity_scores, columns=[
                                     'post_id', 'similarity'])
        similarity_mean = similarity_df.groupby('post_id')['similarity'].agg([
            'sum', 'mean']).reset_index()

        # similarity_scores를 유사도 값의 평균 기준으로 정렬
        sorted_by_mean = similarity_mean.sort_values(
            by='mean', ascending=False)

        # 유사도 값에 비례한 확률을 사용해 무작위로 게시글 선택
        post_ids = sorted_by_mean['post_id'].values
        similarity_sums = sorted_by_mean['mean'].values
        probabilities = similarity_sums / similarity_sums.sum()

        selected_posts = np.random.choice(
            post_ids, size=page_size, replace=False, p=probabilities)

        return selected_posts


post_recommender = PostRecommender()
