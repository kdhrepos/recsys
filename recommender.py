import pandas as pd
import numpy as np
from preprocessor import preprocess_text, tokenize_text, get_vector, cosine_similarity


def get_post_recommendations(model, post_like, posts, member_info, page_num, page_size=10):
    # 좋아요 표시한 게시글이 없을 경우
    # 자신의 직무 또는 관심 분야를 설정하지 않은 경우

    # redis에 탐색 후 이미 반환했던 게시글 제거

    # Extract vectors from posts and store them in a data of tuples (post_id, vector)
    # Convert lists of tuples to DataFrames for easier manipulation
    post_like_vectors = [(post['id'], get_vector(
        model, post['content'])) for _, post in post_like.iterrows()]
    post_data_vectors = [(post['id'], get_vector(
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
            similarity = cosine_similarity(like_vector, post_vector)
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


def get_hashtag_recommendations(self, model, post_like, posts, member_info, page_num, page_size=10):
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
