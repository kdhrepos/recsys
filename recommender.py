import pandas as pd
import numpy as np
from preprocessor import get_vector, cosine_similarity


def get_post_recommendations(model, post_like, post_list, member_info, page_num, page_size=10):
    try:
        # 좋아요 표시한 게시글이 없을 경우
        # 자신의 직무 또는 관심 분야를 설정하지 않은 경우

        # Extract vectors from posts and store them in a data of tuples (post_id, vector)
        # Convert lists of tuples to DataFrames for easier manipulation
        post_like_vectors = [(post['id'], get_vector(
            model, post['content'])) for _, post in post_like.iterrows()]
        post_list_vectors = [(post['id'], get_vector(
            model, post['content'])) for _, post in post_list.iterrows()]

        post_like_df = pd.DataFrame(
            post_like_vectors, columns=['id', 'vector'])
        post_list_df = pd.DataFrame(
            post_list_vectors, columns=['id', 'vector'])

        # 코사인 유사도 계산
        similarity_scores = []
        for _, post_row in post_list_df.iterrows():
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
        similarity_means = sorted_by_mean['mean'].values
        probabilities = similarity_means / similarity_means.sum()

        # 확률이 0인 경우가 0이 아닌 경우보다 많은 경우
        zero_count = np.count_nonzero(probabilities != 0)
        if (zero_count <= page_size):
            post_len = len(page_size)
            if (page_size > post_len):
                return post_ids[:post_len]
            return post_ids[:page_size]

        selected_posts = np.random.choice(
            post_ids, size=page_size, replace=False, p=probabilities)

        return selected_posts
    except Exception as e:
        print(f"Error : {e}")
